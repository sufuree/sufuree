package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ec2"

	"github.com/sufuree/sufuree/internal/bootfloor"
	"github.com/sufuree/sufuree/internal/budget"
	"github.com/sufuree/sufuree/internal/controlauth"
	"github.com/sufuree/sufuree/internal/controlraft"
	"github.com/sufuree/sufuree/internal/demand"
	"github.com/sufuree/sufuree/internal/edge"
	"github.com/sufuree/sufuree/internal/executor"
	"github.com/sufuree/sufuree/internal/executor/ec2exec"
	"github.com/sufuree/sufuree/internal/raftnode"
	"github.com/sufuree/sufuree/internal/reconcile"
	"github.com/sufuree/sufuree/internal/resource"
	"github.com/sufuree/sufuree/internal/store"
)

// ensureDeployment records an operator-supplied deployment id the first time.
// It does not invent an id and does not change a deployment that is already stored.
func ensureDeployment(db interface {
	Update(func(*store.Tx) error) error
}, id string) error {
	if id == "" {
		return nil
	}
	return db.Update(func(tx *store.Tx) error {
		old, err := tx.GetMeta()
		if errors.Is(err, store.ErrNotFound) {
			return tx.PutMeta(resource.ClusterMeta{
				DeploymentUID: id, RecoveryIncarnation: 1, OwnerEpoch: 1,
			})
		}
		if err != nil {
			return err
		}
		if old.DeploymentUID != id {
			return fmt.Errorf("store deployment is %s, flag is %s", old.DeploymentUID, id)
		}
		return nil
	})
}

func runController(args []string) error {
	fs := flag.NewFlagSet("controller", flag.ContinueOnError)
	dataDir := fs.String("data-dir", "", "durable control store directory")
	listen := fs.String("listen", "", "control API listen address")
	execName := fs.String("executor", "", "empty refuses effects; \"memory\" or \"ec2\" enable mutations")
	deployment := fs.String("deployment", "", "deployment UID written only when the store has no cluster meta")
	tickFlag := fs.Duration("tick", time.Second, "reconcile period")
	raftBind := fs.String("raft-bind", "", "raft transport address, for example 127.0.0.1:7000")
	raftID := fs.String("raft-id", "", "raft server id, for example controller-1")
	raftBootstrap := fs.Bool("raft-bootstrap", false, "create the initial one-voter raft configuration; refuse if raft state already exists")
	edgeListen := fs.String("edge-listen", "", "optional public HTTP edge listen address")
	edgeMax := fs.Int("edge-max-concurrent", 256, "edge concurrent request cap; 0 disables the cap")
	bootFloor := fs.Bool("boot-floor", false, "serve per-installation boot generations and rollback floors to hosts")
	tlsCert := fs.String("tls-cert", "", "control API server certificate (from `sufuree ca issue --role controller`)")
	tlsKey := fs.String("tls-key", "", "control API server key")
	tlsClientCA := fs.String("tls-client-ca", "", "controller CA certificate; every caller must present a certificate it issued")
	replicaCfg := fs.String("replica-credentials", "", "JSON config: issue each installation its own replica credentials, limited to its prefix (package replicacred)")
	grantMaxHosts := fs.Int("grant-max-hosts", 0, "growth grant host cap; required with --executor memory|ec2 when >0 gates the ledger")
	grantMaxMemory := fs.Int("grant-max-memory-mib", 0, "growth grant memory cap (MiB)")
	grantExpires := fs.Duration("grant-expires", 0, "growth grant lifetime from process start; 0 means no expiry")
	grantRevision := fs.String("grant-revision", "", "optional growth grant revision id")
	ec2Region := fs.String("ec2-region", "", "AWS region for --executor ec2")
	ec2AMI := fs.String("ec2-ami", "", "approved AMI for --executor ec2")
	ec2Type := fs.String("ec2-instance-type", "", "approved instance type for --executor ec2")
	ec2Subnet := fs.String("ec2-subnet", "", "subnet id for --executor ec2")
	ec2SG := fs.String("ec2-security-group", "", "security group id for --executor ec2")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dataDir == "" || *listen == "" {
		return errors.New("--data-dir and --listen are required")
	}
	raftOn := *raftBind != "" || *raftID != "" || *raftBootstrap
	// TLS is all or nothing, checked before Raft opens so a partial set
	// never starts a plaintext peer transport.
	authOn := *tlsCert != "" || *tlsKey != "" || *tlsClientCA != ""
	if authOn && (*tlsCert == "" || *tlsKey == "" || *tlsClientCA == "") {
		return errors.New("--tls-cert, --tls-key and --tls-client-ca must be set together")
	}
	if raftOn && (*raftBind == "" || *raftID == "") {
		return errors.New("--raft-bind and --raft-id are required to enable raft")
	}
	// Loaded before Raft opens, so a bad config or an unreachable KMS
	// leaves no partial cluster.
	var replica reconcile.ReplicaIssuer
	if *replicaCfg != "" {
		if !authOn {
			// Without caller authentication anyone could claim a host and
			// take its installations' credentials.
			return errors.New("--replica-credentials needs --tls-cert, --tls-key and --tls-client-ca")
		}
		svc, err := loadReplicaService(context.Background(), *replicaCfg)
		if err != nil {
			return err
		}
		replica = svc
	}
	if *execName == "ec2" && (*tlsCert == "" || *tlsKey == "" || *tlsClientCA == "") {
		return errors.New("--executor ec2 requires --tls-cert, --tls-key and --tls-client-ca")
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	effects, err := buildExecutor(*execName, executorOpts{
		deployment:       *deployment,
		grantMaxHosts:    *grantMaxHosts,
		grantMaxMemory:   *grantMaxMemory,
		grantExpires:     *grantExpires,
		grantRevision:    *grantRevision,
		ec2Region:        *ec2Region,
		ec2AMI:           *ec2AMI,
		ec2Type:          *ec2Type,
		ec2Subnet:        *ec2Subnet,
		ec2SecurityGroup: *ec2SG,
		log:              log,
	})
	if err != nil {
		return err
	}
	tick := *tickFlag
	if tick <= 0 {
		tick = time.Second
	}
	// Open the record file so the Raft FSM can Import/Export it. With Raft
	// enabled, Journal.Update proposes restore_store; the FSM Import is the
	// only write to control.db. Without Raft, Journal writes the store
	// directly. If Raft open fails and this process created control.db, remove
	// it so a bad flag combination leaves no partial cluster.
	controlPath := filepath.Join(*dataDir, "control.db")
	_, statErr := os.Stat(controlPath)
	controlExisted := statErr == nil
	st, err := store.Open(controlPath)
	if err != nil {
		return err
	}
	storeOpen := true
	defer func() {
		if storeOpen {
			_ = st.Close()
		}
	}()
	journal := &controlraft.Journal{Store: st}
	var node *raftnode.Node
	if raftOn {
		raftDir := filepath.Join(*dataDir, "raft")
		_, raftStat := os.Stat(raftDir)
		raftExisted := raftStat == nil
		var err error
		var peerTLS *raftnode.PeerTLS
		if authOn {
			srvTLS, cliTLS, terr := controlauth.PeerTLS(*tlsCert, *tlsKey, *tlsClientCA)
			if terr != nil {
				return terr
			}
			peerTLS = &raftnode.PeerTLS{Server: srvTLS, Client: cliTLS}
		} else {
			log.Warn("raft peers without TLS: development only, not for PHI")
		}
		node, err = raftnode.Open(raftnode.Config{
			DataDir:   raftDir,
			NodeID:    *raftID,
			BindAddr:  *raftBind,
			Bootstrap: *raftBootstrap,
			TLS:       peerTLS,
		}, controlraft.StoreFSM(st))
		if err != nil {
			_ = st.Close()
			storeOpen = false
			if !controlExisted {
				_ = os.Remove(controlPath)
			}
			if !raftExisted {
				_ = os.RemoveAll(raftDir)
			}
			return err
		}
		defer node.Close()
		journal.Node = node
		if err := waitLeader(node, 20*time.Second); err != nil {
			return err
		}
		log.Info("raft", "id", *raftID, "bind", *raftBind, "bootstrap", *raftBootstrap)
	}
	if err := ensureDeployment(journal, *deployment); err != nil {
		return err
	}
	dem := demand.New(demand.Bolt{Store: journal})
	c := reconcile.New(journal, effects, dem, reconcile.Options{
		Tick: tick, Backoff: 15 * time.Second, Logger: log,
	})
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		return err
	}
	var leader leaderView
	if node != nil {
		leader = node
	}
	srv := &http.Server{Handler: controlHandlerAuth(leader, c, floorJournal(*bootFloor, journal), authOn, replica)}
	if authOn {
		if srv.TLSConfig, err = controlauth.ServerTLS(*tlsCert, *tlsKey, *tlsClientCA); err != nil {
			return err
		}
	} else {
		log.Warn("control API without TLS or caller authentication: development only, not for PHI")
	}
	var edgeSrv *http.Server
	var edgeLn net.Listener
	if *edgeListen != "" {
		edgeLn, err = net.Listen("tcp", *edgeListen)
		if err != nil {
			return err
		}
		edgeSrv = &http.Server{Handler: &edge.Server{
			Activator:     &reconcile.EdgeActivator{Controller: c, Demand: dem},
			MaxConcurrent: *edgeMax,
		}}
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	errc := make(chan error, 3)
	if node != nil {
		go func() { errc <- reconcileWhenLeader(ctx, node, c, tick, log) }()
	} else {
		go func() { errc <- c.Run(ctx) }()
	}
	go func() {
		log.Info("control API", "addr", ln.Addr().String(), "executor", *execName)
		var err error
		if authOn {
			err = srv.ServeTLS(ln, "", "")
		} else {
			err = srv.Serve(ln)
		}
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		errc <- err
	}()
	if edgeSrv != nil {
		go func() {
			log.Info("edge", "addr", edgeLn.Addr().String())
			err := edgeSrv.Serve(edgeLn)
			if errors.Is(err, http.ErrServerClosed) {
				err = nil
			}
			errc <- err
		}()
	}
	select {
	case <-ctx.Done():
		shut, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shut)
		if edgeSrv != nil {
			_ = edgeSrv.Shutdown(shut)
		}
		n := 2
		if edgeSrv != nil {
			n = 3
		}
		var first error
		for i := 0; i < n; i++ {
			if err := <-errc; first == nil {
				first = err
			}
		}
		return first
	case err := <-errc:
		stop()
		shut, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shut)
		if edgeSrv != nil {
			_ = edgeSrv.Shutdown(shut)
		}
		return err
	}
}

// leaderView is the leadership check the control API uses. A nil view means
// this process is not running Raft and keeps the single-store behavior.
type leaderView interface {
	IsLeader() bool
}

func controlHandler(leader leaderView, c *reconcile.Controller) http.Handler {
	return controlHandlerWith(leader, c, nil)
}

func floorJournal(on bool, j *controlraft.Journal) bootfloor.Committer {
	if !on {
		return nil
	}
	return j
}

// controlHandlerWith is the control API, plus the boot-floor routes when
// floors is set.
func controlHandlerWith(leader leaderView, c *reconcile.Controller, floors bootfloor.Committer) http.Handler {
	return controlHandlerAuth(leader, c, floors, false, nil)
}

// controlHandlerAuth is the control API. With auth, every request needs a
// verified client certificate whose principal the route policy admits
// (controlauth), and floor lowering takes two authenticated principals.
// With replica set, hosts also get their installations' replica
// credentials.
func controlHandlerAuth(leader leaderView, c *reconcile.Controller, floors bootfloor.Committer, auth bool, replica reconcile.ReplicaIssuer) http.Handler {
	next := reconcile.HandlerWith(c, replica)
	if floors != nil {
		mux := http.NewServeMux()
		var identify bootfloor.Identify
		if auth {
			identify = func(r *http.Request) (string, bool) {
				p, ok := controlauth.FromContext(r.Context())
				return p.String(), ok
			}
		}
		bootfloor.RegisterWithIdentity(mux, floors, nil, identify)
		mux.Handle("/", next)
		next = mux
	}
	if leader != nil {
		next = leaderOnly(leader, next)
	}
	if auth {
		next = controlauth.Middleware(next)
	}
	return next
}

func leaderOnly(leader leaderView, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if mutates(r.Method) && !leader.IsLeader() {
			http.Error(w, raftnode.ErrNotLeader.Error(), http.StatusServiceUnavailable)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func mutates(method string) bool {
	switch method {
	case http.MethodPut, http.MethodPost, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

func waitLeader(node *raftnode.Node, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		if node.IsLeader() {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%w after %s", raftnode.ErrNotLeader, timeout)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func reconcileWhenLeader(ctx context.Context, node *raftnode.Node, c *reconcile.Controller, tick time.Duration, log *slog.Logger) error {
	if tick <= 0 {
		tick = time.Second
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	leading := false
	for {
		isLeader := node.IsLeader()
		if isLeader && !leading {
			log.Info("raft leader; reconcile running")
		}
		if !isLeader && leading {
			log.Info("raft follower; reconcile paused")
		}
		leading = isLeader
		if isLeader {
			if err := c.ReconcileOnce(ctx); err != nil && ctx.Err() == nil {
				log.Error("reconcile", "err", err)
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

type executorOpts struct {
	deployment       string
	grantMaxHosts    int
	grantMaxMemory   int
	grantExpires     time.Duration
	grantRevision    string
	ec2Region        string
	ec2AMI           string
	ec2Type          string
	ec2Subnet        string
	ec2SecurityGroup string
	log              *slog.Logger
}

func buildExecutor(name string, opt executorOpts) (executor.Executor, error) {
	switch name {
	case "":
		return &executor.Memory{}, nil
	case "memory":
		inner := &executor.Memory{AllowEffects: true}
		return gateExecutor(inner, opt)
	case "ec2":
		if opt.deployment == "" {
			return nil, errors.New("--deployment is required with --executor ec2")
		}
		if opt.ec2Region == "" || opt.ec2AMI == "" || opt.ec2Type == "" || opt.ec2Subnet == "" || opt.ec2SecurityGroup == "" {
			return nil, errors.New("--ec2-region, --ec2-ami, --ec2-instance-type, --ec2-subnet and --ec2-security-group are required with --executor ec2")
		}
		if opt.grantMaxHosts < 1 || opt.grantMaxMemory < 32 {
			return nil, errors.New("--grant-max-hosts (>=1) and --grant-max-memory-mib (>=32) are required with --executor ec2")
		}
		cfg, err := awsconfig.LoadDefaultConfig(context.Background(), awsconfig.WithRegion(opt.ec2Region))
		if err != nil {
			return nil, fmt.Errorf("ec2 config: %w", err)
		}
		ex, err := ec2exec.New(ec2exec.Config{
			Client:        ec2.NewFromConfig(cfg),
			DeploymentUID: opt.deployment,
			AllowEffects:  true,
			Launch: ec2exec.Launch{
				AMI: opt.ec2AMI, InstanceType: opt.ec2Type,
				SubnetID: opt.ec2Subnet, SecurityGroupID: opt.ec2SecurityGroup,
				NamePrefix: "sufuree-" + opt.deployment,
			},
		})
		if err != nil {
			return nil, err
		}
		opt.log.Warn("ec2 executor enabled: real AWS RunInstances/TerminateInstances will run under the growth grant")
		return gateExecutor(ex, opt)
	default:
		return nil, fmt.Errorf("unknown executor %q", name)
	}
}

func gateExecutor(inner executor.Executor, opt executorOpts) (executor.Executor, error) {
	if opt.grantMaxHosts < 0 || opt.grantMaxMemory < 0 {
		return nil, errors.New("--grant-max-hosts and --grant-max-memory-mib must be zero or positive")
	}
	if opt.grantMaxHosts == 0 && opt.grantMaxMemory == 0 {
		// memory without an explicit grant keeps the historical uncapped path
		if _, ok := inner.(*executor.Memory); ok {
			return inner, nil
		}
		return nil, errors.New("growth grant flags are required for this executor")
	}
	if opt.deployment == "" {
		return nil, errors.New("--deployment is required when a growth grant is set")
	}
	if opt.grantMaxHosts < 1 || opt.grantMaxMemory < 32 {
		return nil, errors.New("--grant-max-hosts (>=1) and --grant-max-memory-mib (>=32) are required")
	}
	g := budget.Grant{
		DeploymentUID: opt.deployment,
		Revision:      opt.grantRevision,
		MaxHosts:      opt.grantMaxHosts,
		MaxMemoryMiB:  opt.grantMaxMemory,
	}
	if opt.grantExpires > 0 {
		g.Expires = time.Now().UTC().Add(opt.grantExpires)
	}
	if err := g.Valid(time.Now().UTC()); err != nil {
		return nil, err
	}
	return &executor.Gated{Inner: inner, Ledger: &budget.Ledger{Grant: g}}, nil
}
