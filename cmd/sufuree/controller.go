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

	"github.com/sufuree/sufuree/internal/bootfloor"
	"github.com/sufuree/sufuree/internal/controlauth"
	"github.com/sufuree/sufuree/internal/controlraft"
	"github.com/sufuree/sufuree/internal/demand"
	"github.com/sufuree/sufuree/internal/edge"
	"github.com/sufuree/sufuree/internal/executor"
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
	execName := fs.String("executor", "", "empty refuses effects; \"memory\" enables the in-memory executor")
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
	mem := &executor.Memory{}
	switch *execName {
	case "":
	case "memory":
		mem.AllowEffects = true
	default:
		return fmt.Errorf("unknown executor %q", *execName)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
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
	c := reconcile.New(journal, mem, dem, reconcile.Options{
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
	srv := &http.Server{Handler: controlHandlerAuth(leader, c, floorJournal(*bootFloor, journal), authOn)}
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
	return controlHandlerAuth(leader, c, floors, false)
}

// controlHandlerAuth is the control API. With auth, every request needs a
// verified client certificate whose principal the route policy admits
// (controlauth), and floor lowering takes two authenticated principals.
func controlHandlerAuth(leader leaderView, c *reconcile.Controller, floors bootfloor.Committer, auth bool) http.Handler {
	next := reconcile.Handler(c)
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
