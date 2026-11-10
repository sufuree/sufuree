package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/sufuree/sufuree/internal/activator"
	"github.com/sufuree/sufuree/internal/bootcred"
	"github.com/sufuree/sufuree/internal/bootfloor"
	"github.com/sufuree/sufuree/internal/controlauth"
	"github.com/sufuree/sufuree/internal/gateway"
	"github.com/sufuree/sufuree/internal/guest"
	"github.com/sufuree/sufuree/internal/journal"
	"github.com/sufuree/sufuree/internal/wakehint"
	"github.com/sufuree/sufuree/internal/workload"
)

func runHost(args []string) error {
	fs := flag.NewFlagSet("host", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "host config file")
	hintPoll := fs.Duration("hint-poll", 15*time.Second, "how often a sleeping workload's hint is read")
	tick := fs.Duration("tick", time.Second, "control loop period")
	controllerURL := fs.String("controller", "", "optional control API base URL for assignment pull and credential authorize")
	hostUID := fs.String("host-id", "", "this host's control-plane UID; required with --controller")
	bootFloor := fs.Bool("boot-floor", false, "take every boot generation and rollback floor from the controller and report progress to it; needs --controller")
	var tlsf controlauth.TLSFlags
	fs.StringVar(&tlsf.Cert, "tls-cert", "", "this host's client certificate for an https controller")
	fs.StringVar(&tlsf.Key, "tls-key", "", "this host's client key")
	fs.StringVar(&tlsf.CA, "tls-ca", "", "controller CA certificate")
	floorEvery := fs.Duration("floor-report", bootfloor.DefaultInterval, "how often progress is reported to the controller with --boot-floor")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *cfgPath == "" {
		return errors.New("--config is required")
	}
	if (*controllerURL == "") != (*hostUID == "") {
		return errors.New("--controller and --host-id must be set together")
	}
	if *bootFloor && *controllerURL == "" {
		return errors.New("--boot-floor needs --controller and --host-id")
	}
	if tlsf.Set() && *controllerURL == "" {
		return errors.New("--tls-* flags need --controller")
	}
	controlClient, err := tlsf.Client(*controllerURL)
	if err != nil {
		return err
	}
	cfg, err := workload.Load(*cfgPath)
	if err != nil {
		return err
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// One agent per data_dir: a second would steal sockets, race the
	// journal and reap the first one's guests.
	unlock, err := lockHost(cfg.DataDir)
	if err != nil {
		return err
	}
	defer unlock()

	j, err := journal.Open(filepath.Join(cfg.DataDir, "journal"))
	if err != nil {
		return err
	}
	var hints wakehint.Reader
	if cfg.WakeHints != nil {
		ak, ok1 := os.LookupEnv("SUFUREE_HINT_ACCESS_KEY")
		sk, ok2 := os.LookupEnv("SUFUREE_HINT_SECRET_KEY")
		if !ok1 || !ok2 {
			return errors.New("wake_hints needs SUFUREE_HINT_ACCESS_KEY and SUFUREE_HINT_SECRET_KEY")
		}
		var ca []byte
		if f := cfg.WakeHints.CAFile; f != "" {
			if ca, err = os.ReadFile(f); err != nil {
				return fmt.Errorf("wake_hints.ca_file: %w", err)
			}
		}
		if hints, err = wakehint.NewS3(cfg.WakeHints.Endpoint, cfg.WakeHints.Region, cfg.WakeHints.Bucket, ak, sk, ca); err != nil {
			return err
		}
	}
	// Per-boot credentials are issued with the agent's own AWS credentials
	// (the default chain, such as the instance profile). When --controller
	// is set, session tags are authorized against that host's placements
	// before AssumeRole (no STS call on the controller).
	var issuer bootcred.Issuer = &bootcred.STS{NewClient: func(region string) (bootcred.AssumeRoler, error) {
		c, err := awsconfig.LoadDefaultConfig(context.Background(), awsconfig.WithRegion(region))
		if err != nil {
			return nil, err
		}
		return sts.NewFromConfig(c), nil
	}}
	if *controllerURL != "" {
		issuer = &bootcred.Guarded{
			Inner: issuer,
			Client: &bootcred.AssignmentClient{
				BaseURL: *controllerURL,
				HostUID: *hostUID,
				HTTP:    controlClient,
			},
		}
		log.Info("boot credentials guarded by controller assignments", "controller", *controllerURL, "host", *hostUID)
	}
	launcher := &guest.QEMU{DataDir: cfg.DataDir}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// With --boot-floor, the controller is the authority for every boot
	// generation and rollback floor; boots fail closed without it. The
	// reporter outlives the activators so their exit reports are sent.
	var floor *bootfloor.Host
	if *bootFloor {
		client := &bootfloor.Client{BaseURL: *controllerURL, HostUID: *hostUID, HTTP: controlClient}
		floor = &bootfloor.Host{Client: client, Reporter: &bootfloor.Reporter{Client: client, Interval: *floorEvery, Logger: log}}
		// Enrolment: report every workload's local record once, so the
		// controller's records start at or above this host's.
		for _, w := range cfg.Workloads {
			r, err := j.Get(w.ID)
			if err != nil {
				return fmt.Errorf("workload %s: journal: %w", w.ID, err)
			}
			if r.Generation > 0 {
				floor.Note(bootfloor.Entry{Installation: w.ID, Incarnation: r.Incarnation, IncarnationFrom: r.IncarnationFrom,
					Generation: r.Generation, Epoch: r.ProgressEpoch, Seq: r.ProgressSeq})
			}
		}
		reportCtx, stopReports := context.WithCancel(context.Background())
		reporterDone := make(chan struct{})
		go func() { defer close(reporterDone); floor.Reporter.Run(reportCtx) }()
		floor.Kick() // send the enrolment report now
		defer func() { stopReports(); <-reporterDone }()
		log.Info("boot generations and floors from the controller", "controller", *controllerURL, "host", *hostUID, "report_every", *floorEvery)
	}

	var wg sync.WaitGroup
	errs := make(chan error, len(cfg.Workloads))
	for _, w := range cfg.Workloads {
		n, unreadable, err := launcher.Reap(w.ID)
		if err != nil {
			return fmt.Errorf("workload %s: reaping leftover guests: %w", w.ID, err)
		}
		if n > 0 {
			log.Warn("stopped guests left running by an earlier agent", "workload", w.ID, "count", n)
		}
		if unreadable > 0 {
			log.Warn("some processes could not be checked for a leftover guest", "workload", w.ID, "unreadable", unreadable)
		}
		opt := activator.Options{Tick: *tick, HintPoll: *hintPoll, Logger: log, Credentials: issuer}
		if floor != nil {
			opt.Floor = floor
		}
		wh := hints
		if w.HintKeyEnvKey != "" {
			key, err := wakehint.LoadOrCreateKey(filepath.Join(cfg.DataDir, "hintkeys"), w.ID)
			if err != nil {
				return fmt.Errorf("workload %s: hint key: %w", w.ID, err)
			}
			src, ok := hints.(wakehint.RawReader)
			if !ok {
				return fmt.Errorf("workload %s: hint_key_env_key needs wake_hints", w.ID)
			}
			id := w.ID
			opt.HintKey, wh = key, &wakehint.Verifying{Source: src, Key: key,
				Record: filepath.Join(cfg.DataDir, "hintkeys", w.ID+".last"),
				OnRefused: func(err error) {
					log.Warn("refused a wake hint; using the last authentic one", "workload", id, "err", err)
				},
				OnRecordError: func(err error) {
					log.Warn("wake hint replay record unavailable; a restart may accept one older hint", "workload", id, "err", err)
				},
			}
		}
		act := activator.New(w, j, launcher, wh, opt)
		gw := &gateway.Gateway{
			ID: w.ID, SocketPath: filepath.Join(cfg.SocketDir, w.ID+".sock"), Act: act,
			WakeTimeout: activator.WakeBudget(w, 0) + 10*time.Second, Logger: log,
		}
		if err := gw.Listen(); err != nil {
			stop()
			wg.Wait()
			return fmt.Errorf("workload %s: %w", w.ID, err)
		}
		log.Info("serving", "workload", w.ID, "socket", gw.SocketPath)
		wg.Add(2)
		go func() { defer wg.Done(); act.Run(ctx) }()
		go func() {
			defer wg.Done()
			if err := gw.Serve(ctx); err != nil {
				errs <- fmt.Errorf("workload %s gateway: %w", w.ID, err)
				stop()
			}
		}()
	}
	<-ctx.Done()
	wg.Wait()
	close(errs)
	return <-errs
}

// lockHost takes an exclusive, non-blocking lock on <dataDir>/agent.lock
// for the life of the process.
func lockHost(dataDir string) (func(), error) {
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dataDir, "agent.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("another sufuree agent holds %s: %w", dataDir, err)
	}
	return func() { _ = f.Close() }, nil
}
