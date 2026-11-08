// Command sufuree runs the single-host slice of the Sufuree host agent:
//
//	sufuree host --config host.json
//
// For each admitted workload it serves a 0600 Unix socket, wakes the guest
// on demand, stops it when idle, and wakes it for scheduled work announced
// in the workload's published hint.
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
	"github.com/sufuree/sufuree/internal/gateway"
	"github.com/sufuree/sufuree/internal/guest"
	"github.com/sufuree/sufuree/internal/journal"
	"github.com/sufuree/sufuree/internal/wakehint"
	"github.com/sufuree/sufuree/internal/workload"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "sufuree:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 || args[0] != "host" {
		return errors.New("usage: sufuree host --config <host.json> [--hint-poll 15s] [--tick 1s]")
	}
	fs := flag.NewFlagSet("host", flag.ContinueOnError)
	cfgPath := fs.String("config", "", "host config file")
	hintPoll := fs.Duration("hint-poll", 15*time.Second, "how often a sleeping workload's hint is read")
	tick := fs.Duration("tick", time.Second, "control loop period")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if *cfgPath == "" {
		return errors.New("--config is required")
	}
	cfg, err := workload.Load(*cfgPath)
	if err != nil {
		return err
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

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
	// (the default chain, such as the instance profile).
	issuer := &bootcred.STS{NewClient: func(region string) (bootcred.AssumeRoler, error) {
		c, err := awsconfig.LoadDefaultConfig(context.Background(), awsconfig.WithRegion(region))
		if err != nil {
			return nil, err
		}
		return sts.NewFromConfig(c), nil
	}}
	launcher := &guest.QEMU{DataDir: cfg.DataDir}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var wg sync.WaitGroup
	errs := make(chan error, len(cfg.Workloads))
	for _, w := range cfg.Workloads {
		act := activator.New(w, j, launcher, hints, activator.Options{Tick: *tick, HintPoll: *hintPoll, Logger: log, Credentials: issuer})
		gw := &gateway.Gateway{
			ID: w.ID, SocketPath: filepath.Join(cfg.SocketDir, w.ID+".sock"), Act: act,
			WakeTimeout: 2*w.StartupTimeout.Duration + 10*time.Second, Logger: log,
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
