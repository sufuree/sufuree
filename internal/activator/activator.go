// Package activator owns one stateful workload's wake/stop lifecycle on one
// host (RFC-0001 §4–5, single-host slice):
//
//   - demand (a gateway connection) holds an activity lease; a sleeping
//     workload is woken once, however many callers arrive (singleflight);
//   - each boot uses a fresh, durably journaled lease generation; a boot
//     that is not ready within the startup timeout is stopped and retried
//     once under a new generation (the workload refuses a reused one);
//   - an awake workload with no leases stops after its idle timeout, unless
//     its published hint says it is busy;
//   - a sleeping workload is woken for scheduled work when its hint is busy
//     or its next-due instant is within the scheduled lead.
//
// Stopping is crash-consistent by contract: the workload acknowledges only
// writes that are already durable off-host.
package activator

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/sufuree/sufuree/internal/guest"
	"github.com/sufuree/sufuree/internal/journal"
	"github.com/sufuree/sufuree/internal/wakehint"
	"github.com/sufuree/sufuree/internal/workload"
)

// State is the lifecycle state.
type State string

const (
	Asleep   State = "asleep"
	Waking   State = "waking"
	Awake    State = "awake"
	Stopping State = "stopping"
)

// ErrUnavailable means the workload could not be made ready.
var ErrUnavailable = errors.New("activator: workload could not be made ready")

// Options tunes the control loop; zero values take defaults.
type Options struct {
	Tick      time.Duration // control loop period (default 1s)
	HintPoll  time.Duration // sleeping-workload hint poll (default 15s)
	StopGrace time.Duration // SIGTERM to SIGKILL (default 5s)
	Attempts  int           // boot attempts per wake (default 2)
	Getenv    func(string) (string, bool)
	Logger    *slog.Logger
	// Ready probes a booted guest; default is an HTTP response on Addr.
	Ready func(ctx context.Context, addr string) error
}

// Activator manages one workload.
type Activator struct {
	spec     workload.Spec
	journal  *journal.Journal
	launcher guest.Launcher
	hints    wakehint.Reader // nil: no scheduled wake, no busy veto
	opt      Options
	log      *slog.Logger

	mu          sync.Mutex
	state       State
	transition  chan struct{} // closed when the current waking/stopping ends
	wakeErr     error
	guest       guest.Guest
	generation  uint64
	active      int
	lastActive  time.Time
	awakeSince  time.Time
	lastHintAt  time.Time
	stats       Stats
	stopControl chan struct{}
	done        chan struct{}
}

// Stats are counters for receipts.
type Stats struct {
	Wakes, ScheduledWakes, IdleStops, BusyVetoes, FailedBoots, UnexpectedExits int
}

// New builds an activator; Run starts its control loop.
func New(spec workload.Spec, j *journal.Journal, l guest.Launcher, hints wakehint.Reader, opt Options) *Activator {
	if opt.Tick <= 0 {
		opt.Tick = time.Second
	}
	if opt.HintPoll <= 0 {
		opt.HintPoll = 15 * time.Second
	}
	if opt.StopGrace <= 0 {
		opt.StopGrace = 5 * time.Second
	}
	if opt.Attempts <= 0 {
		opt.Attempts = 2
	}
	if opt.Getenv == nil {
		opt.Getenv = os.LookupEnv
	}
	if opt.Logger == nil {
		opt.Logger = slog.Default()
	}
	if opt.Ready == nil {
		opt.Ready = httpReady
	}
	return &Activator{
		spec: spec, journal: j, launcher: l, hints: hints, opt: opt,
		log:   opt.Logger.With("workload", spec.ID),
		state: Asleep, stopControl: make(chan struct{}), done: make(chan struct{}),
	}
}

// Snapshot reports the state, generation and counters.
func (a *Activator) Snapshot() (State, uint64, int, Stats) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.state, a.generation, a.active, a.stats
}

// Acquire holds an activity lease, waking the workload if needed, and
// returns the guest address. release must be called exactly once.
func (a *Activator) Acquire(ctx context.Context) (addr string, release func(), err error) {
	a.mu.Lock()
	a.active++ // counted before waiting, so an idle stop cannot race us
	for {
		switch a.state {
		case Awake:
			addr := a.guest.Addr()
			a.mu.Unlock()
			return addr, a.releaser(), nil
		case Asleep:
			a.beginWake(false)
		}
		wait := a.transition
		a.mu.Unlock()
		select {
		case <-wait:
		case <-ctx.Done():
			a.mu.Lock()
			a.dropLease()
			a.mu.Unlock()
			return "", nil, ctx.Err()
		}
		a.mu.Lock()
		if a.state == Asleep && a.wakeErr != nil {
			err := a.wakeErr
			a.dropLease()
			a.mu.Unlock()
			return "", nil, err
		}
	}
}

func (a *Activator) releaser() func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			a.mu.Lock()
			a.dropLease()
			a.mu.Unlock()
		})
	}
}

// dropLease requires a.mu.
func (a *Activator) dropLease() {
	a.active--
	a.lastActive = time.Now()
}

// beginWake requires a.mu and state Asleep.
func (a *Activator) beginWake(scheduled bool) {
	a.state = Waking
	a.wakeErr = nil
	a.transition = make(chan struct{})
	a.stats.Wakes++
	if scheduled {
		a.stats.ScheduledWakes++
	}
	go a.wake(a.transition, scheduled)
}

func (a *Activator) wake(done chan struct{}, scheduled bool) {
	var lastErr error
	for attempt := 1; attempt <= a.opt.Attempts; attempt++ {
		g, gen, err := a.bootOnce()
		if err == nil {
			a.mu.Lock()
			a.state, a.guest, a.generation = Awake, g, gen
			a.awakeSince, a.lastActive = time.Now(), time.Now()
			close(done)
			a.mu.Unlock()
			a.log.Info("awake", "generation", gen, "scheduled", scheduled, "attempt", attempt)
			go a.watchExit(g, gen)
			return
		}
		lastErr = err
		a.mu.Lock()
		a.stats.FailedBoots++
		a.mu.Unlock()
		a.log.Warn("boot failed; retrying under a new generation", "attempt", attempt, "err", err)
	}
	a.mu.Lock()
	a.state = Asleep
	a.wakeErr = fmt.Errorf("%w: %v", ErrUnavailable, lastErr)
	close(done)
	a.mu.Unlock()
}

func (a *Activator) bootOnce() (guest.Guest, uint64, error) {
	gen, err := a.journal.NextGeneration(a.spec.ID) // durable before boot
	if err != nil {
		return nil, 0, fmt.Errorf("issue generation: %w", err)
	}
	env, err := a.spec.ResolveBootEnv(gen, a.opt.Getenv)
	if err != nil {
		return nil, 0, err
	}
	files, err := a.spec.ResolveBootFiles(a.opt.Getenv)
	if err != nil {
		return nil, 0, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), a.spec.StartupTimeout.Duration)
	defer cancel()
	t0 := time.Now()
	g, err := a.launcher.Boot(ctx, a.spec, gen, env, files)
	if err != nil {
		return nil, 0, err
	}
	for {
		select {
		case <-g.Exited():
			return nil, 0, fmt.Errorf("generation %d exited before ready", gen)
		default:
		}
		pctx, pcancel := context.WithTimeout(ctx, time.Second)
		err := a.opt.Ready(pctx, g.Addr())
		pcancel()
		if err == nil {
			a.log.Info("ready", "generation", gen, "ms", time.Since(t0).Milliseconds())
			return g, gen, nil
		}
		select {
		case <-ctx.Done():
			_ = g.Stop(a.opt.StopGrace)
			return nil, 0, fmt.Errorf("generation %d not ready within %s: %v", gen, a.spec.StartupTimeout.Duration, err)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// watchExit returns the workload to Asleep if its guest ends on its own
// (crash, OOM, guest-initiated exit).
func (a *Activator) watchExit(g guest.Guest, gen uint64) {
	<-g.Exited()
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.guest == g && a.state == Awake {
		a.state, a.guest = Asleep, nil
		a.stats.UnexpectedExits++
		a.log.Warn("guest exited unexpectedly", "generation", gen)
	}
}

// Run drives idle stop and scheduled wake until ctx ends, then stops the
// guest.
func (a *Activator) Run(ctx context.Context) {
	defer close(a.done)
	t := time.NewTicker(a.opt.Tick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			a.shutdown()
			return
		case <-t.C:
			a.tick(ctx)
		}
	}
}

// Done closes after Run returns.
func (a *Activator) Done() <-chan struct{} { return a.done }

func (a *Activator) tick(ctx context.Context) {
	a.mu.Lock()
	state, active, gen := a.state, a.active, a.generation
	idleFor, upFor := time.Since(a.lastActive), time.Since(a.awakeSince)
	pollDue := time.Since(a.lastHintAt) >= a.opt.HintPoll
	a.mu.Unlock()

	switch {
	case state == Awake && active == 0 && idleFor >= a.spec.IdleTimeout.Duration && upFor >= a.spec.MinAwake.Duration:
		if busy, err := a.busy(ctx, gen); err != nil || busy {
			a.mu.Lock()
			a.stats.BusyVetoes++
			a.lastActive = time.Now() // re-check after another idle period
			a.mu.Unlock()
			a.log.Info("idle stop vetoed", "busy", busy, "err", err)
			return
		}
		a.stopIfStillIdle()
	case state == Asleep && a.hints != nil && a.spec.HintPrefix != "" && pollDue:
		a.mu.Lock()
		a.lastHintAt = time.Now()
		a.mu.Unlock()
		h, ok, err := a.hints.Read(ctx, a.spec.HintPrefix)
		if err != nil || !ok {
			return
		}
		due := h.Busy || (h.NextDue != nil && !time.Now().Before(h.NextDue.Add(-a.spec.ScheduledLead.Duration)))
		if !due {
			return
		}
		a.mu.Lock()
		if a.state == Asleep {
			a.log.Info("scheduled wake", "busy", h.Busy, "next_due", h.NextDue)
			a.beginWake(true)
		}
		a.mu.Unlock()
	}
}

// busy reads the current generation's hint. A hint from an older
// generation says nothing about this boot and is treated as not busy.
func (a *Activator) busy(ctx context.Context, gen uint64) (bool, error) {
	if a.hints == nil || a.spec.HintPrefix == "" {
		return false, nil
	}
	h, ok, err := a.hints.Read(ctx, a.spec.HintPrefix)
	if err != nil {
		return false, err
	}
	return ok && h.Generation == gen && h.Busy, nil
}

func (a *Activator) stopIfStillIdle() {
	a.mu.Lock()
	if a.state != Awake || a.active != 0 {
		a.mu.Unlock()
		return
	}
	g, gen := a.guest, a.generation
	a.state = Stopping
	a.transition = make(chan struct{})
	done := a.transition
	a.stats.IdleStops++
	a.mu.Unlock()

	a.log.Info("idle stop", "generation", gen)
	_ = g.Stop(a.opt.StopGrace)

	a.mu.Lock()
	a.state, a.guest = Asleep, nil
	close(done)
	a.mu.Unlock()
}

func (a *Activator) shutdown() {
	a.mu.Lock()
	g := a.guest
	wait := a.transition
	st := a.state
	a.mu.Unlock()
	if st == Waking && wait != nil {
		<-wait
		a.mu.Lock()
		g = a.guest
		a.mu.Unlock()
	}
	if g != nil {
		_ = g.Stop(a.opt.StopGrace)
	}
}

// httpReady succeeds when the guest answers HTTP at all. A TCP connect is
// not enough: QEMU user-mode forwarding accepts on the host before the
// guest listens.
func httpReady(ctx context.Context, addr string) error {
	var d net.Dialer
	c, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()
	if dl, ok := ctx.Deadline(); ok {
		_ = c.SetDeadline(dl)
	}
	if _, err := c.Write([]byte("GET / HTTP/1.1\r\nHost: guest\r\nConnection: close\r\n\r\n")); err != nil {
		return err
	}
	line, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		return err
	}
	if !strings.HasPrefix(line, "HTTP/") {
		return fmt.Errorf("not HTTP: %q", line)
	}
	return nil
}
