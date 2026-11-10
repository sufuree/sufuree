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
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/sufuree/sufuree/internal/bootcred"
	"github.com/sufuree/sufuree/internal/bootfloor"
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

// ErrClosed is returned once the activator is shutting down.
var ErrClosed = errors.New("activator: shutting down")

// maxHintErrors is how many consecutive unreadable hints may veto an idle
// stop. The hint is advisory, so after that the stop goes ahead: stopping is
// crash-consistent and the next wake resumes any work.
const maxHintErrors = 3

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
	// Credentials issues per-boot credentials for a workload with
	// boot_credentials. A boot of such a workload fails without it.
	Credentials bootcred.Issuer
	// HintKey is the workload's hint key (hint_key_env_key), passed to
	// every boot. A boot of such a workload fails without it.
	HintKey []byte
	// Floor, when set, makes the controller the authority for the boot
	// generation and the rollback floor (package bootfloor): every boot
	// asks it for a fresh grant and fails closed without one, and recorded
	// progress is reported to it. Nil keeps the host-local journal, for
	// development and single-host use only.
	Floor FloorAuthority
	// FloorRead is how often an awake workload's hint is read so its
	// progress reaches the controller (default bootfloor.DefaultInterval).
	FloorRead time.Duration
}

// FloorAuthority is the controller's boot-floor service as a host sees it.
type FloorAuthority interface {
	Boot(ctx context.Context, installation string, local bootfloor.Entry) (bootfloor.Grant, error)
	Note(e bootfloor.Entry)
	Kick()
}

// FloorGrantTimeout bounds one boot grant request.
const FloorGrantTimeout = 10 * time.Second

// Activator manages one workload.
type Activator struct {
	spec     workload.Spec
	journal  *journal.Journal
	launcher guest.Launcher
	hints    wakehint.Reader // nil: no scheduled wake, no busy veto
	opt      Options
	log      *slog.Logger

	mu         sync.Mutex
	state      State
	transition chan struct{} // closed when the current waking/stopping ends
	wakeErr    error
	guest      guest.Guest
	generation uint64
	active     int
	lastActive time.Time
	awakeSince time.Time
	lastHintAt time.Time
	// lastFloorRead is when an awake workload's progress was last read
	// for the controller.
	lastFloorRead time.Time
	exits         sync.WaitGroup // running watchExit goroutines
	closed        bool           // shutting down: no new leases or wakes
	// A hint wakes a sleeping workload once per trigger (a busy hint's
	// generation, or a next_due instant), so a guest that never updates
	// its hint is not woken, idle-stopped and woken again forever. A
	// failed wake clears these so the next poll retries.
	wokeBusy    bool
	wokeBusyGen uint64
	wokeDue     time.Time
	// failedHintWakes counts consecutive failed scheduled wakes; hint
	// wakes back off (hintBackoff) until one succeeds.
	failedHintWakes int
	hintWakeAfter   time.Time
	hintErrs        int // consecutive unreadable hints at idle stop
	stats           Stats
	stopControl     chan struct{}
	done            chan struct{}
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
		opt.Attempts = defaultAttempts
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
		if a.closed {
			a.dropLease()
			a.mu.Unlock()
			return "", nil, ErrClosed
		}
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
	a.hintErrs = 0
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
			a.failedHintWakes, a.hintWakeAfter = 0, time.Time{}
			a.state, a.guest, a.generation = Awake, g, gen
			a.awakeSince, a.lastActive = time.Now(), time.Now()
			// Counted before anyone can see Awake, so shutdown's Wait
			// always covers this guest's exit watcher.
			a.exits.Add(1)
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
	if scheduled {
		// Let a later hint poll retry the trigger, after a backoff.
		a.wokeBusy, a.wokeDue = false, time.Time{}
		a.failedHintWakes++
		a.hintWakeAfter = time.Now().Add(hintBackoff(a.failedHintWakes, a.opt.HintPoll))
	}
	close(done)
	a.mu.Unlock()
}

func (a *Activator) bootOnce() (guest.Guest, uint64, error) {
	// Read the hint before the generation is issued: the record sent to
	// the controller then carries the last guest's final progress, even
	// when the read right after its exit failed. With a floor incarnation
	// still open, a later read could fall below a start this boot moves.
	a.readProgress()
	gen, err := a.issueGeneration() // durable before boot
	if err != nil {
		return nil, 0, fmt.Errorf("issue generation: %w", err)
	}
	env, err := a.spec.ResolveBootEnv(gen, a.opt.Getenv)
	if err != nil {
		return nil, 0, err
	}
	if err := a.progressEnv(env); err != nil {
		return nil, 0, err
	}
	if k := a.spec.HintKeyEnvKey; k != "" {
		if len(a.opt.HintKey) != wakehint.KeySize {
			return nil, 0, errors.New("hint_key_env_key is set but the agent has no hint key")
		}
		env[k] = hex.EncodeToString(a.opt.HintKey)
	}
	files, err := a.spec.ResolveBootFiles(a.opt.Getenv)
	if err != nil {
		return nil, 0, err
	}
	if a.spec.BootCredentials != nil {
		if err := a.issueCredentials(gen, env); err != nil {
			return nil, 0, err
		}
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

// CredentialIssueTimeout bounds issuing one boot's credentials.
const CredentialIssueTimeout = 30 * time.Second

// ProgressReadTimeout bounds the hint read before each boot (progress_env_key).
const ProgressReadTimeout = 5 * time.Second

// defaultAttempts is the boot attempts per wake when Options.Attempts is 0.
const defaultAttempts = 2

// WakeBudget is the longest a wake can take: every attempt's credential
// issue (if any) and startup timeout. attempts 0 means the default. A caller
// waiting on a wake (the gateway) must allow at least this.
func WakeBudget(w workload.Spec, attempts int) time.Duration {
	if attempts <= 0 {
		attempts = defaultAttempts
	}
	per := w.StartupTimeout.Duration
	if w.BootCredentials != nil {
		per += CredentialIssueTimeout
	}
	if w.ProgressEnvKey != "" {
		per += ProgressReadTimeout
	}
	return time.Duration(attempts) * per
}

// issueGeneration returns the generation for the next boot, durable in the
// host journal. With a controller it is a fresh grant, merged with this
// host's record first; there is no fallback when the controller is
// unreachable.
func (a *Activator) issueGeneration() (uint64, error) {
	if a.opt.Floor == nil {
		return a.journal.NextGeneration(a.spec.ID)
	}
	r, err := a.journal.Get(a.spec.ID)
	if err != nil {
		return 0, err
	}
	local := entryOf(a.spec.ID, r)
	// A verified hint's generation is an authenticated lower bound on H:
	// sending it keeps a controller restored behind the store from granting
	// an H the guest's fence refuses, one boot attempt at a time.
	if hg := a.verifiedHintGeneration(); hg > local.Generation && hg <= bootfloor.MaxGeneration {
		local.Generation = hg
	}
	ctx, cancel := context.WithTimeout(context.Background(), FloorGrantTimeout)
	defer cancel()
	g, err := a.opt.Floor.Boot(ctx, a.spec.ID, local)
	if err != nil {
		if errors.Is(err, bootfloor.ErrBehindHost) {
			// Report the record so the controller holds one (at least its
			// H) for the operator to re-apply the lowering against.
			a.opt.Floor.Note(local)
			a.opt.Floor.Kick()
			a.log.Error("ALERT: refusing a boot: the controller is behind this host's floor incarnation (restored from an older backup?); an operator must re-apply the lowering",
				"incarnation", r.Incarnation)
		}
		return 0, fmt.Errorf("controller boot grant: %w", err)
	}
	nr, err := a.journal.Adopt(a.spec.ID, journal.Grant{
		Generation: g.Generation, Incarnation: g.Incarnation, IncarnationFrom: g.IncarnationFrom,
		Epoch: g.ProgressEpoch, Seq: g.ProgressSeq,
	})
	if err != nil {
		if errors.Is(err, journal.ErrNewerLocalIncarnation) {
			a.log.Error("ALERT: refusing a boot: the controller granted an older floor incarnation than this host holds", "err", err)
		}
		return 0, err
	}
	if r.Incarnation != 0 && nr.Incarnation > r.Incarnation {
		a.log.Warn("floor incarnation changed (an intentional rollback); hints from earlier generations are ignored",
			"incarnation", nr.Incarnation, "from_generation", nr.IncarnationFrom)
	}
	return g.Generation, nil
}

// verifiedHintGeneration returns the generation of the current signed
// hint, or 0 when hints are unsigned or unreadable.
func (a *Activator) verifiedHintGeneration() uint64 {
	if a.hints == nil || a.spec.HintPrefix == "" || len(a.opt.HintKey) == 0 {
		return 0
	}
	ctx, cancel := context.WithTimeout(context.Background(), ProgressReadTimeout)
	defer cancel()
	h, ok, err := a.hints.Read(ctx, a.spec.HintPrefix)
	if (err != nil && !errors.Is(err, wakehint.ErrFallback)) || !ok {
		return 0
	}
	return h.Generation
}

func entryOf(id string, r journal.Record) bootfloor.Entry {
	return bootfloor.Entry{
		Installation: id, Incarnation: r.Incarnation, IncarnationFrom: r.IncarnationFrom,
		Generation: r.Generation, Epoch: r.ProgressEpoch, Seq: r.ProgressSeq,
	}
}

// issueCredentials adds this generation's scoped credentials to env.
func (a *Activator) issueCredentials(gen uint64, env map[string]string) error {
	if a.opt.Credentials == nil {
		return fmt.Errorf("workload %q has boot_credentials but the agent has no credential issuer", a.spec.ID)
	}
	ctx, cancel := context.WithTimeout(context.Background(), CredentialIssueTimeout)
	defer cancel()
	t0 := time.Now()
	creds, err := a.opt.Credentials.Issue(ctx, a.spec, gen)
	if err != nil {
		return err
	}
	for k, v := range creds {
		env[k] = v
	}
	a.log.Info("boot credentials issued", "generation", gen, "session", bootcred.SessionName(a.spec.ID, gen), "ms", time.Since(t0).Milliseconds())
	return nil
}

// recordProgress reads the hint and durably keeps its progress if higher.
func (a *Activator) recordProgress(ctx context.Context) error {
	if a.hints == nil || a.spec.HintPrefix == "" {
		return nil
	}
	h, ok, err := a.hints.Read(ctx, a.spec.HintPrefix)
	if errors.Is(err, wakehint.ErrFallback) {
		err = nil // the last authentic hint's progress still counts
	}
	if err != nil || !ok {
		return err
	}
	return a.noteProgress(h)
}

func (a *Activator) noteProgress(h wakehint.Hint) error {
	if h.Progress == nil {
		return nil
	}
	// Progress comes only from signed hints (the spec requires a hint key
	// with progress_env_key). It is also bounded: a hint naming a generation
	// this host never issued is from another host or a misbehaving guest,
	// and would otherwise block every boot.
	r, err := a.journal.Get(a.spec.ID)
	if err != nil {
		return err
	}
	if h.Progress.Epoch > r.Generation || h.Generation > r.Generation {
		a.log.Warn("ignoring hint progress naming a generation never issued; check the hint store",
			"hint_generation", h.Generation, "epoch", h.Progress.Epoch, "issued", r.Generation)
		return nil
	}
	// After an intentional rollback, a hint published before the new
	// incarnation's first generation describes the discarded data.
	if h.Generation < r.IncarnationFrom {
		return nil
	}
	prev, up, err := a.journal.AdvanceProgress(a.spec.ID, h.Progress.Epoch, h.Progress.Seq)
	if up && h.Progress.Epoch != prev.Epoch {
		a.log.Info("progress epoch advanced", "epoch", h.Progress.Epoch, "seq", h.Progress.Seq)
	}
	if err == nil && up && a.opt.Floor != nil {
		r.ProgressEpoch, r.ProgressSeq = h.Progress.Epoch, h.Progress.Seq
		a.opt.Floor.Note(entryOf(a.spec.ID, r))
	}
	return err
}

// readProgress records the hint's progress before a boot. An unreadable
// hint does not block the wake: the journal still holds the last progress
// seen.
func (a *Activator) readProgress() {
	if a.spec.ProgressEnvKey == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), ProgressReadTimeout)
	err := a.recordProgress(ctx)
	cancel()
	if err != nil {
		a.log.Warn("hint unreadable before boot; using recorded progress", "err", err)
	}
}

// progressEnv sets the progress key from the journal, after the grant
// (which may have started a new incarnation) was adopted.
func (a *Activator) progressEnv(env map[string]string) error {
	if a.spec.ProgressEnvKey == "" {
		return nil
	}
	r, err := a.journal.Get(a.spec.ID)
	if err != nil {
		return err
	}
	if r.ProgressEpoch != 0 || r.ProgressSeq != 0 {
		env[a.spec.ProgressEnvKey] = fmt.Sprintf("%d:%d", r.ProgressEpoch, r.ProgressSeq)
	}
	return nil
}

// watchExit returns the workload to Asleep if its guest ends on its own
// (crash, OOM, guest-initiated exit). After any exit it records the final
// hint's progress, while the store is least likely to have been altered.
func (a *Activator) watchExit(g guest.Guest, gen uint64) {
	defer a.exits.Done()
	<-g.Exited()
	a.mu.Lock()
	if a.guest == g && a.state == Awake {
		a.state, a.guest = Asleep, nil
		a.stats.UnexpectedExits++
		a.log.Warn("guest exited unexpectedly", "generation", gen)
	}
	a.mu.Unlock()
	// After the state change, so a dead guest is never handed out while
	// the store is read. A boot that starts meanwhile reads it itself.
	if a.spec.ProgressEnvKey != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := a.recordProgress(ctx); err != nil {
			a.log.Warn("recording progress after exit", "err", err)
		}
		if a.opt.Floor != nil {
			a.opt.Floor.Kick() // report at guest exit and clean stop
		}
		cancel()
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
	a.readProgressWhileAwake(ctx)
	a.mu.Lock()
	state, active, gen := a.state, a.active, a.generation
	idleFor, upFor := time.Since(a.lastActive), time.Since(a.awakeSince)
	pollDue := time.Since(a.lastHintAt) >= a.opt.HintPoll
	a.mu.Unlock()

	switch {
	case state == Awake && active == 0 && idleFor >= a.spec.IdleTimeout.Duration && upFor >= a.spec.MinAwake.Duration:
		busy, err := a.busy(ctx, gen)
		a.mu.Lock()
		if err != nil {
			a.hintErrs++
		} else {
			a.hintErrs = 0
		}
		errs := a.hintErrs
		veto := busy || (err != nil && errs <= maxHintErrors)
		if veto {
			a.stats.BusyVetoes++
			a.lastActive = time.Now() // re-check after another idle period
		}
		a.mu.Unlock()
		switch {
		case veto && err != nil:
			a.log.Warn("idle stop deferred: hint unreadable", "attempt", errs, "of", maxHintErrors, "err", err)
			return
		case veto:
			a.log.Info("idle stop vetoed: busy", "generation", gen)
			return
		case err != nil:
			a.log.Warn("hint still unreadable; stopping anyway (hints are advisory)", "err", err)
		}
		a.stopIfStillIdle()
	case state == Asleep && a.hints != nil && a.spec.HintPrefix != "" && pollDue:
		a.mu.Lock()
		a.lastHintAt = time.Now()
		a.mu.Unlock()
		h, ok, err := a.hints.Read(ctx, a.spec.HintPrefix)
		// A fallback is the last authentic hint: a sleeping workload's hint
		// does not change, so it still schedules the wake.
		if (err != nil && !errors.Is(err, wakehint.ErrFallback)) || !ok {
			return
		}
		if a.spec.ProgressEnvKey != "" {
			if err := a.noteProgress(h); err != nil {
				a.log.Warn("recording progress", "err", err)
			}
		}
		a.mu.Lock()
		dueWake := h.NextDue != nil && !time.Now().Before(h.NextDue.Add(-a.spec.ScheduledLead.Duration)) && !h.NextDue.Equal(a.wokeDue)
		busyWake := h.Busy && (!a.wokeBusy || h.Generation > a.wokeBusyGen)
		if (dueWake || busyWake) && a.state == Asleep && !a.closed && !time.Now().Before(a.hintWakeAfter) {
			if busyWake {
				a.wokeBusy, a.wokeBusyGen = true, h.Generation
			}
			if dueWake {
				a.wokeDue = *h.NextDue
			}
			a.log.Info("scheduled wake", "busy", h.Busy, "hint_generation", h.Generation, "next_due", h.NextDue)
			a.beginWake(true)
		}
		a.mu.Unlock()
	}
}

// readProgressWhileAwake reads an awake workload's hint once per FloorRead,
// so its progress reaches the controller while it runs.
func (a *Activator) readProgressWhileAwake(ctx context.Context) {
	if a.opt.Floor == nil || a.spec.ProgressEnvKey == "" {
		return
	}
	every := a.opt.FloorRead
	if every <= 0 {
		every = bootfloor.DefaultInterval
	}
	a.mu.Lock()
	due := a.state == Awake && time.Since(a.lastFloorRead) >= every
	if due {
		a.lastFloorRead = time.Now()
	}
	a.mu.Unlock()
	if !due {
		return
	}
	rctx, cancel := context.WithTimeout(ctx, ProgressReadTimeout)
	defer cancel()
	if err := a.recordProgress(rctx); err != nil {
		a.log.Warn("reading progress while awake", "err", err)
	}
}

// hintBackoff is the delay before a hint may wake the workload again after
// n consecutive failed scheduled wakes: the poll interval, doubled per
// failure, capped at 5 minutes.
func hintBackoff(n int, poll time.Duration) time.Duration {
	d := poll
	for i := 1; i < n && d < 5*time.Minute; i++ {
		d *= 2
	}
	return min(d, 5*time.Minute)
}

// busy reads the current generation's hint. A hint from an older
// generation says nothing about this boot and is treated as not busy.
func (a *Activator) busy(ctx context.Context, gen uint64) (bool, error) {
	if a.hints == nil || a.spec.HintPrefix == "" {
		return false, nil
	}
	h, ok, err := a.hints.Read(ctx, a.spec.HintPrefix)
	if err != nil {
		// Including a fallback: an awake workload republishes, so a stored
		// hint that keeps being refused counts toward maxHintErrors rather
		// than vetoing the stop on an old "busy" forever.
		return false, err
	}
	if ok && a.spec.ProgressEnvKey != "" {
		if err := a.noteProgress(h); err != nil {
			a.log.Warn("recording progress", "err", err)
		}
	}
	return ok && h.Generation == gen && h.Busy, nil
}

func (a *Activator) stopIfStillIdle() {
	a.mu.Lock()
	if a.state != Awake || a.active != 0 || time.Since(a.lastActive) < a.spec.IdleTimeout.Duration {
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

// shutdown refuses new leases and wakes, then stops the guest.
func (a *Activator) shutdown() {
	a.mu.Lock()
	a.closed = true
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
	// Wait for the exit watchers to record the final progress (and note it
	// for the controller), so Run returns only after that and the host's
	// last report carries it. Each is bounded by its hint read timeout.
	a.exits.Wait()
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
