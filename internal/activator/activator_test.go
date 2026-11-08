package activator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sufuree/sufuree/internal/bootcred"
	"github.com/sufuree/sufuree/internal/guest"
	"github.com/sufuree/sufuree/internal/journal"
	"github.com/sufuree/sufuree/internal/wakehint"
	"github.com/sufuree/sufuree/internal/workload"
)

type fakeGuest struct {
	addr   string
	exited chan struct{}
	once   sync.Once
	stops  atomic.Int32
}

func (g *fakeGuest) Addr() string            { return g.addr }
func (g *fakeGuest) Exited() <-chan struct{} { return g.exited }
func (g *fakeGuest) Stop(time.Duration) error {
	g.stops.Add(1)
	g.once.Do(func() { close(g.exited) })
	return nil
}

type fakeLauncher struct {
	mu     sync.Mutex
	boots  []uint64
	envs   []map[string]string
	guests []*fakeGuest
	delay  time.Duration
}

func (l *fakeLauncher) Boot(ctx context.Context, w workload.Spec, gen uint64, env map[string]string, files map[string][]byte) (guest.Guest, error) {
	if string(files["key"]) != "s3cret" {
		return nil, errors.New("boot file not resolved")
	}
	time.Sleep(l.delay)
	g := &fakeGuest{addr: "guest-" + env[w.GenerationEnvKey], exited: make(chan struct{})}
	l.mu.Lock()
	l.boots = append(l.boots, gen)
	l.envs = append(l.envs, env)
	l.guests = append(l.guests, g)
	l.mu.Unlock()
	return g, nil
}

func (l *fakeLauncher) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.boots)
}

type fakeHints struct {
	mu    sync.Mutex
	h     wakehint.Hint
	ok    bool
	err   error
	reads int
}

func (f *fakeHints) set(h wakehint.Hint) {
	f.mu.Lock()
	f.h, f.ok = h, true
	f.mu.Unlock()
}

func (f *fakeHints) Read(context.Context, string) (wakehint.Hint, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads++
	return f.h, f.ok, f.err
}

func spec() workload.Spec {
	return workload.Spec{
		ID: "w1", Image: "img", MemoryMiB: 128, GuestPort: 8080, StateMiB: 64,
		BootEnvFile: "zati.env", GenerationEnvKey: "GEN",
		BootEnv:        map[string]string{"SECRET": "env:TEST_SECRET", "PLAIN": "x"},
		BootFiles:      map[string]string{"key": "env:TEST_SECRET"},
		HintPrefix:     "inst/w1",
		IdleTimeout:    workload.Duration{Duration: 50 * time.Millisecond},
		StartupTimeout: workload.Duration{Duration: 300 * time.Millisecond},
	}
}

func newAct(t *testing.T, l guest.Launcher, h wakehint.Reader, ready func(context.Context, string) error) (*Activator, *journal.Journal) {
	t.Helper()
	j, err := journal.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if ready == nil {
		ready = func(context.Context, string) error { return nil }
	}
	a := New(spec(), j, l, h, Options{
		Tick: 10 * time.Millisecond, HintPoll: 10 * time.Millisecond, StopGrace: 10 * time.Millisecond,
		Getenv: func(k string) (string, bool) {
			if k == "TEST_SECRET" {
				return "s3cret", true
			}
			return "", false
		},
		Ready: ready,
	})
	return a, j
}

func run(t *testing.T, a *Activator) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	go a.Run(ctx)
	t.Cleanup(func() { cancel(); <-a.Done() })
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestConcurrentAcquireBootsOnce(t *testing.T) {
	l := &fakeLauncher{delay: 50 * time.Millisecond}
	a, _ := newAct(t, l, nil, nil)
	var wg sync.WaitGroup
	addrs := make(chan string, 20)
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			addr, release, err := a.Acquire(context.Background())
			if err != nil {
				t.Error(err)
				return
			}
			addrs <- addr
			release()
		}()
	}
	wg.Wait()
	close(addrs)
	if n := l.count(); n != 1 {
		t.Fatalf("boots = %d, want 1", n)
	}
	for addr := range addrs {
		if addr != "guest-1" {
			t.Fatalf("addr = %q", addr)
		}
	}
	if l.envs[0]["SECRET"] != "s3cret" || l.envs[0]["PLAIN"] != "x" || l.envs[0]["GEN"] != "1" {
		t.Fatalf("boot env = %v", l.envs[0])
	}
}

func TestIdleStopThenRewakeUsesNewGeneration(t *testing.T) {
	l := &fakeLauncher{}
	a, j := newAct(t, l, nil, nil)
	run(t, a)
	_, release, err := a.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(120 * time.Millisecond)
	if st, _, _, _ := a.Snapshot(); st != Awake {
		t.Fatalf("stopped while a lease was held: %s", st)
	}
	release()
	waitFor(t, "idle stop", func() bool { st, _, _, _ := a.Snapshot(); return st == Asleep })
	if l.guests[0].stops.Load() == 0 {
		t.Fatal("guest was not stopped")
	}
	addr, release2, err := a.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer release2()
	if addr != "guest-2" {
		t.Fatalf("rewake addr = %q, want generation 2", addr)
	}
	if r, _ := j.Get("w1"); r.Generation != 2 {
		t.Fatalf("journal generation = %d", r.Generation)
	}
}

func TestBusyHintVetoesIdleStopOnlyForCurrentGeneration(t *testing.T) {
	l := &fakeLauncher{}
	h := &fakeHints{}
	a, _ := newAct(t, l, h, nil)
	h.set(wakehint.Hint{Version: 1, Generation: 1, Busy: true})
	_, release, err := a.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	release()
	run(t, a)
	time.Sleep(200 * time.Millisecond)
	st, _, _, stats := a.Snapshot()
	if st != Awake || stats.BusyVetoes == 0 {
		t.Fatalf("state %s vetoes %d: busy hint did not hold the guest", st, stats.BusyVetoes)
	}
	h.set(wakehint.Hint{Version: 1, Generation: 1, Busy: false})
	waitFor(t, "stop after work finished", func() bool { st, _, _, _ := a.Snapshot(); return st == Asleep })

	// A stale busy hint from generation 1 must not hold generation 2 up.
	h.set(wakehint.Hint{Version: 1, Generation: 1, Busy: true})
	_, release, err = a.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	release()
	// (While asleep, the same busy hint triggers a scheduled wake, which is
	// correct for interrupted work; here we only assert generation 2 stops.)
	waitFor(t, "stale hint ignored", func() bool {
		_, gen, _, stats := a.Snapshot()
		return gen >= 2 && stats.IdleStops >= 2
	})
}

func TestScheduledWakeFromNextDue(t *testing.T) {
	l := &fakeLauncher{}
	h := &fakeHints{}
	a, _ := newAct(t, l, h, nil)
	future := time.Now().Add(time.Hour)
	h.set(wakehint.Hint{Version: 1, Generation: 0, NextDue: &future})
	run(t, a)
	time.Sleep(100 * time.Millisecond)
	if l.count() != 0 {
		t.Fatal("woke for work due in an hour")
	}
	past := time.Now().Add(-time.Second)
	h.set(wakehint.Hint{Version: 1, Generation: 0, NextDue: &past})
	waitFor(t, "scheduled wake", func() bool { return l.count() >= 1 })
	_, _, _, stats := a.Snapshot()
	if stats.ScheduledWakes == 0 {
		t.Fatal("wake was not counted as scheduled")
	}
}

func TestNotReadyRetriesUnderNewGenerationThenFails(t *testing.T) {
	l := &fakeLauncher{}
	a, _ := newAct(t, l, nil, func(context.Context, string) error { return errors.New("no") })
	_, _, err := a.Acquire(context.Background())
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
	if got := l.boots; len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("boots = %v, want generations [1 2]", got)
	}
	for i, g := range l.guests {
		if g.stops.Load() == 0 {
			t.Fatalf("unready guest %d was not stopped", i)
		}
	}
	if st, _, active, _ := a.Snapshot(); st != Asleep || active != 0 {
		t.Fatalf("after failure: state %s active %d", st, active)
	}
}

func TestUnexpectedExitReturnsToAsleep(t *testing.T) {
	l := &fakeLauncher{}
	a, _ := newAct(t, l, nil, nil)
	_, release, err := a.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	release()
	_ = l.guests[0].Stop(0) // the guest dies on its own
	waitFor(t, "asleep after crash", func() bool { st, _, _, _ := a.Snapshot(); return st == Asleep })
	addr, release, err := a.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	release()
	if addr != "guest-2" {
		t.Fatalf("addr after crash = %q", addr)
	}
}

func TestAcquireHonoursContext(t *testing.T) {
	l := &fakeLauncher{delay: 200 * time.Millisecond}
	a, _ := newAct(t, l, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, _, err := a.Acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	if _, _, active, _ := a.Snapshot(); active != 0 {
		t.Fatalf("lease leaked: active %d", active)
	}
}

type fakeIssuer struct {
	mu   sync.Mutex
	gens []uint64
	err  error
}

func (f *fakeIssuer) Issue(_ context.Context, w workload.Spec, gen uint64) (map[string]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gens = append(f.gens, gen)
	if f.err != nil {
		return nil, f.err
	}
	return map[string]string{w.BootCredentials.Env.SessionToken: fmt.Sprintf("tok-%d", gen)}, nil
}

func credSpec() workload.Spec {
	s := spec()
	s.BootCredentials = &workload.AWSRole{RoleARN: "arn:aws:iam::1:role/g", Region: "r", SessionTags: map[string]string{"t": "v"},
		Env: workload.AWSRoleEnv{AccessKeyID: "AK", SecretAccessKey: "SK", SessionToken: "ST"}}
	return s
}

func newCredAct(t *testing.T, l guest.Launcher, iss bootcred.Issuer) *Activator {
	t.Helper()
	j, err := journal.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	opt := Options{
		Tick: 10 * time.Millisecond, StopGrace: 10 * time.Millisecond, Attempts: 1,
		Getenv: func(k string) (string, bool) { return "s3cret", k == "TEST_SECRET" },
		Ready:  func(context.Context, string) error { return nil },
	}
	if iss != nil {
		opt.Credentials = iss
	}
	return New(credSpec(), j, l, nil, opt)
}

func TestEachBootGetsItsOwnCredentials(t *testing.T) {
	l := &fakeLauncher{}
	iss := &fakeIssuer{}
	a := newCredAct(t, l, iss)
	run(t, a)
	for i := 0; i < 2; i++ {
		_, release, err := a.Acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		release()
		waitFor(t, "idle stop", func() bool { s, _, _, _ := a.Snapshot(); return s == Asleep })
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.envs) != 2 || l.envs[0]["ST"] != "tok-1" || l.envs[1]["ST"] != "tok-2" || iss.gens[1] != l.boots[1] {
		t.Fatalf("boots %v issued %v envs %v", l.boots, iss.gens, l.envs)
	}
}

func TestBootFailsClosedWithoutCredentials(t *testing.T) {
	for name, iss := range map[string]bootcred.Issuer{"no issuer": nil, "denied": &fakeIssuer{err: errors.New("AccessDenied")}} {
		l := &fakeLauncher{}
		a := newCredAct(t, l, iss)
		run(t, a)
		if _, _, err := a.Acquire(context.Background()); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("%s: err = %v", name, err)
		}
		if l.count() != 0 {
			t.Fatalf("%s: guest booted without credentials", name)
		}
	}
}

func TestNoLeaseOrWakeAfterShutdown(t *testing.T) {
	l := &fakeLauncher{}
	a, _ := newAct(t, l, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	go a.Run(ctx)
	_, release, err := a.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	release()
	cancel()
	<-a.Done()
	waitFor(t, "guest stopped at shutdown", func() bool { st, _, _, _ := a.Snapshot(); return st == Asleep })
	if _, _, err := a.Acquire(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Acquire after shutdown: err = %v", err)
	}
	if l.count() != 1 {
		t.Fatalf("booted %d guests; a wake after shutdown would leak one", l.count())
	}
	if _, _, active, _ := a.Snapshot(); active != 0 {
		t.Fatalf("lease leaked: active %d", active)
	}
}

func TestStaleBusyHintWakesOnlyOnce(t *testing.T) {
	l := &fakeLauncher{}
	h := &fakeHints{}
	a, _ := newAct(t, l, h, nil)
	// Generation 7 crashed while busy and nothing republishes the hint.
	h.set(wakehint.Hint{Version: 1, Generation: 7, Busy: true})
	run(t, a)
	waitFor(t, "one wake for the interrupted work", func() bool { return l.count() == 1 })
	waitFor(t, "idle stop of that boot", func() bool { _, _, _, s := a.Snapshot(); return s.IdleStops == 1 })
	time.Sleep(300 * time.Millisecond)
	if l.count() != 1 {
		t.Fatalf("stale busy hint woke the workload %d times", l.count())
	}
	// A newer generation's busy hint wakes it again.
	h.set(wakehint.Hint{Version: 1, Generation: 8, Busy: true})
	waitFor(t, "wake for a newer busy hint", func() bool { return l.count() == 2 })
}

func TestUnreadableHintDefersIdleStopBoundedly(t *testing.T) {
	l := &fakeLauncher{}
	h := &fakeHints{err: errors.New("store down")}
	a, _ := newAct(t, l, h, nil)
	_, release, err := a.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	release()
	run(t, a)
	waitFor(t, "stop despite an unreadable hint", func() bool { st, _, _, _ := a.Snapshot(); return st == Asleep })
	if _, _, _, s := a.Snapshot(); s.BusyVetoes != maxHintErrors {
		t.Fatalf("vetoes = %d, want %d", s.BusyVetoes, maxHintErrors)
	}
}

func TestFailedHintWakeIsRetried(t *testing.T) {
	l := &fakeLauncher{}
	h := &fakeHints{}
	// Both attempts of the first wake (generations 1 and 2) never become
	// ready; later boots do.
	a, _ := newAct(t, l, h, func(_ context.Context, addr string) error {
		if addr == "guest-1" || addr == "guest-2" {
			return errors.New("not ready")
		}
		return nil
	})
	h.set(wakehint.Hint{Version: 1, Generation: 3, Busy: true})
	run(t, a)
	waitFor(t, "a retried wake after the first one failed", func() bool {
		st, _, _, s := a.Snapshot()
		return s.FailedBoots >= 2 && (st == Awake || s.IdleStops > 0)
	})
}

func TestStaleNextDueWakesOnce(t *testing.T) {
	l := &fakeLauncher{}
	h := &fakeHints{}
	a, _ := newAct(t, l, h, nil)
	past := time.Now().Add(-time.Minute)
	// The woken guest never moves next_due forward.
	h.set(wakehint.Hint{Version: 1, Generation: 1, NextDue: &past})
	run(t, a)
	waitFor(t, "one wake for the due work", func() bool { return l.count() == 1 })
	waitFor(t, "idle stop", func() bool { _, _, _, s := a.Snapshot(); return s.IdleStops == 1 })
	time.Sleep(300 * time.Millisecond)
	if l.count() != 1 {
		t.Fatalf("a stale next_due woke the workload %d times", l.count())
	}
	next := time.Now().Add(-time.Second)
	h.set(wakehint.Hint{Version: 1, Generation: 1, NextDue: &next})
	waitFor(t, "wake for a new next_due", func() bool { return l.count() == 2 })
}
