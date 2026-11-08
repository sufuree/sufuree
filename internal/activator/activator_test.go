package activator

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

func (l *fakeLauncher) Boot(ctx context.Context, w workload.Spec, gen uint64, env map[string]string) (guest.Guest, error) {
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
	mu sync.Mutex
	h  wakehint.Hint
	ok bool
}

func (f *fakeHints) set(h wakehint.Hint) {
	f.mu.Lock()
	f.h, f.ok = h, true
	f.mu.Unlock()
}

func (f *fakeHints) Read(context.Context, string) (wakehint.Hint, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.h, f.ok, nil
}

func spec() workload.Spec {
	return workload.Spec{
		ID: "w1", Image: "img", MemoryMiB: 128, GuestPort: 8080, StateMiB: 64,
		BootEnvFile: "zati.env", GenerationEnvKey: "GEN",
		BootEnv:        map[string]string{"SECRET": "env:TEST_SECRET", "PLAIN": "x"},
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
