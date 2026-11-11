package activator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sufuree/sufuree/internal/bootfloor"
	"github.com/sufuree/sufuree/internal/journal"
	"github.com/sufuree/sufuree/internal/wakehint"
)

// fakeFloor is a controller that grants from a counter.
type fakeFloor struct {
	mu      sync.Mutex
	gen     uint64
	inc     uint64
	incFrom uint64
	epoch   uint64
	seq     uint64
	err     error
	locals  []bootfloor.Entry
	notes   []bootfloor.Entry
	kicks   int
}

func (f *fakeFloor) Boot(_ context.Context, id string, local bootfloor.Entry) (bootfloor.Grant, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.locals = append(f.locals, local)
	if f.err != nil {
		return bootfloor.Grant{}, f.err
	}
	f.gen = max(f.gen, local.Generation) + 1
	return bootfloor.Grant{Installation: id, Generation: f.gen, Incarnation: f.inc, IncarnationFrom: f.incFrom,
		ProgressEpoch: f.epoch, ProgressSeq: f.seq}, nil
}

func (f *fakeFloor) Note(e bootfloor.Entry) {
	f.mu.Lock()
	f.notes = append(f.notes, e)
	f.mu.Unlock()
}

func (f *fakeFloor) Kick() {
	f.mu.Lock()
	f.kicks++
	f.mu.Unlock()
}

func newFloorAct(t *testing.T, l *fakeLauncher, h wakehint.Reader, f *fakeFloor) (*Activator, *journal.Journal) {
	t.Helper()
	j, err := journal.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	w := spec()
	w.ProgressEnvKey = "MIN"
	a := New(w, j, l, h, Options{
		Tick: time.Hour, HintPoll: time.Hour, StopGrace: 10 * time.Millisecond, Attempts: 1,
		Getenv: func(k string) (string, bool) { return "s3cret", k == "TEST_SECRET" },
		Ready:  func(context.Context, string) error { return nil },
		Floor:  f,
	})
	run(t, a)
	return a, j
}

func wakeAndStop(t *testing.T, a *Activator, l *fakeLauncher) (uint64, map[string]string, error) {
	t.Helper()
	_, release, err := a.Acquire(context.Background())
	if err != nil {
		return 0, nil, err
	}
	release()
	time.Sleep(a.spec.IdleTimeout.Duration + 10*time.Millisecond)
	a.stopIfStillIdle()
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.boots[len(l.boots)-1], l.envs[len(l.envs)-1], nil
}

func TestControllerGrantsEveryGeneration(t *testing.T) {
	l, h := &fakeLauncher{}, &fakeHints{}
	f := &fakeFloor{gen: 40, inc: 1, epoch: 30, seq: 4}
	a, j := newFloorAct(t, l, h, f)
	gen, env, err := wakeAndStop(t, a, l)
	if err != nil || gen != 41 || env["MIN"] != "30:4" {
		t.Fatalf("first boot = gen %d, MIN %q, %v; want 41, 30:4", gen, env["MIN"], err)
	}
	if r, _ := j.Get("w1"); r.Generation != 41 || r.Incarnation != 1 {
		t.Fatalf("journal = %+v", r)
	}
	// Progress from a signed hint is recorded and noted for the report.
	h.set(wakehint.Hint{Version: 2, Generation: 41, Seq: 1, Progress: &wakehint.Progress{Epoch: 41, Seq: 9}})
	gen, env, err = wakeAndStop(t, a, l)
	if err != nil || gen != 42 || env["MIN"] != "41:9" {
		t.Fatalf("second boot = gen %d, MIN %q, %v", gen, env["MIN"], err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if last := f.locals[len(f.locals)-1]; last.Generation != 41 || last.Incarnation != 1 {
		t.Fatalf("the boot request did not carry the host's record: %+v", last)
	}
	if len(f.notes) == 0 || f.notes[len(f.notes)-1].Epoch != 41 || f.notes[len(f.notes)-1].Seq != 9 {
		t.Fatalf("progress was not noted for the report: %+v", f.notes)
	}
	waitFor(t, "an exit report kick", func() bool { return f.kicks > 0 })
}

func TestBootFailsClosedWithoutAGrant(t *testing.T) {
	l := &fakeLauncher{}
	f := &fakeFloor{err: errors.New("controller unreachable")}
	a, j := newFloorAct(t, l, &fakeHints{}, f)
	if _, _, err := a.Acquire(context.Background()); err == nil {
		t.Fatal("booted without a controller grant")
	}
	if l.count() != 0 {
		t.Fatal("a guest was launched")
	}
	if r, _ := j.Get("w1"); r.Generation != 0 {
		t.Fatalf("a generation was issued locally: %+v", r)
	}
}

func TestGrantNotAboveTheHostIsRefused(t *testing.T) {
	l := &fakeLauncher{}
	f := &fakeFloor{inc: 1}
	a, j := newFloorAct(t, l, &fakeHints{}, f)
	for range 5 {
		if _, err := j.NextGeneration("w1"); err != nil {
			t.Fatal(err)
		}
	}
	// A controller that ignores the host's record would grant 1.
	f.Boot(context.Background(), "w1", bootfloor.Entry{}) // gen = 1
	f.mu.Lock()
	f.gen = 0
	f.mu.Unlock()
	a.opt.Floor = &ignoreLocal{f}
	if _, _, err := a.Acquire(context.Background()); err == nil {
		t.Fatal("booted with a generation not above the host's record")
	}
	if l.count() != 0 {
		t.Fatal("a guest was launched")
	}
}

type ignoreLocal struct{ *fakeFloor }

func (i *ignoreLocal) Boot(ctx context.Context, id string, _ bootfloor.Entry) (bootfloor.Grant, error) {
	return i.fakeFloor.Boot(ctx, id, bootfloor.Entry{})
}

func TestRollbackIgnoresHintsFromBeforeIt(t *testing.T) {
	l, h := &fakeLauncher{}, &fakeHints{}
	f := &fakeFloor{gen: 10, inc: 1}
	a, _ := newFloorAct(t, l, h, f)
	h.set(wakehint.Hint{Version: 2, Generation: 11, Seq: 1, Progress: &wakehint.Progress{Epoch: 11, Seq: 50}})
	if _, _, err := wakeAndStop(t, a, l); err != nil {
		t.Fatal(err)
	}
	// The owner rolled back to 5:2; the new incarnation starts at 12.
	f.mu.Lock()
	f.inc, f.incFrom, f.epoch, f.seq = 2, 12, 5, 2
	f.mu.Unlock()
	// The store still holds the pre-rollback hint from generation 11.
	gen, env, err := wakeAndStop(t, a, l)
	if err != nil || gen != 12 || env["MIN"] != "5:2" {
		t.Fatalf("boot after rollback = gen %d, MIN %q, %v; want 12, 5:2", gen, env["MIN"], err)
	}
}

func TestBehindControllerRefusesTheBoot(t *testing.T) {
	l := &fakeLauncher{}
	f := &fakeFloor{err: bootfloor.ErrBehindHost}
	a, _ := newFloorAct(t, l, &fakeHints{}, f)
	if _, _, err := a.Acquire(context.Background()); !errors.Is(err, bootfloor.ErrBehindHost) && err == nil {
		t.Fatal("booted while the controller is behind")
	}
	if l.count() != 0 {
		t.Fatal("a guest was launched")
	}
}

// TestShutdownWaitsForTheFinalProgress: when the agent stops, Run returns
// only after the exit watcher has recorded and noted the final progress.
func TestShutdownWaitsForTheFinalProgress(t *testing.T) {
	l, h := &fakeLauncher{}, &fakeHints{}
	f := &fakeFloor{gen: 5, inc: 1}
	j, err := journal.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	w := spec()
	w.ProgressEnvKey = "MIN"
	a := New(w, j, l, h, Options{
		Tick: time.Hour, HintPoll: time.Hour, StopGrace: 10 * time.Millisecond, Attempts: 1,
		Getenv: func(k string) (string, bool) { return "s3cret", k == "TEST_SECRET" },
		Ready:  func(context.Context, string) error { return nil },
		Floor:  f,
	})
	ctx, cancel := context.WithCancel(context.Background())
	go a.Run(ctx)
	_, release, err := a.Acquire(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	release()
	h.set(wakehint.Hint{Version: 2, Generation: 6, Seq: 3, Progress: &wakehint.Progress{Epoch: 6, Seq: 77}})
	cancel()
	<-a.Done()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.notes) == 0 || f.notes[len(f.notes)-1].Seq != 77 || f.kicks == 0 {
		t.Fatalf("Run returned before the final progress was noted: notes %+v, kicks %d", f.notes, f.kicks)
	}
}

func TestBootSendsTheVerifiedHintGeneration(t *testing.T) {
	l, h := &fakeLauncher{}, &fakeHints{}
	f := &fakeFloor{inc: 1}
	a, _ := newFloorAct(t, l, h, f)
	a.opt.HintKeys = wakehint.NewKeyring()
	a.spec.HintKeyEnvKey = "HK"
	h.set(wakehint.Hint{Version: 2, Generation: 33, Seq: 1})
	if _, _, err := wakeAndStop(t, a, l); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.locals[0].Generation != 33 {
		t.Fatalf("the boot request did not carry the hint's generation: %+v", f.locals[0])
	}
}

// TestBootSendsProgressTheExitReadMissed: when the read after a guest's exit
// fails, the next boot reads the hint before asking for a generation, so the
// record sent to the controller carries that guest's final progress (with
// an open incarnation start, a later read could fall below the moved start).
func TestBootSendsProgressTheExitReadMissed(t *testing.T) {
	l, h, f := &fakeLauncher{}, &fakeHints{}, &fakeFloor{inc: 1}
	a, _ := newFloorAct(t, l, h, f)
	if _, _, err := wakeAndStop(t, a, l); err != nil { // generation 1
		t.Fatal(err)
	}
	h.mu.Lock()
	h.err = errors.New("store unavailable") // the exit-time read is missed
	h.mu.Unlock()
	if _, _, err := wakeAndStop(t, a, l); err != nil { // generation 2
		t.Fatal(err)
	}
	// Generation 2's guest published progress the agent hasn't read yet.
	h.mu.Lock()
	h.err = nil
	h.mu.Unlock()
	h.set(wakehint.Hint{Version: 1, Generation: 2, Progress: &wakehint.Progress{Epoch: 2, Seq: 7}})
	if _, _, err := wakeAndStop(t, a, l); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	last := f.locals[len(f.locals)-1]
	if last.Epoch != 2 || last.Seq != 7 {
		t.Fatalf("boot sent %+v, want the unread progress 2:7", last)
	}
}
