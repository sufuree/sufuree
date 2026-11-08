package store

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/sufuree/sufuree/internal/resource"
	"go.etcd.io/bbolt"
)

const digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

var (
	t0 = time.Date(2026, 11, 2, 15, 4, 5, 0, time.UTC)
	t1 = t0.Add(time.Second)
	t2 = t0.Add(2 * time.Second)
	t3 = t0.Add(3 * time.Second)
)

func exampleApp() resource.Resource {
	return resource.Resource{
		UID:        "app-1",
		Name:       "api",
		Generation: 1,
		Kind:       resource.KindApp,
		Lifecycle:  resource.LifecycleEnabled,
		Spec: resource.AppSpec{
			Image:          "/img/api",
			MemoryMiB:      512,
			GuestPort:      8080,
			IdleTimeout:    resource.Duration{Duration: 60 * time.Second},
			StartupTimeout: resource.Duration{Duration: 20 * time.Second},
			MinReplicas:    0,
			MaxReplicas:    4,
		},
		Status: resource.Status{
			ObservedGeneration: 1,
			Phase:              resource.PhaseDormant,
			Message:            "idle",
			UpdatedAt:          t0,
		},
	}
}

func examplePool() resource.Resource {
	return resource.Resource{
		UID:        "pool-1",
		Name:       "edge",
		Generation: 1,
		Kind:       resource.KindPool,
		Lifecycle:  resource.LifecycleEnabled,
		Spec: resource.PoolSpec{
			MinHosts:    0,
			MaxHosts:    2,
			QuietPeriod: resource.Duration{Duration: 300 * time.Second},
			WarmMode:    resource.WarmObserve,
		},
	}
}

func exampleOp(id, uid string, at time.Time) resource.Operation {
	return resource.Operation{
		OperationID:        id,
		ResourceUID:        uid,
		OwnerEpoch:         4,
		ExpectedGeneration: 1,
		Action:             "provision-host",
		InputDigest:        digest,
		Outcome:            resource.OutcomePrepared,
		CreatedAt:          at,
		UpdatedAt:          at,
	}
}

func exampleHost(pool string) resource.Host {
	return resource.Host{
		UID:        "host-1",
		PoolUID:    pool,
		ProviderID: "i-abc123",
		MemoryMiB:  2048,
		ObservedAt: t0,
	}
}

func examplePlacement(app, host string) resource.Placement {
	return resource.Placement{
		UID:         "place-1",
		ResourceUID: app,
		HostUID:     host,
		Replica:     "rep-1",
		VolumeUID:   "vol-1",
		Generation:  1,
		OwnerEpoch:  4,
		MemoryMiB:   512,
		Phase:       resource.PhaseStarting,
		UpdatedAt:   t0,
	}
}

func exampleClaim(app string) resource.ActivationClaim {
	return resource.ActivationClaim{
		UID:         "claim-1",
		ResourceUID: app,
		Generation:  1,
		Count:       2,
		CreatedAt:   t0,
		Deadline:    t0.Add(time.Minute),
	}
}

func newStore(t *testing.T) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "control.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func mustUpdate(t *testing.T, s *Store, fn func(*Tx) error) {
	t.Helper()
	if err := s.Update(fn); err != nil {
		t.Fatal(err)
	}
}

func TestWritesRequireSync(t *testing.T) {
	if _, err := Open(""); err == nil {
		t.Fatal("empty path accepted")
	}
	s := newStore(t)
	if s.db.NoSync {
		t.Fatal("NoSync is enabled")
	}
	if err := s.Update(nil); err == nil {
		t.Fatal("nil update accepted")
	}
	if err := s.View(nil); err == nil {
		t.Fatal("nil view accepted")
	}
	fi, err := os.Stat(s.path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %o, want 0600", fi.Mode().Perm())
	}
	err = s.View(func(tx *Tx) error {
		_, err := tx.GetMeta()
		return err
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing meta: %v", err)
	}
	if err := s.Update(func(tx *Tx) error {
		_, err := tx.BumpOwnerEpoch()
		return err
	}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("bump without meta: %v", err)
	}
}

func TestStateSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state", "control.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	meta := resource.ClusterMeta{DeploymentUID: "dep-1", RecoveryIncarnation: 2, OwnerEpoch: 4}
	var bumped resource.ClusterMeta
	var app, pool resource.Resource
	op := exampleOp("op-1", "app-1", t0)
	host := exampleHost("pool-1")
	place := examplePlacement("app-1", "host-1")
	claim := exampleClaim("app-1")
	mustUpdate(t, s, func(tx *Tx) error {
		if err := tx.PutMeta(meta); err != nil {
			return err
		}
		var err error
		if app, err = tx.ApplyResource(exampleApp()); err != nil {
			return err
		}
		if pool, err = tx.ApplyResource(examplePool()); err != nil {
			return err
		}
		if err := tx.PutHost(host); err != nil {
			return err
		}
		if err := tx.PutOperation(op); err != nil {
			return err
		}
		if err := tx.PutPlacement(place); err != nil {
			return err
		}
		if err := tx.PutClaim(claim); err != nil {
			return err
		}
		bumped, err = tx.BumpOwnerEpoch()
		return err
	})
	if app.ResourceVersion != 1 || pool.ResourceVersion != 1 {
		t.Fatalf("versions app=%d pool=%d", app.ResourceVersion, pool.ResourceVersion)
	}
	if bumped.OwnerEpoch != 5 || bumped.RecoveryIncarnation != 2 || bumped.DeploymentUID != "dep-1" {
		t.Fatalf("bumped meta %+v", bumped)
	}
	before := snapshot(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s2.Close() }()
	if s2.db.NoSync {
		t.Fatal("reopened with NoSync")
	}
	after := snapshot(t, s2)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("reopen changed state\nbefore %+v\nafter  %+v", before, after)
	}
	if after.meta.OwnerEpoch != 5 || after.op.OwnerEpoch != 4 {
		t.Fatalf("epoch rewrite: meta %d operation %d", after.meta.OwnerEpoch, after.op.OwnerEpoch)
	}
}

type snap struct {
	meta  resource.ClusterMeta
	app   resource.Resource
	pool  resource.Resource
	op    resource.Operation
	host  resource.Host
	place resource.Placement
	claim resource.ActivationClaim
}

func snapshot(t *testing.T, s *Store) snap {
	t.Helper()
	var out snap
	if err := s.View(func(tx *Tx) error {
		var err error
		if out.meta, err = tx.GetMeta(); err != nil {
			return err
		}
		if out.app, err = tx.GetResource("app-1"); err != nil {
			return err
		}
		if out.pool, err = tx.GetResource("pool-1"); err != nil {
			return err
		}
		if out.op, err = tx.GetOperation("op-1"); err != nil {
			return err
		}
		if out.host, err = tx.GetHost("host-1"); err != nil {
			return err
		}
		if out.place, err = tx.GetPlacement("place-1"); err != nil {
			return err
		}
		out.claim, err = tx.GetClaim("claim-1")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestApplyRejectsStaleResourceVersion(t *testing.T) {
	s := newStore(t)
	var created resource.Resource
	mustUpdate(t, s, func(tx *Tx) error {
		var err error
		created, err = tx.ApplyResource(exampleApp())
		return err
	})
	if created.ResourceVersion != 1 {
		t.Fatalf("created version %d", created.ResourceVersion)
	}
	updated := created
	updated.Lifecycle = resource.LifecycleSuspended
	var next resource.Resource
	mustUpdate(t, s, func(tx *Tx) error {
		var err error
		next, err = tx.ApplyResource(updated)
		return err
	})
	if next.ResourceVersion != 2 || next.Lifecycle != resource.LifecycleSuspended || next.Generation != 1 {
		t.Fatalf("updated %+v", next)
	}
	stale := created
	stale.Lifecycle = resource.LifecycleDeleting
	err := s.Update(func(tx *Tx) error {
		_, err := tx.ApplyResource(stale)
		return err
	})
	if !errors.Is(err, ErrStaleResourceVersion) {
		t.Fatalf("stale apply: %v", err)
	}
	missing := exampleApp()
	missing.UID = "app-2"
	missing.Name = "other"
	missing.ResourceVersion = 1
	err = s.Update(func(tx *Tx) error {
		_, err := tx.ApplyResource(missing)
		return err
	})
	if !errors.Is(err, ErrStaleResourceVersion) {
		t.Fatalf("create with version: %v", err)
	}
	var got resource.Resource
	if err := s.View(func(tx *Tx) error {
		var err error
		got, err = tx.GetResource("app-1")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if got.ResourceVersion != 2 || got.Lifecycle != resource.LifecycleSuspended {
		t.Fatalf("stored %+v", got)
	}
}

func TestGenerationTracksSpecChanges(t *testing.T) {
	s := newStore(t)
	var cur resource.Resource
	mustUpdate(t, s, func(tx *Tx) error {
		var err error
		cur, err = tx.ApplyResource(exampleApp())
		return err
	})
	cur.Lifecycle = resource.LifecycleSuspended
	mustUpdate(t, s, func(tx *Tx) error {
		var err error
		cur, err = tx.ApplyResource(cur)
		return err
	})
	if cur.Generation != 1 || cur.Lifecycle != resource.LifecycleSuspended {
		t.Fatalf("lifecycle %+v", cur)
	}
	bumped := cur
	spec := bumped.Spec.(resource.AppSpec)
	spec.MaxReplicas = 8
	bumped.Spec = spec
	err := s.Update(func(tx *Tx) error {
		_, err := tx.ApplyResource(bumped)
		return err
	})
	if !errors.Is(err, ErrGeneration) {
		t.Fatalf("spec change without generation: %v", err)
	}
	bumped.Generation = 2
	mustUpdate(t, s, func(tx *Tx) error {
		var err error
		cur, err = tx.ApplyResource(bumped)
		return err
	})
	if cur.Generation != 2 {
		t.Fatalf("generation %d", cur.Generation)
	}
	same := cur
	same.Generation = 3
	err = s.Update(func(tx *Tx) error {
		_, err := tx.ApplyResource(same)
		return err
	})
	if !errors.Is(err, ErrGeneration) {
		t.Fatalf("generation bump without spec change: %v", err)
	}
	deleting := cur
	deleting.Lifecycle = resource.LifecycleDeleting
	mustUpdate(t, s, func(tx *Tx) error {
		var err error
		cur, err = tx.ApplyResource(deleting)
		return err
	})
	spec = cur.Spec.(resource.AppSpec)
	spec.MaxReplicas = 3
	bad := cur
	bad.Spec = spec
	bad.Generation = cur.Generation + 1
	err = s.Update(func(tx *Tx) error {
		_, err := tx.ApplyResource(bad)
		return err
	})
	if !errors.Is(err, ErrLifecycle) {
		t.Fatalf("spec change while deleting: %v", err)
	}
}

func TestListOpenOperations(t *testing.T) {
	s := newStore(t)
	mustUpdate(t, s, func(tx *Tx) error {
		if err := tx.PutMeta(resource.ClusterMeta{DeploymentUID: "dep-1", RecoveryIncarnation: 1, OwnerEpoch: 4}); err != nil {
			return err
		}
		if _, err := tx.ApplyResource(exampleApp()); err != nil {
			return err
		}
		for _, id := range []string{"op-a", "op-b", "op-c", "op-d", "op-e"} {
			if err := tx.PutOperation(exampleOp(id, "app-1", t0)); err != nil {
				return err
			}
		}
		if err := advance(tx, "op-b", resource.OutcomeDispatched, t1); err != nil {
			return err
		}
		if err := advance(tx, "op-c", resource.OutcomeUnknown, t1); err != nil {
			return err
		}
		if err := advance(tx, "op-d", resource.OutcomeDispatched, t1); err != nil {
			return err
		}
		if err := advance(tx, "op-d", resource.OutcomeConfirmed, t2); err != nil {
			return err
		}
		return advance(tx, "op-e", resource.OutcomeFailed, t1)
	})
	var got []resource.Operation
	if err := s.View(func(tx *Tx) error {
		var err error
		got, err = tx.ListOpenOperations()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	want := []string{"op-a", "op-b", "op-c"}
	if len(got) != len(want) {
		t.Fatalf("open = %d, want %d (%v)", len(got), len(want), ids(got))
	}
	for i, id := range want {
		if got[i].OperationID != id || !got[i].Open() {
			t.Fatalf("open[%d] = %+v", i, got[i])
		}
	}
	err := s.Update(func(tx *Tx) error {
		op, err := tx.GetOperation("op-a")
		if err != nil {
			return err
		}
		op.Outcome = resource.OutcomeConfirmed
		op.UpdatedAt = t1
		return tx.PutOperation(op)
	})
	if !errors.Is(err, ErrOutcome) {
		t.Fatalf("prepared confirmed directly: %v", err)
	}
	err = s.Update(func(tx *Tx) error { return tx.DeleteOperation("op-b") })
	if !errors.Is(err, ErrInUse) {
		t.Fatalf("delete open: %v", err)
	}
	mustUpdate(t, s, func(tx *Tx) error { return tx.DeleteOperation("op-d") })
	err = s.Update(func(tx *Tx) error {
		op, err := tx.GetOperation("op-a")
		if err != nil {
			return err
		}
		op.InputDigest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
		op.UpdatedAt = t1
		return tx.PutOperation(op)
	})
	if !errors.Is(err, ErrIdentity) {
		t.Fatalf("digest change: %v", err)
	}
}

func advance(tx *Tx, id string, outcome resource.Outcome, at time.Time) error {
	op, err := tx.GetOperation(id)
	if err != nil {
		return err
	}
	op.Outcome = outcome
	op.UpdatedAt = at
	return tx.PutOperation(op)
}

func ids(ops []resource.Operation) []string {
	out := make([]string, len(ops))
	for i, op := range ops {
		out[i] = op.OperationID
	}
	return out
}

func TestOperationCreateMatchesEpochAndGeneration(t *testing.T) {
	s := newStore(t)
	mustUpdate(t, s, func(tx *Tx) error {
		if err := tx.PutMeta(resource.ClusterMeta{DeploymentUID: "dep-1", RecoveryIncarnation: 1, OwnerEpoch: 1}); err != nil {
			return err
		}
		_, err := tx.ApplyResource(exampleApp())
		return err
	})
	op := exampleOp("op-1", "app-1", t0)
	op.OwnerEpoch = 2
	err := s.Update(func(tx *Tx) error { return tx.PutOperation(op) })
	if !errors.Is(err, ErrRegression) {
		t.Fatalf("stale epoch: %v", err)
	}
	op.OwnerEpoch = 1
	op.ExpectedGeneration = 9
	err = s.Update(func(tx *Tx) error { return tx.PutOperation(op) })
	if !errors.Is(err, ErrGeneration) {
		t.Fatalf("stale generation: %v", err)
	}
	op.ExpectedGeneration = 1
	op.Outcome = resource.OutcomeConfirmed
	err = s.Update(func(tx *Tx) error { return tx.PutOperation(op) })
	if !errors.Is(err, ErrOutcome) {
		t.Fatalf("create confirmed: %v", err)
	}
	op.Outcome = resource.OutcomePrepared
	mustUpdate(t, s, func(tx *Tx) error { return tx.PutOperation(op) })
}

func TestOwnerEpochDoesNotMoveBackward(t *testing.T) {
	s := newStore(t)
	base := resource.ClusterMeta{DeploymentUID: "dep-1", RecoveryIncarnation: 1, OwnerEpoch: 1}
	mustUpdate(t, s, func(tx *Tx) error { return tx.PutMeta(base) })
	var bumped resource.ClusterMeta
	mustUpdate(t, s, func(tx *Tx) error {
		var err error
		bumped, err = tx.BumpOwnerEpoch()
		return err
	})
	if bumped.OwnerEpoch != 2 {
		t.Fatalf("epoch %d", bumped.OwnerEpoch)
	}
	back := base
	err := s.Update(func(tx *Tx) error { return tx.PutMeta(back) })
	if !errors.Is(err, ErrRegression) {
		t.Fatalf("epoch backward: %v", err)
	}
	inc := bumped
	inc.RecoveryIncarnation = 2
	err = s.Update(func(tx *Tx) error { return tx.PutMeta(inc) })
	if !errors.Is(err, ErrRegression) {
		t.Fatalf("incarnation without epoch: %v", err)
	}
	inc.OwnerEpoch = 3
	mustUpdate(t, s, func(tx *Tx) error { return tx.PutMeta(inc) })
	renamed := inc
	renamed.DeploymentUID = "dep-2"
	renamed.OwnerEpoch = 4
	err = s.Update(func(tx *Tx) error { return tx.PutMeta(renamed) })
	if !errors.Is(err, ErrIdentity) {
		t.Fatalf("deployment rename: %v", err)
	}
	if err := s.View(func(tx *Tx) error {
		got, err := tx.GetMeta()
		if err != nil {
			return err
		}
		if got != inc {
			return fmt.Errorf("meta %+v", got)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestOpenRejectsForeignSchema(t *testing.T) {
	s := newStore(t)
	path := s.path
	if err := s.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket([]byte("meta")).Put([]byte("schema"), []byte("2"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); err == nil {
		t.Fatal("opened schema 2")
	}
	if _, err := Open(path); err == nil {
		t.Fatal("second open reset the database")
	}
}

func TestCorruptResourceIsNotReturned(t *testing.T) {
	s := newStore(t)
	bad := exampleApp()
	bad.Generation = 0
	buf, err := resource.Encode(bad)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.db.Update(func(tx *bbolt.Tx) error {
		if err := tx.Bucket([]byte("resources")).Put([]byte("app-1"), []byte("{")); err != nil {
			return err
		}
		return tx.Bucket([]byte("resources")).Put([]byte("app-2"), buf)
	}); err != nil {
		t.Fatal(err)
	}
	err = s.View(func(tx *Tx) error {
		if _, err := tx.GetResource("app-1"); err == nil {
			return errors.New("corrupt record returned")
		}
		if _, err := tx.GetResource("app-2"); err == nil {
			return errors.New("invalid record returned")
		}
		_, err := tx.ListResources()
		if err == nil {
			return errors.New("list skipped corrupt records")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestNameTakenAndConcurrentApplies(t *testing.T) {
	s := newStore(t)
	mustUpdate(t, s, func(tx *Tx) error {
		_, err := tx.ApplyResource(exampleApp())
		return err
	})
	other := exampleApp()
	other.UID = "app-2"
	err := s.Update(func(tx *Tx) error {
		_, err := tx.ApplyResource(other)
		return err
	})
	if !errors.Is(err, ErrNameTaken) {
		t.Fatalf("duplicate name: %v", err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := exampleApp()
			r.UID = fmt.Sprintf("app-%d", i+10)
			r.Name = fmt.Sprintf("api-%d", i+10)
			errs <- s.Update(func(tx *Tx) error {
				_, err := tx.ApplyResource(r)
				return err
			})
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Error(err)
		}
	}
}
