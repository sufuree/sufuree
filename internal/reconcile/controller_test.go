package reconcile

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sufuree/sufuree/internal/demand"
	"github.com/sufuree/sufuree/internal/executor"
	"github.com/sufuree/sufuree/internal/resource"
	"github.com/sufuree/sufuree/internal/store"
)

func TestConcurrentDemandOneCapacityAndIdleZero(t *testing.T) {
	t.Parallel()
	base := time.Now().UTC().Truncate(time.Millisecond)
	cur := base
	mem := &executor.Memory{AllowEffects: true}
	exec := &counting{Memory: mem}
	c, st := newController(t, exec, func() time.Time { return cur }, time.Hour)
	ctx := context.Background()
	pool := applyPool(t, c)
	app := applyApp(t, c)
	mgr := demand.New(demand.Bolt{Store: st})
	c.demand = mgr
	deadline := time.Now().Add(time.Hour)
	const n = 16
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, _, err := mgr.Join(ctx, app.UID, "req", deadline); err != nil {
				t.Errorf("join: %v", err)
			}
		}()
	}
	wg.Wait()
	claims := listClaims(t, st)
	if len(claims) != 1 || claims[0].ResourceUID != app.UID {
		t.Fatalf("claims = %+v", claims)
	}
	if err := c.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if exec.n.Load() != 1 {
		t.Fatalf("create calls = %d, want 1", exec.n.Load())
	}
	hosts := mustHosts(t, c)
	placements := listPlacements(t, st)
	if len(hosts) != 1 || len(placements) != 1 || len(mem.Running()) != 1 {
		t.Fatalf("hosts=%d placements=%d running=%d", len(hosts), len(placements), len(mem.Running()))
	}
	if placements[0].ResourceUID != app.UID || placements[0].HostUID != hosts[0].UID {
		t.Fatalf("placement %+v host %+v", placements[0], hosts[0])
	}
	// A single-replica app is one installation: its replica is its UID on
	// every host, so its storage and boot floor follow it.
	if placements[0].Replica != app.UID {
		t.Fatalf("replica = %q, want the resource UID %q", placements[0].Replica, app.UID)
	}
	if pool.UID != hosts[0].PoolUID {
		t.Fatalf("host pool = %s", hosts[0].PoolUID)
	}
	cur = cur.Add(2 * time.Millisecond)
	if err := c.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if len(mem.Running()) != 0 || len(mustHosts(t, c)) != 0 {
		t.Fatalf("pool did not reach zero: running=%d hosts=%d", len(mem.Running()), len(mustHosts(t, c)))
	}
	if exec.n.Load() != 1 {
		t.Fatalf("create calls after scale-in = %d", exec.n.Load())
	}
}

func TestJournalFailureDoesNotDispatch(t *testing.T) {
	t.Parallel()
	exec := &counting{Memory: &executor.Memory{AllowEffects: true}}
	st := openStore(t)
	putMeta(t, st)
	ready := New(st, exec, nil, Options{Now: time.Now, Logger: discard()})
	applyPool(t, ready)
	app := applyApp(t, ready)
	deadline := time.Now().Add(time.Hour)
	if _, _, err := demand.New(demand.Bolt{Store: st}).Join(context.Background(), app.UID, "req", deadline); err != nil {
		t.Fatal(err)
	}
	c := New(failDB{st}, exec, nil, Options{Now: time.Now, Logger: discard()})
	err := c.ReconcileOnce(context.Background())
	if err == nil {
		t.Fatal("expected journal error")
	}
	if exec.n.Load() != 0 {
		t.Fatalf("CreateHost calls = %d", exec.n.Load())
	}
	if ops := listOps(t, st); len(ops) != 0 {
		t.Fatalf("operations recorded despite failed journal: %+v", ops)
	}
}

func TestCreateTimeoutStaysUnknown(t *testing.T) {
	t.Parallel()
	exec := &timeoutExec{}
	c, st := newController(t, exec, time.Now, time.Hour)
	ctx := context.Background()
	applyPool(t, c)
	app := applyApp(t, c)
	if _, _, err := demand.New(demand.Bolt{Store: st}).Join(ctx, app.UID, "req", time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := c.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if exec.n.Load() != 1 {
		t.Fatalf("calls = %d", exec.n.Load())
	}
	ops := listOps(t, st)
	if len(ops) != 1 || ops[0].Outcome != resource.OutcomeUnknown {
		t.Fatalf("ops = %+v", ops)
	}
	if err := c.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if exec.n.Load() != 1 {
		t.Fatalf("timeout was retried, calls = %d", exec.n.Load())
	}
	if hosts := mustHosts(t, c); len(hosts) != 0 {
		t.Fatalf("unknown create recorded a host: %+v", hosts)
	}
}

func TestStaleResourceVersionRejected(t *testing.T) {
	t.Parallel()
	c, _ := newController(t, &executor.Memory{}, time.Now, 0)
	ctx := context.Background()
	got := applyApp(t, c)
	if got.ResourceVersion != 1 || got.Generation != 1 {
		t.Fatalf("created = %+v", got)
	}
	again, err := c.ApplyResource(ctx, got)
	if err != nil {
		t.Fatal(err)
	}
	if again.ResourceVersion != got.ResourceVersion || again.Generation != got.Generation {
		t.Fatalf("identical apply changed identity: %+v", again)
	}
	next := got
	spec := got.Spec.(resource.AppSpec)
	spec.Image = "img-b"
	next.Spec = spec
	updated, err := c.ApplyResource(ctx, next)
	if err != nil {
		t.Fatal(err)
	}
	if updated.ResourceVersion != 2 || updated.Generation != 2 {
		t.Fatalf("updated = %+v", updated)
	}
	stale := next
	stale.ResourceVersion = 1
	spec.Image = "img-c"
	stale.Spec = spec
	_, err = c.ApplyResource(ctx, stale)
	if !errors.Is(err, store.ErrStaleResourceVersion) {
		t.Fatalf("stale apply err = %v", err)
	}
}

func TestAPIConflictAndLists(t *testing.T) {
	t.Parallel()
	c, _ := newController(t, &executor.Memory{}, time.Now, 0)
	srv := httptest.NewServer(Handler(c))
	t.Cleanup(srv.Close)
	body := []byte(`{"name":"web","kind":"App","lifecycle":"enabled","spec":{"image":"img","memory_mib":128,"guest_port":8080,"idle_timeout":"60s","startup_timeout":"10s","min_replicas":0,"max_replicas":1}}`)
	req, err := http.NewRequest(http.MethodPut, srv.URL+"/v1/resources/App/web", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("create status = %d", res.StatusCode)
	}
	req, err = http.NewRequest(http.MethodPut, srv.URL+"/v1/resources/App/web", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("If-Match", "1")
	req.Header.Set("Content-Type", "application/json")
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("idempotent status = %d", res.StatusCode)
	}
	req, err = http.NewRequest(http.MethodPut, srv.URL+"/v1/resources/App/web", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("If-Match", "0")
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusConflict {
		t.Fatalf("stale status = %d", res.StatusCode)
	}
	res, err = http.Get(srv.URL + "/v1/resources")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("list status = %d", res.StatusCode)
	}
}

type counting struct {
	*executor.Memory
	n atomic.Int32
}

func (c *counting) CreateHost(ctx context.Context, in executor.CreateHostInput) (executor.ProviderRef, error) {
	c.n.Add(1)
	return c.Memory.CreateHost(ctx, in)
}

type timeoutExec struct {
	n atomic.Int32
	executor.Executor
}

func (t *timeoutExec) CreateHost(context.Context, executor.CreateHostInput) (executor.ProviderRef, error) {
	t.n.Add(1)
	return executor.ProviderRef{}, context.DeadlineExceeded
}

func (t *timeoutExec) Describe(context.Context, string) (executor.HostDesc, error) {
	return executor.HostDesc{}, executor.ErrNotFound
}

type failDB struct{ *store.Store }

func (f failDB) Update(func(*store.Tx) error) error { return errors.New("disk full") }

func newController(t *testing.T, exec executor.Executor, now func() time.Time, backoff time.Duration) (*Controller, *store.Store) {
	t.Helper()
	st := openStore(t)
	putMeta(t, st)
	if te, ok := exec.(*timeoutExec); ok && te.Executor == nil {
		te.Executor = &executor.Memory{}
	}
	c := New(st, exec, nil, Options{Now: now, Backoff: backoff, Logger: discard()})
	return c, st
}

func openStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func putMeta(t *testing.T, st *store.Store) {
	t.Helper()
	err := st.Update(func(tx *store.Tx) error {
		return tx.PutMeta(resource.ClusterMeta{DeploymentUID: "dep1", RecoveryIncarnation: 1, OwnerEpoch: 1})
	})
	if err != nil {
		t.Fatal(err)
	}
}

func applyPool(t *testing.T, c *Controller) resource.Resource {
	t.Helper()
	got, err := c.ApplyResource(context.Background(), resource.Resource{
		Name: "main", Kind: resource.KindPool, Lifecycle: resource.LifecycleEnabled,
		Spec: resource.PoolSpec{
			MinHosts: 0, MaxHosts: 4,
			QuietPeriod: resource.Duration{Duration: time.Millisecond},
			WarmMode:    resource.WarmOff,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func applyApp(t *testing.T, c *Controller) resource.Resource {
	t.Helper()
	got, err := c.ApplyResource(context.Background(), resource.Resource{
		Name: "web", Kind: resource.KindApp, Lifecycle: resource.LifecycleEnabled,
		Spec: resource.AppSpec{
			Image: "img", MemoryMiB: 128, GuestPort: 8080,
			IdleTimeout:    resource.Duration{Duration: time.Millisecond},
			StartupTimeout: resource.Duration{Duration: time.Second},
			MinReplicas:    0, MaxReplicas: 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func mustHosts(t *testing.T, c *Controller) []resource.Host {
	t.Helper()
	hosts, err := c.ListHosts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return hosts
}

func listOps(t *testing.T, st *store.Store) []resource.Operation {
	t.Helper()
	var ops []resource.Operation
	err := st.View(func(tx *store.Tx) error {
		got, err := tx.ListOperations()
		ops = got
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return ops
}

func listClaims(t *testing.T, st *store.Store) []resource.ActivationClaim {
	t.Helper()
	var claims []resource.ActivationClaim
	err := st.View(func(tx *store.Tx) error {
		got, err := tx.ListClaims()
		claims = got
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return claims
}

func listPlacements(t *testing.T, st *store.Store) []resource.Placement {
	t.Helper()
	var out []resource.Placement
	err := st.View(func(tx *store.Tx) error {
		got, err := tx.ListPlacements()
		out = got
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func discard() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestReplicaIDIsStableOnlyForSingleReplicaApps(t *testing.T) {
	app := func(uid string, max int) resource.Resource {
		return resource.Resource{UID: uid, Kind: resource.KindApp, Spec: resource.AppSpec{MaxReplicas: max}}
	}
	w := world{apps: []resource.Resource{app("one", 1), app("many", 4)}}
	if got, err := replicaID(w, "one"); err != nil || got != "one" {
		t.Fatalf("single-replica app: %q, %v", got, err)
	}
	a, _ := replicaID(w, "many")
	b, _ := replicaID(w, "many")
	if a == b || a == "many" || a[0] != 'r' {
		t.Fatalf("multi-replica app replicas %q, %q are not fresh", a, b)
	}
}
