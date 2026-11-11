package reconcile

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sufuree/sufuree/internal/demand"
	"github.com/sufuree/sufuree/internal/executor"
	"github.com/sufuree/sufuree/internal/resource"
	"github.com/sufuree/sufuree/internal/store"
)

type fakeLease struct {
	addr     string
	held     atomic.Int32
	acquires atomic.Int32
}

func (f *fakeLease) Acquire(context.Context) (string, func(), error) {
	f.acquires.Add(1)
	f.held.Add(1)
	return f.addr, func() { f.held.Add(-1) }, nil
}

func TestEdgeActivatorHostLeaseHeldUntilRelease(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(dir + "/control.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.Update(func(tx *store.Tx) error {
		return tx.PutMeta(resource.ClusterMeta{DeploymentUID: "d", RecoveryIncarnation: 1, OwnerEpoch: 1})
	}); err != nil {
		t.Fatal(err)
	}
	dem := demand.New(demand.Bolt{Store: st})
	c := New(st, &executor.Memory{AllowEffects: true}, dem, Options{
		Tick: time.Second, Backoff: time.Hour, Now: time.Now,
	})
	ctx := context.Background()
	if _, err := c.ApplyResource(ctx, resource.Resource{
		Name: "main", Kind: resource.KindPool, Lifecycle: resource.LifecycleEnabled,
		Spec: resource.PoolSpec{MinHosts: 0, MaxHosts: 1, QuietPeriod: resource.Duration{Duration: time.Hour}, WarmMode: resource.WarmOff},
	}); err != nil {
		t.Fatal(err)
	}
	app, err := c.ApplyResource(ctx, resource.Resource{
		Name: "web", Kind: resource.KindApp, Lifecycle: resource.LifecycleEnabled,
		Spec: resource.AppSpec{
			Image: "img", MemoryMiB: 64, GuestPort: 8080,
			IdleTimeout: resource.Duration{Duration: time.Hour}, StartupTimeout: resource.Duration{Duration: time.Second},
			MinReplicas: 0, MaxReplicas: 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := dem.Join(ctx, app.UID, "claim", time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := c.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	lease := &fakeLease{addr: "127.0.0.1:9"}
	act := &EdgeActivator{
		Controller: c, Demand: dem,
		HostLeases: map[string]HostLease{"web": lease},
		Wait:       3 * time.Second,
	}
	req := httptest.NewRequest(http.MethodGet, "http://edge/svc/web/", nil)
	errc := make(chan error, 1)
	go func() {
		// Handoff after Acquire has joined a fresh claim.
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			if err := c.AcknowledgeReadyPlacements(ctx); err != nil {
				errc <- err
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		errc <- nil
	}()
	addr, release, err := act.Acquire(req, "web")
	if err != nil {
		t.Fatal(err)
	}
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	if addr != "127.0.0.1:9" || lease.held.Load() != 1 || lease.acquires.Load() != 1 {
		t.Fatalf("addr=%q held=%d acquires=%d", addr, lease.held.Load(), lease.acquires.Load())
	}
	release()
	if lease.held.Load() != 0 {
		t.Fatalf("lease held after release = %d", lease.held.Load())
	}
}
