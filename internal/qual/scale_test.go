package qual_test

import (
	"context"
	"testing"
	"time"

	"github.com/sufuree/sufuree/internal/budget"
	"github.com/sufuree/sufuree/internal/demand"
	"github.com/sufuree/sufuree/internal/executor"
	"github.com/sufuree/sufuree/internal/reconcile"
	"github.com/sufuree/sufuree/internal/resource"
	"github.com/sufuree/sufuree/internal/store"
)

// TestLocalZeroOneZeroWithGrant is the no-AWS stand-in for RFC sequence §3:
// demand → one capacity create under a growth grant → placement → idle →
// terminate → ledger back to zero. Live EC2 remains opt-in (see cloud path doc).
func TestLocalZeroOneZeroWithGrant(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(dir + "/control.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Update(func(tx *store.Tx) error {
		return tx.PutMeta(resource.ClusterMeta{
			DeploymentUID: "qual-z10", RecoveryIncarnation: 1, OwnerEpoch: 1,
		})
	}); err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 11, 10, 15, 0, 0, 0, time.UTC)
	cur := now
	ledger := &budget.Ledger{
		Grant: budget.Grant{DeploymentUID: "qual-z10", MaxHosts: 2, MaxMemoryMiB: 2048, Expires: now.Add(time.Hour)},
		Now:   func() time.Time { return cur },
	}
	exec := &executor.Gated{Inner: &executor.Memory{AllowEffects: true}, Ledger: ledger}
	dem := demand.New(demand.Bolt{Store: st})
	c := reconcile.New(st, exec, dem, reconcile.Options{
		Tick: time.Second, Backoff: time.Hour, Now: func() time.Time { return cur },
	})
	ctx := context.Background()

	if _, err := c.ApplyResource(ctx, resource.Resource{
		Name: "main", Kind: resource.KindPool, Lifecycle: resource.LifecycleEnabled,
		Spec: resource.PoolSpec{
			MinHosts: 0, MaxHosts: 2,
			QuietPeriod: resource.Duration{Duration: time.Millisecond},
			WarmMode:    resource.WarmOff,
		},
	}); err != nil {
		t.Fatal(err)
	}
	app, err := c.ApplyResource(ctx, resource.Resource{
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
	if _, _, err := dem.Join(ctx, app.UID, "qual", time.Now().UTC().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := c.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	hosts, err := c.ListHosts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(hosts) != 1 {
		t.Fatalf("hosts after scale-up = %d", len(hosts))
	}
	h, mem := ledger.Usage()
	if h != 1 || mem != 128 {
		t.Fatalf("ledger after create = hosts=%d mem=%d", h, mem)
	}

	cur = cur.Add(2 * time.Millisecond)
	if err := c.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	hosts, err = c.ListHosts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(hosts) != 0 {
		t.Fatalf("hosts after scale-to-zero = %d", len(hosts))
	}
	h, mem = ledger.Usage()
	if h != 0 || mem != 0 {
		t.Fatalf("ledger after terminate = hosts=%d mem=%d", h, mem)
	}
}
