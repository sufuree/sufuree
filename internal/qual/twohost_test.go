package qual_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/sufuree/sufuree/internal/budget"
	"github.com/sufuree/sufuree/internal/demand"
	"github.com/sufuree/sufuree/internal/executor"
	"github.com/sufuree/sufuree/internal/reconcile"
	"github.com/sufuree/sufuree/internal/resource"
	"github.com/sufuree/sufuree/internal/store"
)

// TestLocalTwoHostDrainWithGrant is the no-AWS stand-in for RFC sequence §4:
// admitted demand that cannot pack onto one host → bounded two-host placement →
// idle drain → scale-to-zero, with the growth ledger accounting for both hosts
// and releasing them only after termination. Live two-host EC2 drain remains
// opt-in (see cloud path doc).
func TestLocalTwoHostDrainWithGrant(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(dir + "/control.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Update(func(tx *store.Tx) error {
		return tx.PutMeta(resource.ClusterMeta{
			DeploymentUID: "qual-2host", RecoveryIncarnation: 1, OwnerEpoch: 1,
		})
	}); err != nil {
		t.Fatal(err)
	}

	now := time.Date(2026, 11, 10, 16, 0, 0, 0, time.UTC)
	cur := now
	ledger := &budget.Ledger{
		Grant: budget.Grant{DeploymentUID: "qual-2host", MaxHosts: 2, MaxMemoryMiB: 2048, Expires: now.Add(time.Hour)},
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
	// Two apps whose 128 MiB reservations cannot pack onto a single host sized
	// to the first workload, forcing a second host.
	for _, name := range []string{"web1", "web2"} {
		app, err := c.ApplyResource(ctx, resource.Resource{
			Name: name, Kind: resource.KindApp, Lifecycle: resource.LifecycleEnabled,
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
		if _, _, err := dem.Join(ctx, app.UID, "qual-"+name, time.Now().UTC().Add(time.Hour)); err != nil {
			t.Fatal(err)
		}
	}

	// Reconcile until both hosts exist (one capacity request per pass).
	if err := reconcileUntilHosts(ctx, c, 2, &cur); err != nil {
		t.Fatalf("scale-up: %v", err)
	}
	if h, mem := ledger.Usage(); h != 2 || mem != 256 {
		t.Fatalf("ledger after two-host create = hosts=%d mem=%d", h, mem)
	}

	// Let both guests go idle and both hosts drain to zero.
	cur = cur.Add(5 * time.Millisecond)
	if err := reconcileUntilHosts(ctx, c, 0, &cur); err != nil {
		t.Fatalf("scale-to-zero: %v", err)
	}
	if h, mem := ledger.Usage(); h != 0 || mem != 0 {
		t.Fatalf("ledger after terminate = hosts=%d mem=%d", h, mem)
	}
}

// reconcileUntilHosts advances the fake clock and reconciles until the host
// count reaches want, bounded so a stuck planner fails the test instead of
// looping forever.
func reconcileUntilHosts(ctx context.Context, c *reconcile.Controller, want int, cur *time.Time) error {
	for i := 0; i < 40; i++ {
		if err := c.ReconcileOnce(ctx); err != nil {
			return err
		}
		hosts, err := c.ListHosts(ctx)
		if err != nil {
			return err
		}
		if len(hosts) == want {
			return nil
		}
		*cur = cur.Add(2 * time.Millisecond)
	}
	hosts, _ := c.ListHosts(ctx)
	return fmt.Errorf("host count = %d, want %d after bounded reconciles", len(hosts), want)
}
