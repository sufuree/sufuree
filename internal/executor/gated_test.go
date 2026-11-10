package executor

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sufuree/sufuree/internal/budget"
)

func TestGatedCreateRespectsGrant(t *testing.T) {
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	inner := &Memory{AllowEffects: true}
	g := &Gated{
		Inner: inner,
		Ledger: &budget.Ledger{
			Grant: budget.Grant{DeploymentUID: "d1", MaxHosts: 1, MaxMemoryMiB: 512, Expires: now.Add(time.Hour)},
			Now:   func() time.Time { return now },
		},
	}
	ref, err := g.CreateHost(context.Background(), CreateHostInput{OperationID: "c1", MemoryMiB: 256})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.CreateHost(context.Background(), CreateHostInput{OperationID: "c2", MemoryMiB: 256}); !errors.Is(err, budget.ErrDenied) {
		t.Fatalf("err = %v", err)
	}
	if err := g.TerminateHost(context.Background(), TerminateHostInput{OperationID: "t1", ProviderID: ref.ProviderID}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.CreateHost(context.Background(), CreateHostInput{OperationID: "c2", MemoryMiB: 256}); err != nil {
		t.Fatal(err)
	}
}

func TestGatedCreateFailureReleases(t *testing.T) {
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	inner := &Memory{} // effects disabled
	g := &Gated{
		Inner: inner,
		Ledger: &budget.Ledger{
			Grant: budget.Grant{DeploymentUID: "d1", MaxHosts: 1, MaxMemoryMiB: 512},
			Now:   func() time.Time { return now },
		},
	}
	if _, err := g.CreateHost(context.Background(), CreateHostInput{OperationID: "c1", MemoryMiB: 256}); !errors.Is(err, ErrEffectsDisabled) {
		t.Fatalf("err = %v", err)
	}
	h, _ := g.Ledger.Usage()
	if h != 0 {
		t.Fatalf("hosts = %d", h)
	}
}
