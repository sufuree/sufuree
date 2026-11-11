package executor

import (
	"context"
	"errors"
	"fmt"
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

type uncertainExec struct {
	Memory
}

func (u *uncertainExec) CreateHost(context.Context, CreateHostInput) (ProviderRef, error) {
	return ProviderRef{}, fmt.Errorf("%w: timed out", ErrOutcomeUnknown)
}

func TestGatedKeepsReservationOnUnknown(t *testing.T) {
	g := &Gated{
		Inner:  &uncertainExec{Memory: Memory{AllowEffects: true}},
		Ledger: &budget.Ledger{Grant: budget.Grant{DeploymentUID: "d1", MaxHosts: 1, MaxMemoryMiB: 512}},
	}
	if _, err := g.CreateHost(context.Background(), CreateHostInput{OperationID: "c1", MemoryMiB: 256}); !errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("err = %v", err)
	}
	h, _ := g.Ledger.Usage()
	if h != 1 {
		t.Fatalf("hosts = %d, want reservation retained", h)
	}
}

func TestGatedTerminateReleasesAdoptedHost(t *testing.T) {
	inner := &Memory{AllowEffects: true}
	g := &Gated{
		Inner:  inner,
		Ledger: &budget.Ledger{Grant: budget.Grant{DeploymentUID: "d1", MaxHosts: 1, MaxMemoryMiB: 512}},
	}
	// Simulate unknown create: reservation held, host exists, but Gated never saw success.
	if err := g.Ledger.ReserveCreate("c-adopt", 256); err != nil {
		t.Fatal(err)
	}
	if err := g.Ledger.Confirm("c-adopt"); err != nil {
		t.Fatal(err)
	}
	ref, err := inner.CreateHost(context.Background(), CreateHostInput{OperationID: "c-adopt", MemoryMiB: 256})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.TerminateHost(context.Background(), TerminateHostInput{OperationID: "t-adopt", ProviderID: ref.ProviderID}); err != nil {
		t.Fatal(err)
	}
	h, _ := g.Ledger.Usage()
	if h != 0 {
		t.Fatalf("hosts = %d after adopt terminate", h)
	}
}
