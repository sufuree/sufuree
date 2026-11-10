package executor

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/sufuree/sufuree/internal/budget"
)

// Gated wraps an Executor with a growth grant and capacity ledger.
// CreateHost reserves before the inner call and releases on failure.
// After a successful create, the create OperationID is remembered by
// provider ID so TerminateHost can free the correct reservation.
type Gated struct {
	Inner  Executor
	Ledger *budget.Ledger

	mu               sync.Mutex
	createByProvider map[string]string
}

var _ Executor = (*Gated)(nil)

// Describe delegates.
func (g *Gated) Describe(ctx context.Context, id string) (HostDesc, error) {
	if g == nil || g.Inner == nil {
		return HostDesc{}, errors.New("executor: gated inner is required")
	}
	return g.Inner.Describe(ctx, id)
}

// CreateHost reserves capacity, then calls the inner executor.
func (g *Gated) CreateHost(ctx context.Context, in CreateHostInput) (ProviderRef, error) {
	if g == nil || g.Inner == nil || g.Ledger == nil {
		return ProviderRef{}, errors.New("executor: gated ledger and inner are required")
	}
	if err := g.Ledger.ReserveCreate(in.OperationID, in.MemoryMiB); err != nil {
		return ProviderRef{}, err
	}
	ref, err := g.Inner.CreateHost(ctx, in)
	if err != nil {
		_ = g.Ledger.Release(in.OperationID)
		return ProviderRef{}, err
	}
	if err := g.Ledger.Confirm(in.OperationID); err != nil {
		return ref, fmt.Errorf("executor: create succeeded but budget confirm failed: %w", err)
	}
	g.mu.Lock()
	if g.createByProvider == nil {
		g.createByProvider = map[string]string{}
	}
	g.createByProvider[ref.ProviderID] = in.OperationID
	g.mu.Unlock()
	return ref, nil
}

// TerminateHost terminates via the inner executor, then releases the create
// reservation for that provider when known.
func (g *Gated) TerminateHost(ctx context.Context, in TerminateHostInput) error {
	if g == nil || g.Inner == nil || g.Ledger == nil {
		return errors.New("executor: gated ledger and inner are required")
	}
	if err := g.Inner.TerminateHost(ctx, in); err != nil {
		return err
	}
	g.mu.Lock()
	createOp := g.createByProvider[in.ProviderID]
	if createOp != "" {
		delete(g.createByProvider, in.ProviderID)
	}
	g.mu.Unlock()
	if createOp == "" {
		createOp = in.OperationID
	}
	if err := g.Ledger.Release(createOp); err != nil && !errors.Is(err, budget.ErrUnknown) {
		return fmt.Errorf("executor: terminate succeeded but budget release failed: %w", err)
	}
	return nil
}

// AttachVolume delegates.
func (g *Gated) AttachVolume(ctx context.Context, in AttachVolumeInput) error {
	return g.Inner.AttachVolume(ctx, in)
}

// DetachVolume delegates.
func (g *Gated) DetachVolume(ctx context.Context, in DetachVolumeInput) error {
	return g.Inner.DetachVolume(ctx, in)
}

// CreateSnapshot delegates.
func (g *Gated) CreateSnapshot(ctx context.Context, in SnapshotInput) error {
	return g.Inner.CreateSnapshot(ctx, in)
}

// RestoreSnapshot delegates.
func (g *Gated) RestoreSnapshot(ctx context.Context, in RestoreInput) error {
	return g.Inner.RestoreSnapshot(ctx, in)
}

// ExpireSnapshot delegates.
func (g *Gated) ExpireSnapshot(ctx context.Context, in ExpireInput) error {
	return g.Inner.ExpireSnapshot(ctx, in)
}
