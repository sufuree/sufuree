package executor

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/sufuree/sufuree/internal/budget"
)

// HostFencer terminates a provider host and waits for terminal state.
type HostFencer interface {
	Fence(ctx context.Context, providerID string) error
}

// Gated wraps an Executor with a growth grant and capacity ledger.
// CreateHost reserves before the inner call. Reservations are released only
// when the create is known not to have run; ambiguous provider errors keep
// the reservation until reconciliation. Terminate prefers Fence when the
// inner executor implements HostFencer so grant capacity frees only after
// observed termination.
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
		if definiteNoCreate(err) {
			_ = g.Ledger.Release(in.OperationID)
		}
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

// TerminateHost terminates via Fence when available, else TerminateHost, then
// releases the create reservation. Adopted hosts (create succeeded in the
// provider but never recorded in createByProvider) resolve the create
// OperationID from Describe.
func (g *Gated) TerminateHost(ctx context.Context, in TerminateHostInput) error {
	if g == nil || g.Inner == nil || g.Ledger == nil {
		return errors.New("executor: gated ledger and inner are required")
	}
	if f, ok := g.Inner.(HostFencer); ok && in.ProviderID != "" {
		if err := f.Fence(ctx, in.ProviderID); err != nil {
			return err
		}
	} else if err := g.Inner.TerminateHost(ctx, in); err != nil {
		return err
	}
	createOp, err := g.createOperationID(ctx, in)
	if err != nil {
		return err
	}
	if err := g.Ledger.Release(createOp); err != nil && !errors.Is(err, budget.ErrUnknown) {
		return fmt.Errorf("executor: terminate succeeded but budget release failed: %w", err)
	}
	return nil
}

func (g *Gated) createOperationID(ctx context.Context, in TerminateHostInput) (string, error) {
	g.mu.Lock()
	createOp := g.createByProvider[in.ProviderID]
	if createOp != "" {
		delete(g.createByProvider, in.ProviderID)
		g.mu.Unlock()
		return createOp, nil
	}
	g.mu.Unlock()
	if in.ProviderID != "" {
		d, err := g.Inner.Describe(ctx, in.ProviderID)
		if err != nil {
			return "", fmt.Errorf("executor: terminated but cannot resolve create operation for budget release: %w", err)
		}
		if d.OperationID == "" {
			return "", errors.New("executor: terminated but host has no create operation id for budget release")
		}
		return d.OperationID, nil
	}
	if in.OperationID == "" {
		return "", errors.New("executor: terminate needs a create operation id for budget release")
	}
	return in.OperationID, nil
}

func definiteNoCreate(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrOutcomeUnknown) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return false
	}
	return true
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
