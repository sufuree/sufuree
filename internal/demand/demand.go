// Package demand records activation claims and coalesces concurrent demand
// for one resource UID into a single pending claim. The edge may forward
// only after AcknowledgeHandoff. Cancelling the shared claim fails every
// waiter; it does not invent a completed handoff.
package demand

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	// StatePending is waiting for a placement handoff.
	StatePending = "pending"
	// StateHandedOff means the edge may forward for this claim.
	StateHandedOff = "handed_off"
	// StateCancelled means the caller withdrew the claim.
	StateCancelled = "cancelled"
)

// ErrNotFound is a missing claim.
var ErrNotFound = errors.New("demand: claim not found")

// ErrCancelled is delivered to waiters when the shared claim is cancelled.
var ErrCancelled = errors.New("demand: claim cancelled")

// ErrConflict means the claim is not in the state the transition requires.
var ErrConflict = errors.New("demand: claim state conflict")

// Claim is one durable activation claim.
type Claim struct {
	ClaimID             string
	ResourceUID         string
	RequestID           string
	Deadline            time.Time
	State               string
	PlacementGeneration uint64
}

// Result is the handoff outcome. Err is set when the claim is cancelled or
// rejected. A nil Err with a positive PlacementGeneration is permission to
// forward; an empty result is not.
type Result struct {
	ClaimID             string
	PlacementGeneration uint64
	Err                 error
}

// Tx is the claim mutation surface of one store transaction.
// DeleteClaim removes a pending claim. The activation record is not updated
// in place; handoff and cancel both remove it. Placement state is separate.
type Tx interface {
	PutClaim(c Claim) error
	GetClaim(id string) (Claim, error)
	ListClaims() ([]Claim, error)
	DeleteClaim(id string) error
}

// Store persists claims. Update and View must be serializable with each
// other for a single store.
type Store interface {
	Update(ctx context.Context, fn func(Tx) error) error
	View(ctx context.Context, fn func(Tx) error) error
}

// Manager coalesces in-process waiters onto one pending claim per resource.
type Manager struct {
	store Store
	mu    sync.Mutex
	byRes map[string]*flight
	byID  map[string]*flight
}

// New returns a manager backed by st.
func New(st Store) *Manager {
	return &Manager{store: st, byRes: map[string]*flight{}, byID: map[string]*flight{}}
}

// Join attaches to the pending claim for resourceUID, creating one when
// none exists. Concurrent calls for the same UID share that claim.
func (m *Manager) Join(ctx context.Context, resourceUID, requestID string, deadline time.Time) (Claim, <-chan Result, error) {
	if resourceUID == "" || requestID == "" {
		return Claim{}, nil, errors.New("demand: resource uid and request id are required")
	}
	if deadline.IsZero() {
		return Claim{}, nil, errors.New("demand: deadline is required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if f := m.byRes[resourceUID]; f != nil && !f.done && f.claim.State == StatePending {
		return f.claim, f.add(), nil
	}
	var claim Claim
	err := m.store.Update(ctx, func(tx Tx) error {
		all, err := tx.ListClaims()
		if err != nil {
			return err
		}
		for _, c := range all {
			if c.ResourceUID == resourceUID && c.State == StatePending {
				claim = c
				return nil
			}
		}
		id, err := newID("clm-")
		if err != nil {
			return err
		}
		claim = Claim{
			ClaimID: id, ResourceUID: resourceUID, RequestID: requestID,
			Deadline: deadline, State: StatePending,
		}
		return tx.PutClaim(claim)
	})
	if err != nil {
		return Claim{}, nil, err
	}
	f := &flight{claim: claim}
	m.byRes[resourceUID] = f
	m.byID[claim.ClaimID] = f
	return claim, f.add(), nil
}

// Cancel removes a pending claim and fails its waiters. A claim that this
// manager already finished conflicts. A claim that is not stored is not found.
func (m *Manager) Cancel(ctx context.Context, claimID string) error {
	if claimID == "" {
		return errors.New("demand: claim id required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if f := m.byID[claimID]; f != nil && f.done {
		return fmt.Errorf("demand: cancel %s: %w", claimID, ErrConflict)
	}
	var updated Claim
	err := m.store.Update(ctx, func(tx Tx) error {
		c, err := tx.GetClaim(claimID)
		if err != nil {
			return err
		}
		if c.State != "" && c.State != StatePending {
			return fmt.Errorf("demand: cancel %s: %w (%s)", claimID, ErrConflict, c.State)
		}
		if err := tx.DeleteClaim(claimID); err != nil {
			return err
		}
		updated = c
		updated.State = StateCancelled
		return nil
	})
	if err != nil {
		return err
	}
	m.finish(updated, Result{ClaimID: claimID, Err: ErrCancelled})
	return nil
}

// AcknowledgeHandoff records that placementGen is admitted. Waiters may
// forward only after this returns nil. generation must be positive. A repeat
// with the same generation is a no-op; a different generation conflicts.
func (m *Manager) AcknowledgeHandoff(ctx context.Context, claimID string, placementGen uint64) error {
	if claimID == "" {
		return errors.New("demand: claim id required")
	}
	if placementGen == 0 {
		return errors.New("demand: placement generation required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if f := m.byID[claimID]; f != nil && f.done {
		if f.result.Err == nil && f.result.PlacementGeneration == placementGen {
			return nil
		}
		return fmt.Errorf("demand: handoff %s: %w", claimID, ErrConflict)
	}
	var updated Claim
	err := m.store.Update(ctx, func(tx Tx) error {
		c, err := tx.GetClaim(claimID)
		if err != nil {
			return err
		}
		if c.State != "" && c.State != StatePending {
			return fmt.Errorf("demand: handoff %s: %w (%s)", claimID, ErrConflict, c.State)
		}
		if err := tx.DeleteClaim(claimID); err != nil {
			return err
		}
		c.State = StateHandedOff
		c.PlacementGeneration = placementGen
		updated = c
		return nil
	})
	if err != nil {
		return err
	}
	m.finish(updated, Result{ClaimID: claimID, PlacementGeneration: placementGen})
	return nil
}

func (m *Manager) finish(c Claim, res Result) {
	f := m.byID[c.ClaimID]
	if f == nil || f.done {
		return
	}
	f.claim = c
	f.done = true
	f.result = res
	for _, ch := range f.waiters {
		ch <- res
	}
	f.waiters = nil
	if m.byRes[c.ResourceUID] == f {
		delete(m.byRes, c.ResourceUID)
	}
}

type flight struct {
	claim   Claim
	waiters []chan Result
	done    bool
	result  Result
}

func (f *flight) add() <-chan Result {
	ch := make(chan Result, 1)
	if f.done {
		ch <- f.result
		return ch
	}
	f.waiters = append(f.waiters, ch)
	return ch
}

func newID(prefix string) (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("demand: id: %w", err)
	}
	return prefix + hex.EncodeToString(b[:]), nil
}
