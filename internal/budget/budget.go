// Package budget is the version-one capacity ledger for RFC-0001 F01.
// Reservations are keyed by operation ID so a timed-out create cannot free
// capacity that may still be running in the provider.
package budget

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	// ErrDenied means the grant does not allow the reservation.
	ErrDenied = errors.New("budget: denied")
	// ErrExpired means the growth grant is no longer valid.
	ErrExpired = errors.New("budget: grant expired")
	// ErrUnknown means the operation has no reservation.
	ErrUnknown = errors.New("budget: unknown operation")
)

// Grant is the operator-supplied growth authority for one deployment.
type Grant struct {
	DeploymentUID string
	Revision      string
	MaxHosts      int
	MaxMemoryMiB  int
	Expires       time.Time // zero means no expiry
}

// Valid reports whether the grant may authorize new expansion at now.
func (g Grant) Valid(now time.Time) error {
	if g.DeploymentUID == "" {
		return fmt.Errorf("%w: deployment is required", ErrDenied)
	}
	if g.MaxHosts < 1 {
		return fmt.Errorf("%w: max_hosts must be at least 1", ErrDenied)
	}
	if g.MaxMemoryMiB < 32 {
		return fmt.Errorf("%w: max_memory_mib must be at least 32", ErrDenied)
	}
	if !g.Expires.IsZero() && !now.Before(g.Expires) {
		return ErrExpired
	}
	return nil
}

// Ledger tracks in-flight and confirmed host capacity against a Grant.
type Ledger struct {
	Grant Grant
	Now   func() time.Time

	mu   sync.Mutex
	byOp map[string]reservation
}

type reservation struct {
	MemoryMiB int
	State     string // reserved | confirmed | released
}

func (l *Ledger) now() time.Time {
	if l != nil && l.Now != nil {
		return l.Now()
	}
	return time.Now().UTC()
}

func (l *Ledger) ensure() {
	if l.byOp == nil {
		l.byOp = map[string]reservation{}
	}
}

// ReserveCreate holds one host slot for operationID. Repeating the same ID
// is idempotent. Cleanup/fence paths must Release or leave the reservation
// until the provider outcome is known.
func (l *Ledger) ReserveCreate(operationID string, memoryMiB int) error {
	if l == nil {
		return errors.New("budget: nil ledger")
	}
	if operationID == "" {
		return errors.New("budget: operation id is required")
	}
	if memoryMiB < 32 {
		return fmt.Errorf("budget: memory_mib must be at least 32")
	}
	if err := l.Grant.Valid(l.now()); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ensure()
	if r, ok := l.byOp[operationID]; ok {
		if r.State == "released" {
			return fmt.Errorf("%w: operation %s was released", ErrDenied, operationID)
		}
		if r.MemoryMiB != memoryMiB {
			return fmt.Errorf("%w: operation %s memory mismatch", ErrDenied, operationID)
		}
		return nil
	}
	hosts, mem := l.usageLocked()
	if hosts+1 > l.Grant.MaxHosts {
		return fmt.Errorf("%w: host cap %d", ErrDenied, l.Grant.MaxHosts)
	}
	if mem+memoryMiB > l.Grant.MaxMemoryMiB {
		return fmt.Errorf("%w: memory cap %d", ErrDenied, l.Grant.MaxMemoryMiB)
	}
	l.byOp[operationID] = reservation{MemoryMiB: memoryMiB, State: "reserved"}
	return nil
}

// Confirm marks a successful create. Unknown operations are errors.
func (l *Ledger) Confirm(operationID string) error {
	return l.set(operationID, "confirmed")
}

// Release frees a reservation after a failed create or confirmed terminate.
func (l *Ledger) Release(operationID string) error {
	return l.set(operationID, "released")
}

func (l *Ledger) set(operationID, state string) error {
	if l == nil {
		return errors.New("budget: nil ledger")
	}
	if operationID == "" {
		return errors.New("budget: operation id is required")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ensure()
	r, ok := l.byOp[operationID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrUnknown, operationID)
	}
	if r.State == "released" && state == "released" {
		return nil
	}
	if r.State == "released" {
		return fmt.Errorf("%w: operation %s was released", ErrDenied, operationID)
	}
	if r.State == "confirmed" && state == "confirmed" {
		return nil
	}
	r.State = state
	l.byOp[operationID] = r
	return nil
}

// Usage is confirmed+reserved host count and memory (released excluded).
func (l *Ledger) Usage() (hosts, memoryMiB int) {
	if l == nil {
		return 0, 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.usageLocked()
}

func (l *Ledger) usageLocked() (hosts, memoryMiB int) {
	for _, r := range l.byOp {
		if r.State == "released" {
			continue
		}
		hosts++
		memoryMiB += r.MemoryMiB
	}
	return hosts, memoryMiB
}
