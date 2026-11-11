// Package executor is the scoped infrastructure effect boundary from
// RFC-0001. Cloud mutations fail closed unless a caller explicitly enables
// them. Unimplemented effects return an error; they never report success.
package executor

import (
	"context"
	"errors"
	"fmt"
)

// ErrEffectsDisabled means a mutation was refused because effects are not
// enabled. A timeout or a disabled executor is not success and is not a
// license to invent a provider identity.
var ErrEffectsDisabled = errors.New("executor: effects disabled")

// ErrNotImplemented means the effect is in the version-one API and has no
// implementation in this process.
var ErrNotImplemented = errors.New("executor: not implemented")

// ErrNotFound means Describe did not find a host. It is not evidence that a
// timed-out create never happened on a real provider.
var ErrNotFound = errors.New("executor: host not found")

// ErrOutcomeUnknown means a mutation may have run in the provider; the caller
// must reconcile by operation identity and must not free reserved capacity.
var ErrOutcomeUnknown = errors.New("executor: provider outcome unknown")

// HostDesc is observed provider state for one host.
type HostDesc struct {
	ID          string
	ProviderID  string
	Shape       string
	MemoryMiB   int
	CPU         int
	State       string // running or terminated
	OperationID string
	Tags        map[string]string
}

// CreateHostInput is the immutable create request. OperationID is the
// idempotency key; a repeat must not launch a second host.
type CreateHostInput struct {
	OperationID string
	Template    string
	Shape       string
	MemoryMiB   int
	CPU         int
	Tags        map[string]string
}

// ProviderRef is the identity returned by a confirmed create.
type ProviderRef struct {
	ProviderID string
	Shape      string
	MemoryMiB  int
	CPU        int
}

// TerminateHostInput retires one host. The host identity is not reused.
type TerminateHostInput struct {
	OperationID string
	ProviderID  string
	HostID      string
}

// AttachVolumeInput, DetachVolumeInput, SnapshotInput, RestoreInput and
// ExpireInput are named so callers can see which effect was refused.
type AttachVolumeInput struct {
	OperationID string
	HostID      string
	VolumeID    string
}

// DetachVolumeInput detaches one registered volume.
type DetachVolumeInput struct {
	OperationID string
	HostID      string
	VolumeID    string
}

// SnapshotInput creates a snapshot of a registered volume.
type SnapshotInput struct {
	OperationID string
	VolumeID    string
}

// RestoreInput restores a snapshot into a new volume.
type RestoreInput struct {
	OperationID string
	SnapshotID  string
}

// ExpireInput requests policy-eligible snapshot expiry.
type ExpireInput struct {
	OperationID string
	SnapshotID  string
}

// Executor is the small effect API. Describe reads; every other method is a
// mutation. Attach, detach, snapshot, restore and expire are specified and
// intentionally unimplemented.
type Executor interface {
	Describe(ctx context.Context, id string) (HostDesc, error)
	CreateHost(ctx context.Context, in CreateHostInput) (ProviderRef, error)
	TerminateHost(ctx context.Context, in TerminateHostInput) error
	AttachVolume(ctx context.Context, in AttachVolumeInput) error
	DetachVolume(ctx context.Context, in DetachVolumeInput) error
	CreateSnapshot(ctx context.Context, in SnapshotInput) error
	RestoreSnapshot(ctx context.Context, in RestoreInput) error
	ExpireSnapshot(ctx context.Context, in ExpireInput) error
}

func notImplemented(effect string) error {
	return fmt.Errorf("executor: %s %w", effect, ErrNotImplemented)
}
