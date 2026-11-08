package resource

import (
	"errors"
	"fmt"
	"time"
)

// Outcome is the recorded result of one infrastructure operation.
// unknown means the effect may have happened; it is not success and it is
// not proof that nothing ran.
type Outcome string

const (
	OutcomePrepared   Outcome = "prepared"
	OutcomeDispatched Outcome = "dispatched"
	OutcomeConfirmed  Outcome = "confirmed"
	OutcomeUnknown    Outcome = "unknown"
	OutcomeFailed     Outcome = "failed"
)

func (o Outcome) Valid() bool {
	switch o {
	case OutcomePrepared, OutcomeDispatched, OutcomeConfirmed, OutcomeUnknown, OutcomeFailed:
		return true
	default:
		return false
	}
}

// Operation is one immutable effect binding. Request bodies are not stored;
// InputDigest identifies the inputs that were committed with the operation.
type Operation struct {
	OperationID        string    `json:"operation_id"`
	ResourceUID        string    `json:"resource_uid"`
	OwnerEpoch         uint64    `json:"owner_epoch"`
	ExpectedGeneration uint64    `json:"expected_generation"`
	Action             string    `json:"action"`
	InputDigest        string    `json:"input_digest"`
	Outcome            Outcome   `json:"outcome"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
}

func (o *Operation) UnmarshalJSON(b []byte) error {
	type alias Operation
	var raw alias
	if err := decodeStrict(b, &raw); err != nil {
		return err
	}
	*o = Operation(raw)
	return nil
}

func (o Operation) Validate() error {
	if err := checkID("operation_id", o.OperationID); err != nil {
		return err
	}
	if err := checkID("resource_uid", o.ResourceUID); err != nil {
		return err
	}
	if o.OwnerEpoch < 1 {
		return errors.New("operation: owner_epoch must be at least 1")
	}
	if o.ExpectedGeneration < 1 {
		return errors.New("operation: expected_generation must be at least 1")
	}
	if !actionPattern.MatchString(o.Action) {
		return errors.New("operation: action must be lowercase letters, digits and dashes")
	}
	if !digestPattern.MatchString(o.InputDigest) {
		return errors.New("operation: input_digest must be a lowercase sha256 hex digest")
	}
	if !o.Outcome.Valid() {
		return fmt.Errorf("operation: outcome %q is not supported", o.Outcome)
	}
	if o.CreatedAt.IsZero() || o.UpdatedAt.IsZero() {
		return errors.New("operation: created_at and updated_at are required")
	}
	if o.UpdatedAt.Before(o.CreatedAt) {
		return errors.New("operation: updated_at is before created_at")
	}
	return nil
}

// Open reports whether the outcome still needs reconciliation.
// confirmed and failed are closed. Any other value, including an invalid
// one, is not reported as open; callers still have to reject invalid records.
func (o Operation) Open() bool {
	switch o.Outcome {
	case OutcomePrepared, OutcomeDispatched, OutcomeUnknown:
		return true
	default:
		return false
	}
}

// Terminal reports a closed outcome. Invalid outcomes are not terminal.
func (o Operation) Terminal() bool {
	switch o.Outcome {
	case OutcomeConfirmed, OutcomeFailed:
		return true
	default:
		return false
	}
}

// Placement is the controller's reserved assignment of one replica to one
// host. Generation here is the placement generation, not the spec generation.
// A new placement generation is a new UID.
type Placement struct {
	UID         string    `json:"uid"`
	ResourceUID string    `json:"resource_uid"`
	HostUID     string    `json:"host_uid"`
	Replica     string    `json:"replica"`
	VolumeUID   string    `json:"volume_uid"`
	Generation  uint64    `json:"generation"`
	OwnerEpoch  uint64    `json:"owner_epoch"`
	MemoryMiB   int       `json:"memory_mib"`
	Phase       Phase     `json:"phase"`
	UpdatedAt   time.Time `json:"updated_at"`
}

func (p *Placement) UnmarshalJSON(b []byte) error {
	type alias Placement
	var raw alias
	if err := decodeStrict(b, &raw); err != nil {
		return err
	}
	*p = Placement(raw)
	return nil
}

func (p Placement) Validate() error {
	if err := checkID("uid", p.UID); err != nil {
		return err
	}
	if err := checkID("resource_uid", p.ResourceUID); err != nil {
		return err
	}
	if err := checkID("host_uid", p.HostUID); err != nil {
		return err
	}
	if err := checkID("replica", p.Replica); err != nil {
		return err
	}
	if p.VolumeUID != "" {
		if err := checkID("volume_uid", p.VolumeUID); err != nil {
			return err
		}
	}
	if p.Generation < 1 {
		return errors.New("placement: generation must be at least 1")
	}
	if p.OwnerEpoch < 1 {
		return errors.New("placement: owner_epoch must be at least 1")
	}
	if p.MemoryMiB < 32 {
		return errors.New("placement: memory_mib must be at least 32")
	}
	if !p.Phase.Valid() {
		return fmt.Errorf("placement: phase %q is not supported", p.Phase)
	}
	if p.UpdatedAt.IsZero() {
		return errors.New("placement: updated_at is required")
	}
	return nil
}

// Host is one machine in a pool. ProviderID is the provider instance
// identity and does not change for this UID.
type Host struct {
	UID        string    `json:"uid"`
	PoolUID    string    `json:"pool_uid"`
	ProviderID string    `json:"provider_id"`
	MemoryMiB  int       `json:"memory_mib"`
	Ready      bool      `json:"ready"`
	Draining   bool      `json:"draining"`
	ObservedAt time.Time `json:"observed_at"`
}

func (h *Host) UnmarshalJSON(b []byte) error {
	type alias Host
	var raw alias
	if err := decodeStrict(b, &raw); err != nil {
		return err
	}
	*h = Host(raw)
	return nil
}

func (h Host) Validate() error {
	if err := checkID("uid", h.UID); err != nil {
		return err
	}
	if err := checkID("pool_uid", h.PoolUID); err != nil {
		return err
	}
	if err := checkID("provider_id", h.ProviderID); err != nil {
		return err
	}
	if h.MemoryMiB < 32 {
		return errors.New("host: memory_mib must be at least 32")
	}
	if h.ObservedAt.IsZero() {
		return errors.New("host: observed_at is required")
	}
	return nil
}

// ActivationClaim is bounded pending demand for one resource generation.
// It does not admit a route and it does not start a guest by itself.
type ActivationClaim struct {
	UID         string    `json:"uid"`
	ResourceUID string    `json:"resource_uid"`
	Generation  uint64    `json:"generation"`
	Count       int       `json:"count"`
	CreatedAt   time.Time `json:"created_at"`
	Deadline    time.Time `json:"deadline"`
}

func (c *ActivationClaim) UnmarshalJSON(b []byte) error {
	type alias ActivationClaim
	var raw alias
	if err := decodeStrict(b, &raw); err != nil {
		return err
	}
	*c = ActivationClaim(raw)
	return nil
}

func (c ActivationClaim) Validate() error {
	if err := checkID("uid", c.UID); err != nil {
		return err
	}
	if err := checkID("resource_uid", c.ResourceUID); err != nil {
		return err
	}
	if c.Generation < 1 {
		return errors.New("activation claim: generation must be at least 1")
	}
	if c.Count < 1 {
		return errors.New("activation claim: count must be at least 1")
	}
	if c.CreatedAt.IsZero() || c.Deadline.IsZero() {
		return errors.New("activation claim: created_at and deadline are required")
	}
	if !c.Deadline.After(c.CreatedAt) {
		return errors.New("activation claim: deadline must be after created_at")
	}
	return nil
}

// ClusterMeta is the durable identity of this control store. Recovery
// advances the incarnation and owner epoch; they are not reset on open.
type ClusterMeta struct {
	DeploymentUID       string `json:"deployment_uid"`
	RecoveryIncarnation uint64 `json:"recovery_incarnation"`
	OwnerEpoch          uint64 `json:"owner_epoch"`
}

func (m *ClusterMeta) UnmarshalJSON(b []byte) error {
	type alias ClusterMeta
	var raw alias
	if err := decodeStrict(b, &raw); err != nil {
		return err
	}
	*m = ClusterMeta(raw)
	return nil
}

func (m ClusterMeta) Validate() error {
	if err := checkID("deployment_uid", m.DeploymentUID); err != nil {
		return err
	}
	if m.RecoveryIncarnation < 1 {
		return errors.New("cluster meta: recovery_incarnation must be at least 1")
	}
	if m.OwnerEpoch < 1 {
		return errors.New("cluster meta: owner_epoch must be at least 1")
	}
	return nil
}
