package resource

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// BootFloor is one installation's boot generation and rollback floor, held
// by the controller so both survive the loss of any host (Zati RFC 007
// §7.4, option A). Installation is the stable installation identity: the
// app's resource UID, which is also its placements' Replica.
//
// Generation is the last boot generation H the controller issued. It only
// rises. The floor is (ProgressEpoch, ProgressSeq), the highest durable
// progress the installation reported within Incarnation. Incarnation
// changes only through an audited lowering (Lowered records the latest),
// and is separate from ClusterMeta.RecoveryIncarnation.
type BootFloor struct {
	Installation string `json:"installation"`
	Generation   uint64 `json:"generation"`
	Incarnation  uint64 `json:"incarnation"`
	// IncarnationFrom is the first generation issued in this incarnation
	// (zero for the first incarnation). Progress from a hint published by
	// an earlier generation belongs to data a lowering discarded.
	IncarnationFrom uint64         `json:"incarnation_from,omitempty"`
	ProgressEpoch   uint64         `json:"progress_epoch,omitempty"`
	ProgressSeq     uint64         `json:"progress_seq,omitempty"`
	Lowered         *FloorLowering `json:"lowered,omitempty"`
}

// FloorLowering is the audit record of an intentional rollback: the
// installation owner requested it, a different Zati operator approved it,
// and the floor became the restored lease's exact mark.
type FloorLowering struct {
	RequestedBy string    `json:"requested_by"`
	ApprovedBy  string    `json:"approved_by"`
	Reason      string    `json:"reason"`
	Reference   string    `json:"reference"`
	At          time.Time `json:"at"`
	// The floor before the lowering, kept for the audit trail.
	FromIncarnation uint64 `json:"from_incarnation"`
	FromEpoch       uint64 `json:"from_epoch"`
	FromSeq         uint64 `json:"from_seq"`
}

func (f *BootFloor) UnmarshalJSON(b []byte) error {
	type alias BootFloor
	var raw alias
	if err := decodeStrict(b, &raw); err != nil {
		return err
	}
	*f = BootFloor(raw)
	return nil
}

func (l *FloorLowering) UnmarshalJSON(b []byte) error {
	type alias FloorLowering
	var raw alias
	if err := decodeStrict(b, &raw); err != nil {
		return err
	}
	*l = FloorLowering(raw)
	return nil
}

// Validate checks the record's shape.
func (f BootFloor) Validate() error {
	if err := checkID("installation", f.Installation); err != nil {
		return err
	}
	if f.Incarnation < 1 {
		return errors.New("boot floor: incarnation must be at least 1")
	}
	if f.ProgressEpoch > f.Generation {
		return errors.New("boot floor: progress epoch is above the issued generation")
	}
	if f.Lowered != nil {
		if err := f.Lowered.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// Validate checks the audit record: both people named, distinct, with a
// reason and a reference.
func (l FloorLowering) Validate() error {
	for name, v := range map[string]string{
		"requested_by": l.RequestedBy, "approved_by": l.ApprovedBy,
		"reason": l.Reason, "reference": l.Reference,
	} {
		if strings.TrimSpace(v) == "" || len(v) > 512 {
			return fmt.Errorf("floor lowering: %s is required (at most 512 bytes)", name)
		}
	}
	if strings.EqualFold(strings.TrimSpace(l.RequestedBy), strings.TrimSpace(l.ApprovedBy)) {
		return errors.New("floor lowering: the approver must be a different person from the requester")
	}
	if l.At.IsZero() {
		return errors.New("floor lowering: at is required")
	}
	return nil
}
