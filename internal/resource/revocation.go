package resource

import (
	"errors"
	"time"
)

// Revocation records that an installation's object-store identity must be
// removed: its app was deleted, or the placement that held a one-off
// replica ID was released. The store writes it in the same transaction as
// that deletion, and the controller removes the identity afterwards and
// sets RevokedAt.
//
// The record is kept after the identity is gone. It is the tombstone that
// stops an app with the same UID (resource UIDs derive from names) from
// being created again and inheriting the deleted installation's
// credentials and replica data.
type Revocation struct {
	Installation string `json:"installation"`
	ResourceUID  string `json:"resource_uid"`
	// RevokedAt is when the identity was removed; nil while pending.
	RevokedAt *time.Time `json:"revoked_at,omitempty"`
}

// Pending reports whether the identity still has to be removed.
func (r Revocation) Pending() bool { return r.RevokedAt == nil }

func (r *Revocation) UnmarshalJSON(b []byte) error {
	type alias Revocation
	var raw alias
	if err := decodeStrict(b, &raw); err != nil {
		return err
	}
	*r = Revocation(raw)
	return nil
}

// Validate checks the record's shape.
func (r Revocation) Validate() error {
	if r.Installation == "" {
		return errors.New("revocation: installation is required")
	}
	if r.ResourceUID == "" {
		return errors.New("revocation: resource_uid is required")
	}
	if r.RevokedAt != nil && r.RevokedAt.IsZero() {
		return errors.New("revocation: revoked_at is zero")
	}
	return nil
}
