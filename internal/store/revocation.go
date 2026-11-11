package store

import (
	"errors"
	"fmt"
	"time"

	"github.com/sufuree/sufuree/internal/resource"
)

// bucketRevocations holds resource.Revocation records by installation. It
// is in allBuckets, so Export, Import and the Raft snapshot carry it. An
// archive written before it existed imports as having none.
var bucketRevocations = []byte("revocations")

// ErrRevoked means an app's installation identity was revoked: its UID can
// not be used again.
var ErrRevoked = errors.New("store: installation was deleted; its name can't be reused")

// GetRevocation reads one installation's revocation, or ErrNotFound.
func (tx *Tx) GetRevocation(installation string) (resource.Revocation, error) {
	return getBucket[resource.Revocation](tx, bucketRevocations, installation)
}

// ListRevocations returns every revocation in key order.
func (tx *Tx) ListRevocations() ([]resource.Revocation, error) {
	return listBucket[resource.Revocation](tx, bucketRevocations)
}

// revoke records that installation's identity must be removed. An existing
// record, pending or done, is left as it is.
func (tx *Tx) revoke(installation, resourceUID string) error {
	_, err := tx.GetRevocation(installation)
	if err == nil {
		return nil
	}
	if !errors.Is(err, ErrNotFound) {
		return err
	}
	return putBucket(tx, bucketRevocations, installation, resource.Revocation{
		Installation: installation, ResourceUID: resourceUID,
	})
}

// MarkRevoked records that installation's identity was removed at at.
func (tx *Tx) MarkRevoked(installation string, at time.Time) error {
	r, err := tx.GetRevocation(installation)
	if err != nil {
		return err
	}
	if !r.Pending() {
		return nil
	}
	if at.IsZero() {
		return fmt.Errorf("store: revocation %q: zero time", installation)
	}
	at = at.UTC()
	r.RevokedAt = &at
	return putBucket(tx, bucketRevocations, installation, r)
}

// releasedReplica records the revocation a released placement needs. An
// app's own UID is its installation ID across placements and scale to zero,
// and is revoked only with the app (DeleteResource). Any other replica ID is
// minted for one placement and never issued again once no placement holds
// it.
func (tx *Tx) releasedReplica(p resource.Placement) error {
	if p.Replica == "" || p.Replica == p.ResourceUID {
		return nil
	}
	all, err := tx.ListPlacements()
	if err != nil {
		return err
	}
	for _, other := range all {
		if other.Replica == p.Replica {
			return nil
		}
	}
	return tx.revoke(p.Replica, p.ResourceUID)
}
