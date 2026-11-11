package store

import (
	"errors"
	"fmt"
	"time"

	"github.com/sufuree/sufuree/internal/resource"
)

// Revocations live in two buckets, both in allBuckets, so Export, Import and
// the Raft snapshot carry them. An archive written before they existed
// imports as having none.
//
//   - bucketRevokeQueue holds the pending revocations, by installation. A
//     record leaves it once the identity is removed, so the queue stays as
//     small as the work outstanding.
//   - bucketRevocations holds a permanent tombstone for each deleted app's
//     UID, the installation ID that would otherwise come back with its
//     name. One-off replica IDs are random and never issued again, so they
//     get no tombstone.
var (
	bucketRevocations = []byte("revocations")
	bucketRevokeQueue = []byte("revoke_queue")
)

// ErrRevoked means an app's installation identity was revoked: its UID can
// not be used again.
var ErrRevoked = errors.New("store: installation was deleted; its name can't be reused")

// GetRevocation reads one deleted app's tombstone, or ErrNotFound.
func (tx *Tx) GetRevocation(installation string) (resource.Revocation, error) {
	return getBucket[resource.Revocation](tx, bucketRevocations, installation)
}

// ListRevocations returns every deleted app's tombstone in key order.
func (tx *Tx) ListRevocations() ([]resource.Revocation, error) {
	return listBucket[resource.Revocation](tx, bucketRevocations)
}

// PendingRevocations returns up to limit queued revocations in key order,
// starting after the key after ("" starts at the beginning). more reports
// whether any remain past the last one returned.
func (tx *Tx) PendingRevocations(after string, limit int) (out []resource.Revocation, more bool, err error) {
	b, err := tx.bucket(bucketRevokeQueue)
	if err != nil {
		return nil, false, err
	}
	c := b.Cursor()
	k, _ := c.First()
	if after != "" {
		k, _ = c.Seek([]byte(after))
		if k != nil && string(k) == after {
			k, _ = c.Next()
		}
	}
	for ; k != nil; k, _ = c.Next() {
		if len(out) == limit {
			return out, true, nil
		}
		r, err := getBucket[resource.Revocation](tx, bucketRevokeQueue, string(k))
		if err != nil {
			return nil, false, err
		}
		out = append(out, r)
	}
	return out, false, nil
}

// revoke queues installation's revocation. A tombstone is written too when
// the installation is an app's own UID. Existing records are left as they
// are.
func (tx *Tx) revoke(installation, resourceUID string) error {
	rec := resource.Revocation{Installation: installation, ResourceUID: resourceUID}
	if installation == resourceUID {
		if _, err := tx.GetRevocation(installation); errors.Is(err, ErrNotFound) {
			if err := putBucket(tx, bucketRevocations, installation, rec); err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
	}
	if _, err := getBucket[resource.Revocation](tx, bucketRevokeQueue, installation); err == nil {
		return nil
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}
	return putBucket(tx, bucketRevokeQueue, installation, rec)
}

// MarkRevoked records that installation's identity was removed at at: it
// leaves the queue, and an app's tombstone records the time.
func (tx *Tx) MarkRevoked(installation string, at time.Time) error {
	if at.IsZero() {
		return fmt.Errorf("store: revocation %q: zero time", installation)
	}
	if _, err := getBucket[resource.Revocation](tx, bucketRevokeQueue, installation); err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil
		}
		return err
	}
	if err := delBucket(tx, bucketRevokeQueue, installation); err != nil {
		return err
	}
	r, err := tx.GetRevocation(installation)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil || !r.Pending() {
		return err
	}
	at = at.UTC()
	r.RevokedAt = &at
	return putBucket(tx, bucketRevocations, installation, r)
}

// releasedReplica queues the revocation a released placement needs. An
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
