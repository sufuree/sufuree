package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/sufuree/sufuree/internal/resource"
)

// Boot floors live in their own bucket, outside allBuckets, so Export and
// Import (and the restore_store command built on them) neither carry nor
// replace them. A placement change therefore never ships every
// installation's floor through Raft, and a restore_store staged before a
// floor write commits cannot roll that write back. The Raft snapshot carries
// them separately (ExportBootFloors, ImportBootFloors).
var bucketBootFloors = []byte("bootfloors")

// bootFloorsMaxBytes bounds one floors snapshot section. At about 150 bytes
// a record it holds several million installations.
const bootFloorsMaxBytes = 1 << 30

func ensureBootFloorBucket(tx *Tx) error {
	_, err := tx.btx.CreateBucketIfNotExists(bucketBootFloors)
	return err
}

// GetBootFloor returns one installation's floor, or ErrNotFound.
func (tx *Tx) GetBootFloor(installation string) (resource.BootFloor, error) {
	return getBucket[resource.BootFloor](tx, bucketBootFloors, installation)
}

// PutBootFloor stores a floor record. Monotonicity is the caller's rule
// (package bootfloor); the store checks only the record's shape and key.
func (tx *Tx) PutBootFloor(f resource.BootFloor) error {
	if err := f.Validate(); err != nil {
		return err
	}
	return putBucket(tx, bucketBootFloors, f.Installation, f)
}

// ListBootFloors returns every floor record in key order.
func (tx *Tx) ListBootFloors() ([]resource.BootFloor, error) {
	return listBucket[resource.BootFloor](tx, bucketBootFloors)
}

// ExportBootFloors writes every floor record as one JSON array.
func (s *Store) ExportBootFloors(w io.Writer) error {
	var floors []resource.BootFloor
	if err := s.View(func(tx *Tx) error {
		var err error
		floors, err = tx.ListBootFloors()
		return err
	}); err != nil {
		return err
	}
	return json.NewEncoder(w).Encode(floors)
}

// ImportBootFloors replaces every floor record with the array from r, in
// one transaction. It is for Raft snapshot restore only: a restore from a
// controller backup must not lower floors (see package bootfloor).
func (s *Store) ImportBootFloors(r io.Reader) error {
	data, err := io.ReadAll(io.LimitReader(r, bootFloorsMaxBytes+1))
	if err != nil {
		return err
	}
	if len(data) > bootFloorsMaxBytes {
		return errors.New("store: boot floors exceed the import limit")
	}
	var raw []json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&raw); err != nil {
		return fmt.Errorf("store: boot floors: %w", err)
	}
	floors := make([]resource.BootFloor, 0, len(raw))
	seen := map[string]bool{}
	for _, item := range raw {
		var f resource.BootFloor
		if err := resource.Decode(item, &f); err != nil {
			return fmt.Errorf("store: boot floors: %w", err)
		}
		if err := f.Validate(); err != nil {
			return fmt.Errorf("store: boot floors: %w", err)
		}
		if seen[f.Installation] {
			return fmt.Errorf("store: boot floors duplicate %q", f.Installation)
		}
		seen[f.Installation] = true
		floors = append(floors, f)
	}
	return s.Update(func(tx *Tx) error {
		if err := tx.btx.DeleteBucket(bucketBootFloors); err != nil {
			return err
		}
		if err := ensureBootFloorBucket(tx); err != nil {
			return err
		}
		for _, f := range floors {
			if err := tx.PutBootFloor(f); err != nil {
				return err
			}
		}
		return nil
	})
}
