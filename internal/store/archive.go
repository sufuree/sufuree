package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// archiveVersion is the JSON archive written by Export and read by Import.
const archiveVersion = 1

// archiveMaxBytes bounds one archive so a snapshot cannot allocate without limit.
const archiveMaxBytes = 32 << 20

// archive is every bucket, in bolt key order, as JSON values.
type archive struct {
	Version int             `json:"version"`
	Buckets []archiveBucket `json:"buckets"`
}

type archiveBucket struct {
	Name    string         `json:"name"`
	Entries []archiveEntry `json:"entries"`
}

type archiveEntry struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

// Export writes a versioned JSON archive of every bucket. Values are copied
// out of the read transaction unchanged.
func (s *Store) Export(w io.Writer) error {
	if w == nil {
		return errors.New("store: export writer is required")
	}
	arch := archive{Version: archiveVersion, Buckets: []archiveBucket{}}
	err := s.View(func(tx *Tx) error {
		for _, name := range allBuckets {
			b, err := tx.bucket(name)
			if err != nil {
				return err
			}
			bucket := archiveBucket{Name: string(name), Entries: []archiveEntry{}}
			err = b.ForEach(func(k, v []byte) error {
				if len(v) == 0 || !json.Valid(v) {
					return fmt.Errorf("store: %s %q is not JSON", name, k)
				}
				bucket.Entries = append(bucket.Entries, archiveEntry{
					Key:   string(k),
					Value: append(json.RawMessage(nil), v...),
				})
				return nil
			})
			if err != nil {
				return err
			}
			arch.Buckets = append(arch.Buckets, bucket)
		}
		return nil
	})
	if err != nil {
		return err
	}
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	return enc.Encode(arch)
}

// Import replaces every bucket with the archive and syncs.
//
// The replacement is one transaction. A bad archive leaves the previous
// records in place. On success, Update has already called Sync, so a later
// open sees the imported records.
func (s *Store) Import(r io.Reader) error {
	if r == nil {
		return errors.New("store: import reader is required")
	}
	data, err := io.ReadAll(io.LimitReader(r, archiveMaxBytes+1))
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return errors.New("store: empty import")
	}
	if len(data) > archiveMaxBytes {
		return errors.New("store: import exceeds 32MiB")
	}
	arch, err := decodeArchive(data)
	if err != nil {
		return err
	}
	return s.Update(func(tx *Tx) error {
		if err := tx.installArchive(arch); err != nil {
			return err
		}
		return tx.ensureSchema()
	})
}

func decodeArchive(data []byte) (archive, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var arch archive
	if err := dec.Decode(&arch); err != nil {
		return archive{}, fmt.Errorf("store: import: %w", err)
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return archive{}, errors.New("store: import: trailing JSON")
		}
		return archive{}, fmt.Errorf("store: import: trailing JSON: %w", err)
	}
	if arch.Version != archiveVersion {
		return archive{}, fmt.Errorf("store: import version %d is not supported", arch.Version)
	}
	return arch, nil
}

func (tx *Tx) installArchive(arch archive) error {
	known := map[string]bool{}
	for _, name := range allBuckets {
		known[string(name)] = true
	}
	byName := map[string]archiveBucket{}
	for _, bucket := range arch.Buckets {
		if !known[bucket.Name] {
			return fmt.Errorf("store: import bucket %q is unknown", bucket.Name)
		}
		if _, ok := byName[bucket.Name]; ok {
			return fmt.Errorf("store: import bucket %q is duplicated", bucket.Name)
		}
		byName[bucket.Name] = bucket
	}
	for _, name := range allBuckets {
		if _, ok := byName[string(name)]; ok {
			continue
		}
		// Archives written before revocations existed have none.
		if string(name) == string(bucketRevocations) || string(name) == string(bucketRevokeQueue) {
			byName[string(name)] = archiveBucket{Name: string(name)}
			continue
		}
		return fmt.Errorf("store: import is missing bucket %q", name)
	}
	for _, name := range allBuckets {
		b, err := tx.bucket(name)
		if err != nil {
			return err
		}
		var keys [][]byte
		if err := b.ForEach(func(k, _ []byte) error {
			keys = append(keys, append([]byte(nil), k...))
			return nil
		}); err != nil {
			return err
		}
		for _, key := range keys {
			if err := b.Delete(key); err != nil {
				return err
			}
		}
		seen := map[string]struct{}{}
		for _, entry := range byName[string(name)].Entries {
			if entry.Key == "" {
				return fmt.Errorf("store: import bucket %q has an empty key", name)
			}
			if _, ok := seen[entry.Key]; ok {
				return fmt.Errorf("store: import bucket %q duplicates key %q", name, entry.Key)
			}
			seen[entry.Key] = struct{}{}
			if len(entry.Value) == 0 || !json.Valid(entry.Value) {
				return fmt.Errorf("store: import %s %q is not JSON", name, entry.Key)
			}
			if err := b.Put([]byte(entry.Key), append([]byte(nil), entry.Value...)); err != nil {
				return err
			}
		}
	}
	return nil
}
