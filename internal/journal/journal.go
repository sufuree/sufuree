// Package journal is the host agent's durable per-workload record. Its one
// safety property: a lease generation is persisted (file and directory
// fsync) before any guest boots with it, so generations never repeat even
// across agent crashes. A repeated generation would be refused by the
// workload's own fence, never silently accepted.
package journal

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// Record is one workload's durable state on this host.
type Record struct {
	Version    int    `json:"version"`
	Generation uint64 `json:"generation"` // last generation issued
}

// Journal stores records under dir, one file per workload.
type Journal struct {
	dir string
	mu  sync.Mutex
}

// Open creates dir (0700) if needed.
func Open(dir string) (*Journal, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	// Make the directory entry itself durable, so a crash on a fresh
	// install cannot lose it and reissue generation 1.
	if err := syncDir(filepath.Dir(dir)); err != nil {
		return nil, err
	}
	return &Journal{dir: dir}, nil
}

func (j *Journal) path(id string) string { return filepath.Join(j.dir, id+".json") }

// Get returns the record, or a zero record for a new workload.
func (j *Journal) Get(id string) (Record, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.get(id)
}

func (j *Journal) get(id string) (Record, error) {
	data, err := os.ReadFile(j.path(id))
	if errors.Is(err, fs.ErrNotExist) {
		return Record{Version: 1}, nil
	}
	if err != nil {
		return Record{}, err
	}
	var r Record
	if err := json.Unmarshal(data, &r); err != nil || r.Version != 1 {
		return Record{}, fmt.Errorf("journal: record for %q is corrupt", id)
	}
	return r, nil
}

// NextGeneration durably issues the next generation for id. The caller may
// boot with it only after this returns nil.
func (j *Journal) NextGeneration(id string) (uint64, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	r, err := j.get(id)
	if err != nil {
		return 0, err
	}
	r.Generation++
	if err := j.put(id, r); err != nil {
		return 0, err
	}
	return r.Generation, nil
}

// put writes atomically and durably: temp file, fsync, rename, dir fsync.
func (j *Journal) put(id string, r Record) error {
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	tmp := j.path(id) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, j.path(id)); err != nil {
		return err
	}
	return syncDir(j.dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}
