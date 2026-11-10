// Package controlraft puts authoritative control-store mutations on the Raft
// log. When a Journal has a Node, every Update clones the current archive,
// applies the mutation in a staging store, and proposes restore_store so the
// FSM Import is the only write to the live record file. Without a Node,
// Update writes the local store directly (single-process controller).
package controlraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/raft"

	"github.com/sufuree/sufuree/internal/raftnode"
	"github.com/sufuree/sufuree/internal/store"
)

// OpRestoreStore replaces the control record file with a store Export archive.
const OpRestoreStore = "restore_store"

// Encode builds a CommandFSM log entry.
func Encode(op string, payload any) ([]byte, error) {
	if op == "" {
		return nil, errors.New("controlraft: op is required")
	}
	type env struct {
		Op      string `json:"op"`
		Payload any    `json:"payload,omitempty"`
	}
	return json.Marshal(env{Op: op, Payload: payload})
}

// StoreFSM applies restore_store commands and snapshots via store Export/Import.
func StoreFSM(st *store.Store) *raftnode.CommandFSM {
	return &raftnode.CommandFSM{
		ApplyFn: func(op string, payload []byte) error {
			switch op {
			case OpRestoreStore:
				if len(payload) == 0 {
					return errors.New("controlraft: restore_store payload is required")
				}
				return st.Import(bytes.NewReader(payload))
			default:
				return fmt.Errorf("controlraft: unknown command %q", op)
			}
		},
		SnapshotFn: func() ([]byte, error) {
			var buf bytes.Buffer
			if err := st.Export(&buf); err != nil {
				return nil, err
			}
			return buf.Bytes(), nil
		},
		RestoreFn: func(b []byte) error {
			return st.Import(bytes.NewReader(b))
		},
	}
}

// Journal is a reconcile/demand DB that optionally proposes Updates through Raft.
type Journal struct {
	Store *store.Store
	Node  *raftnode.Node

	// ApplyTimeout bounds one Raft proposal. Zero means 10s.
	ApplyTimeout time.Duration

	mu sync.Mutex
}

// View reads the live store.
func (j *Journal) View(fn func(*store.Tx) error) error {
	if j == nil || j.Store == nil {
		return errors.New("controlraft: nil journal store")
	}
	return j.Store.View(fn)
}

// Update applies fn. With Node set, the resulting archive is proposed and
// applied only through the FSM. Concurrent Updates serialize on the journal.
func (j *Journal) Update(fn func(*store.Tx) error) error {
	if j == nil || j.Store == nil {
		return errors.New("controlraft: nil journal store")
	}
	if fn == nil {
		return errors.New("controlraft: update func is required")
	}
	if j.Node == nil {
		return j.Store.Update(fn)
	}
	j.mu.Lock()
	defer j.mu.Unlock()

	archive, err := j.stage(fn)
	if err != nil {
		return err
	}
	cmd, err := Encode(OpRestoreStore, json.RawMessage(archive))
	if err != nil {
		return err
	}
	timeout := j.ApplyTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return j.Node.Apply(context.Background(), cmd, timeout)
}

// stage exports the live store, mutates a temporary copy, and returns the
// resulting archive bytes. The live store is not written here.
func (j *Journal) stage(fn func(*store.Tx) error) ([]byte, error) {
	var current bytes.Buffer
	if err := j.Store.Export(&current); err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp("", "controlraft-stage-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "stage.db")
	staging, err := store.Open(path)
	if err != nil {
		return nil, err
	}
	defer staging.Close()
	if err := staging.Import(bytes.NewReader(current.Bytes())); err != nil {
		return nil, err
	}
	if err := staging.Update(fn); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	if err := staging.Export(&out); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// AsFSM returns the raft.FSM for Open. Convenience for the controller wiring.
func AsFSM(st *store.Store) raft.FSM {
	return StoreFSM(st)
}
