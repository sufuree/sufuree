// Package controlraft puts authoritative control-store mutations on the Raft
// log. When a Journal has a Node, every Update clones the current archive,
// applies the mutation in a staging store, and proposes restore_store so the
// FSM Import is the only write to the live record file. Boot floors (package
// bootfloor) are outside that archive and are written only by boot_floor
// commands (Commit), one installation or one host's batch at a time. Without
// a Node, Update and Commit write the local store directly
// (single-process controller).
package controlraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"

	"github.com/sufuree/sufuree/internal/bootfloor"
	"github.com/sufuree/sufuree/internal/raftnode"
	"github.com/sufuree/sufuree/internal/store"
)

// OpRestoreStore replaces the control record file with a store Export archive.
const OpRestoreStore = "restore_store"

// archiveMaxBytes matches store.Import's bound so staging fails before a
// propose that the FSM would reject.
const archiveMaxBytes = 32 << 20

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

// StoreFSM applies restore_store and boot_floor commands and snapshots the
// control archive plus the boot floors, which restore_store never carries.
func StoreFSM(st *store.Store) *raftnode.CommandFSM {
	return &raftnode.CommandFSM{
		ApplyFn: func(op string, payload []byte) error {
			switch op {
			case OpRestoreStore:
				if len(payload) == 0 {
					return errors.New("controlraft: restore_store payload is required")
				}
				return st.Import(bytes.NewReader(payload))
			case bootfloor.Op:
				return st.Update(func(tx *store.Tx) error { return bootfloor.Apply(tx, payload) })
			default:
				return fmt.Errorf("controlraft: unknown command %q", op)
			}
		},
		SnapshotFn: func() ([]byte, error) { return snapshot(st) },
		RestoreFn:  func(b []byte) error { return restore(st, b) },
	}
}

// fsmSnapshot is the Raft snapshot: the control archive and the boot
// floors. A snapshot that is a bare control archive (written before boot
// floors existed) restores the archive and leaves the floors empty.
type fsmSnapshot struct {
	Control    json.RawMessage `json:"control"`
	BootFloors json.RawMessage `json:"boot_floors"`
}

func snapshot(st *store.Store) ([]byte, error) {
	var control, floors bytes.Buffer
	if err := st.Export(&control); err != nil {
		return nil, err
	}
	if err := st.ExportBootFloors(&floors); err != nil {
		return nil, err
	}
	return json.Marshal(fsmSnapshot{
		Control:    bytes.TrimSpace(control.Bytes()),
		BootFloors: bytes.TrimSpace(floors.Bytes()),
	})
}

func restore(st *store.Store, b []byte) error {
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(b, &probe); err != nil {
		return fmt.Errorf("controlraft: snapshot: %w", err)
	}
	if _, ok := probe["control"]; !ok {
		if err := st.Import(bytes.NewReader(b)); err != nil {
			return err
		}
		return st.ImportBootFloors(strings.NewReader("[]"))
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var snap fsmSnapshot
	if err := dec.Decode(&snap); err != nil {
		return fmt.Errorf("controlraft: snapshot: %w", err)
	}
	if len(snap.Control) == 0 || len(snap.BootFloors) == 0 {
		return errors.New("controlraft: snapshot is missing a section")
	}
	if err := st.Import(bytes.NewReader(snap.Control)); err != nil {
		return err
	}
	return st.ImportBootFloors(bytes.NewReader(snap.BootFloors))
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

	timeout := j.ApplyTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ctx := context.Background()
	// Wait for prior log entries before Export so a new leader does not stage
	// from a store that is behind the committed log.
	if err := j.Node.Barrier(ctx, timeout); err != nil {
		return err
	}
	term, err := j.Node.Term()
	if err != nil {
		return err
	}
	archive, err := j.stage(fn)
	if err != nil {
		return err
	}
	cmd, err := Encode(OpRestoreStore, json.RawMessage(archive))
	if err != nil {
		return err
	}
	cur, err := j.Node.Term()
	if err != nil || cur != term || !j.Node.IsLeader() {
		return raftnode.ErrNotLeader
	}
	return j.Node.Apply(ctx, cmd, timeout)
}

// Commit plans a boot_floor command from the live store and commits it.
// plan runs under the journal's write lock, so nothing else the journal
// writes can interleave between the read and the commit; with Raft it reads
// after a barrier and the command applies only through the FSM. plan
// returns the encoded command.
func (j *Journal) Commit(plan func(*store.Tx) ([]byte, error)) error {
	if j == nil || j.Store == nil {
		return errors.New("controlraft: nil journal store")
	}
	if plan == nil {
		return errors.New("controlraft: commit plan is required")
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.Node == nil {
		return j.Store.Update(func(tx *store.Tx) error {
			payload, err := plan(tx)
			if err != nil {
				return err
			}
			return bootfloor.Apply(tx, payload)
		})
	}
	timeout := j.ApplyTimeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	ctx := context.Background()
	if err := j.Node.Barrier(ctx, timeout); err != nil {
		return err
	}
	term, err := j.Node.Term()
	if err != nil {
		return err
	}
	var payload []byte
	if err := j.Store.View(func(tx *store.Tx) error {
		var err error
		payload, err = plan(tx)
		return err
	}); err != nil {
		return err
	}
	cmd, err := Encode(bootfloor.Op, json.RawMessage(payload))
	if err != nil {
		return err
	}
	cur, err := j.Node.Term()
	if err != nil || cur != term || !j.Node.IsLeader() {
		return raftnode.ErrNotLeader
	}
	return j.Node.Apply(ctx, cmd, timeout)
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
	if out.Len() > archiveMaxBytes {
		return nil, fmt.Errorf("controlraft: staged archive is %d bytes; store Import limit is %d", out.Len(), archiveMaxBytes)
	}
	return out.Bytes(), nil
}

// AsFSM returns the raft.FSM for Open. Convenience for the controller wiring.
func AsFSM(st *store.Store) raft.FSM {
	return StoreFSM(st)
}
