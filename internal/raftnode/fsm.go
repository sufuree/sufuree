package raftnode

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	raft "github.com/sufuree/raft"
)

// ApplyFunc handles one committed command. payload is the raw JSON value
// from the command, and is nil when the command omitted it.
type ApplyFunc func(op string, payload []byte) error

// SnapshotFunc returns the bytes RestoreFunc will see.
type SnapshotFunc func() ([]byte, error)

// RestoreFunc replaces FSM state with the bytes from a snapshot.
type RestoreFunc func(snapshot []byte) error

// CommandFSM applies JSON commands of the form {"op":"...","payload":...}.
// It implements the github.com/sufuree/raft/driver.FSM contract: batched
// Apply, byte Snapshot, and Restore at an exact applied index.
//
// When SnapshotFn and RestoreFn are set, snapshots are those bytes. When
// neither is set, a snapshot records only the last applied log index. A
// snapshot func without a restore func fails at restore time.
type CommandFSM struct {
	ApplyFn    ApplyFunc
	SnapshotFn SnapshotFunc
	RestoreFn  RestoreFunc

	mu        sync.Mutex
	lastIndex uint64
}

type commandEnvelope struct {
	Op      string          `json:"op"`
	Payload json.RawMessage `json:"payload"`
}

type indexSnapshot struct {
	LastIndex uint64 `json:"last_index"`
}

// Apply decodes and applies each normal command entry in order. Configuration
// entries are owned by the library and skipped. A bad command fails the batch;
// the driver treats an apply failure as terminal, so callers validate commands
// before proposing them.
func (f *CommandFSM) Apply(entries []raft.Entry) error {
	if f == nil {
		return errors.New("raftnode: nil fsm")
	}
	if f.ApplyFn == nil {
		return errors.New("raftnode: apply func is required")
	}
	for _, e := range entries {
		if e.Type != raft.EntryNormal {
			continue
		}
		if len(e.Data) == 0 {
			// A newly elected leader appends an empty no-op entry to commit
			// entries from prior terms. It carries no command but still
			// advances the applied index.
			f.mu.Lock()
			f.lastIndex = uint64(e.Index)
			f.mu.Unlock()
			continue
		}
		var cmd commandEnvelope
		dec := json.NewDecoder(bytes.NewReader(e.Data))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&cmd); err != nil {
			return fmt.Errorf("raftnode: command: %w", err)
		}
		var extra json.RawMessage
		if err := dec.Decode(&extra); err != io.EOF {
			if err == nil {
				return errors.New("raftnode: command: trailing JSON")
			}
			return fmt.Errorf("raftnode: command: trailing JSON: %w", err)
		}
		if cmd.Op == "" {
			return errors.New("raftnode: command op is required")
		}
		if err := f.ApplyFn(cmd.Op, []byte(cmd.Payload)); err != nil {
			return err
		}
		f.mu.Lock()
		f.lastIndex = uint64(e.Index)
		f.mu.Unlock()
	}
	return nil
}

// Snapshot captures either the user function's bytes or the last applied index.
func (f *CommandFSM) Snapshot() ([]byte, error) {
	if f == nil {
		return nil, errors.New("raftnode: nil fsm")
	}
	if f.SnapshotFn != nil {
		return f.SnapshotFn()
	}
	f.mu.Lock()
	idx := f.lastIndex
	f.mu.Unlock()
	return json.Marshal(indexSnapshot{LastIndex: idx})
}

// Restore loads a snapshot at meta.Index. User bytes are passed through when
// RestoreFn is set.
func (f *CommandFSM) Restore(meta raft.SnapshotMeta, data []byte) error {
	if f == nil {
		return errors.New("raftnode: nil fsm")
	}
	if f.RestoreFn != nil {
		if err := f.RestoreFn(data); err != nil {
			return err
		}
		f.mu.Lock()
		f.lastIndex = uint64(meta.Index)
		f.mu.Unlock()
		return nil
	}
	if f.SnapshotFn != nil {
		return errors.New("raftnode: restore func is required when snapshot func is set")
	}
	if len(bytes.TrimSpace(data)) == 0 {
		return nil
	}
	var snap indexSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("raftnode: restore: %w", err)
	}
	f.mu.Lock()
	f.lastIndex = uint64(meta.Index)
	f.mu.Unlock()
	return nil
}
