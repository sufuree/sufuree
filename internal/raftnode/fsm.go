package raftnode

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/hashicorp/raft"
)

// ApplyFunc handles one committed command. payload is the raw JSON value
// from the command, and is nil when the command omitted it.
type ApplyFunc func(op string, payload []byte) error

// SnapshotFunc returns the bytes RestoreFunc will see. Raft may call it
// while a later Apply is in progress only through Persist; the func itself
// runs on the FSM thread, serialized with Apply.
type SnapshotFunc func() ([]byte, error)

// RestoreFunc replaces FSM state with the bytes from a snapshot.
type RestoreFunc func(snapshot []byte) error

// CommandFSM applies JSON commands of the form {"op":"...","payload":...}.
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

// Apply decodes a command log and calls ApplyFn.
func (f *CommandFSM) Apply(l *raft.Log) any {
	if f == nil {
		return errors.New("raftnode: nil fsm")
	}
	if l == nil || l.Type != raft.LogCommand {
		return nil
	}
	var cmd commandEnvelope
	dec := json.NewDecoder(bytes.NewReader(l.Data))
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
	if f.ApplyFn == nil {
		return errors.New("raftnode: apply func is required")
	}
	if err := f.ApplyFn(cmd.Op, []byte(cmd.Payload)); err != nil {
		return err
	}
	f.mu.Lock()
	f.lastIndex = l.Index
	f.mu.Unlock()
	return nil
}

// Snapshot captures either the user function's bytes or the last applied index.
func (f *CommandFSM) Snapshot() (raft.FSMSnapshot, error) {
	if f == nil {
		return nil, errors.New("raftnode: nil fsm")
	}
	var data []byte
	var err error
	if f.SnapshotFn != nil {
		data, err = f.SnapshotFn()
	} else {
		f.mu.Lock()
		idx := f.lastIndex
		f.mu.Unlock()
		data, err = json.Marshal(indexSnapshot{LastIndex: idx})
	}
	if err != nil {
		return nil, err
	}
	return &memorySnapshot{data: append([]byte(nil), data...)}, nil
}

// Restore loads a snapshot. User bytes are passed through when RestoreFn is set.
func (f *CommandFSM) Restore(rc io.ReadCloser) error {
	if f == nil {
		return errors.New("raftnode: nil fsm")
	}
	if rc == nil {
		return errors.New("raftnode: nil snapshot")
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	if f.RestoreFn != nil {
		return f.RestoreFn(data)
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
	f.lastIndex = snap.LastIndex
	f.mu.Unlock()
	return nil
}

type memorySnapshot struct {
	data []byte
}

func (m *memorySnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(m.data); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (m *memorySnapshot) Release() {}
