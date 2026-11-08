package raftnode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/sufuree/sufuree/internal/resource"
	"github.com/sufuree/sufuree/internal/store"
)

func TestBootstrapApplyRestart(t *testing.T) {
	dir := t.TempDir()
	addr := freeAddr(t)
	rec := &record{}
	n := openNode(t, dir, addr, true, rec.fsm())
	waitLeader(t, n)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	const cmd = `{"op":"put","payload":{"k":"v"}}`
	if err := n.Apply(ctx, []byte(cmd), 10*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}

	_, err := Open(Config{
		DataDir: dir, NodeID: "controller-1", BindAddr: addr, Bootstrap: true,
	}, &CommandFSM{})
	if !errors.Is(err, ErrExistingState) {
		t.Fatalf("second bootstrap err = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "raft-log.db")); err != nil {
		t.Fatal(err)
	}

	again := &record{}
	n2 := openNode(t, dir, addr, false, again.fsm())
	waitLeader(t, n2)
	waitRecord(t, again, "put {\"k\":\"v\"}")
	if got := again.copy(); len(got) != 1 {
		t.Fatalf("replayed = %q", got)
	}
}

func TestBootstrapFalseOnEmptyDir(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing")
	_, err := Open(Config{
		DataDir: missing, NodeID: "controller-1", BindAddr: "127.0.0.1:7000", Bootstrap: false,
	}, &CommandFSM{})
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("missing dir err = %v", err)
	}
	if _, statErr := os.Stat(missing); !os.IsNotExist(statErr) {
		t.Fatalf("created missing dir: %v", statErr)
	}

	dir := t.TempDir()
	_, err = Open(Config{
		DataDir: dir, NodeID: "controller-1", BindAddr: "127.0.0.1:7000", Bootstrap: false,
	}, &CommandFSM{})
	if !errors.Is(err, ErrRecoveryRequired) {
		t.Fatalf("empty dir err = %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "raft-log.db")); !os.IsNotExist(statErr) {
		t.Fatalf("created raft log: %v", statErr)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "snapshots")); !os.IsNotExist(statErr) {
		t.Fatalf("created snapshots: %v", statErr)
	}
}

func TestSingleVoterLeaderAndNonLeaderApply(t *testing.T) {
	dir := t.TempDir()
	addr := freeAddr(t)
	rec := &record{}
	n := openNode(t, dir, addr, true, rec.fsm())
	waitLeader(t, n)
	if !n.IsLeader() {
		t.Fatal("single voter is not leader")
	}
	if n.LeaderCh() == nil {
		t.Fatal("nil leader channel")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.Apply(ctx, []byte(`{"op":"put","payload":{"k":"v"}}`), 10*time.Second); err != nil {
		t.Fatal(err)
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if err := n.Apply(canceled, []byte(`{"op":"put","payload":{}}`), time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled apply err = %v", err)
	}
	if err := n.Apply(context.Background(), []byte(`{"op":"nope"}`), 0); err == nil {
		t.Fatal("zero timeout accepted")
	}
	if err := n.Raft().Shutdown().Error(); err != nil {
		t.Fatal(err)
	}
	if n.IsLeader() {
		t.Fatal("shutdown node is leader")
	}
	err := n.Apply(context.Background(), []byte(`{"op":"put","payload":{}}`), time.Second)
	if !errors.Is(err, ErrNotLeader) {
		t.Fatalf("non-leader apply err = %v", err)
	}
}

func TestSnapshotRestoresStore(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "control.db")
	raftDir := filepath.Join(dir, "raft")
	addr := freeAddr(t)

	st, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	n := openNode(t, raftDir, addr, true, storeFSM(st))
	waitLeader(t, n)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.Apply(ctx, []byte(`{"op":"put-app","payload":{}}`), 10*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := n.Raft().Snapshot().Error(); err != nil {
		t.Fatal(err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(dbPath); err != nil {
		t.Fatal(err)
	}

	st2, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	n2 := openNode(t, raftDir, addr, false, storeFSM(st2))
	waitLeader(t, n2)
	if err := n2.Close(); err != nil {
		t.Fatal(err)
	}
	assertApp(t, st2)
	if err := st2.Close(); err != nil {
		t.Fatal(err)
	}

	st3, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st3.Close()
	assertApp(t, st3)
}

func assertApp(t *testing.T, st *store.Store) {
	t.Helper()
	var got resource.Resource
	err := st.View(func(tx *store.Tx) error {
		var err error
		got, err = tx.GetResource("app-1")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "api" || got.ResourceVersion != 1 || got.Generation != 1 {
		t.Fatalf("resource = %+v", got)
	}
}

func storeFSM(st *store.Store) *CommandFSM {
	return &CommandFSM{
		ApplyFn: func(op string, _ []byte) error {
			if op != "put-app" {
				return errors.New("unexpected op " + op)
			}
			return st.Update(func(tx *store.Tx) error {
				_, err := tx.ApplyResource(sampleApp())
				return err
			})
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

func sampleApp() resource.Resource {
	return resource.Resource{
		UID:        "app-1",
		Name:       "api",
		Generation: 1,
		Kind:       resource.KindApp,
		Lifecycle:  resource.LifecycleEnabled,
		Spec: resource.AppSpec{
			Image:          "/img/api",
			MemoryMiB:      512,
			GuestPort:      8080,
			IdleTimeout:    resource.Duration{Duration: 60 * time.Second},
			StartupTimeout: resource.Duration{Duration: 20 * time.Second},
			MinReplicas:    0,
			MaxReplicas:    4,
		},
	}
}

type record struct {
	mu  sync.Mutex
	ops []string
}

func (r *record) fsm() raft.FSM {
	return &CommandFSM{
		ApplyFn: func(op string, payload []byte) error {
			r.mu.Lock()
			r.ops = append(r.ops, op+" "+string(payload))
			r.mu.Unlock()
			return nil
		},
		SnapshotFn: func() ([]byte, error) {
			r.mu.Lock()
			defer r.mu.Unlock()
			return json.Marshal(append([]string(nil), r.ops...))
		},
		RestoreFn: func(b []byte) error {
			var ops []string
			if err := json.Unmarshal(b, &ops); err != nil {
				return err
			}
			r.mu.Lock()
			r.ops = ops
			r.mu.Unlock()
			return nil
		},
	}
}

func (r *record) copy() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.ops...)
}

func openNode(t *testing.T, dir, addr string, bootstrap bool, fsm raft.FSM) *Node {
	t.Helper()
	n, err := Open(Config{
		DataDir:   dir,
		NodeID:    "controller-1",
		BindAddr:  addr,
		Bootstrap: bootstrap,
	}, fsm)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	return n
}

func waitLeader(t *testing.T, n *Node) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if n.IsLeader() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("state = %s", n.Raft().State())
}

func waitRecord(t *testing.T, rec *record, want string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		for _, op := range rec.copy() {
			if op == want {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("applied = %q, want %q", rec.copy(), want)
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}
