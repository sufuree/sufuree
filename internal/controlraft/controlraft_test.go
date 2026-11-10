package controlraft

import (
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/sufuree/sufuree/internal/raftnode"
	"github.com/sufuree/sufuree/internal/resource"
	"github.com/sufuree/sufuree/internal/store"
)

func TestJournalLocalUpdate(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	j := &Journal{Store: st}
	if err := j.Update(func(tx *store.Tx) error {
		_, err := tx.ApplyResource(sampleApp())
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var got resource.Resource
	if err := j.View(func(tx *store.Tx) error {
		var err error
		got, err = tx.GetResource("app-1")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if got.Name != "api" {
		t.Fatalf("got %+v", got)
	}
}

func TestJournalRaftRestore(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	node, err := raftnode.Open(raftnode.Config{
		DataDir:   filepath.Join(dir, "raft"),
		NodeID:    "n1",
		BindAddr:  addr,
		Bootstrap: true,
	}, StoreFSM(st))
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	deadline := time.Now().Add(10 * time.Second)
	for !node.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("not leader")
		}
		time.Sleep(20 * time.Millisecond)
	}
	j := &Journal{Store: st, Node: node}
	if err := j.Update(func(tx *store.Tx) error {
		_, err := tx.ApplyResource(sampleApp())
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var got resource.Resource
	if err := j.View(func(tx *store.Tx) error {
		var err error
		got, err = tx.GetResource("app-1")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if got.Name != "api" || got.ResourceVersion != 1 {
		t.Fatalf("got %+v", got)
	}
}

func sampleApp() resource.Resource {
	return resource.Resource{
		UID: "app-1", Name: "api", Generation: 1, Kind: resource.KindApp,
		Lifecycle: resource.LifecycleEnabled,
		Spec: resource.AppSpec{
			Image: "/img/api", MemoryMiB: 512, GuestPort: 8080,
			IdleTimeout: resource.Duration{Duration: time.Minute},
			StartupTimeout: resource.Duration{Duration: 20 * time.Second},
			MinReplicas: 0, MaxReplicas: 4,
		},
	}
}
