package main

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sufuree/sufuree/internal/raftnode"
	"github.com/sufuree/sufuree/internal/reconcile"
	"github.com/sufuree/sufuree/internal/store"
)

func TestOneAgentPerDataDir(t *testing.T) {
	dir := t.TempDir()
	unlock, err := lockHost(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockHost(dir); err == nil {
		t.Fatal("a second agent took the same data_dir")
	}
	unlock()
	again, err := lockHost(dir)
	if err != nil {
		t.Fatalf("lock after release: %v", err)
	}
	again()
}

func TestVersion(t *testing.T) {
	if version != "0.1.0-rfc0001" {
		t.Fatalf("version = %s", version)
	}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = w
	err = run([]string{"version"})
	w.Close()
	os.Stdout = old
	if err != nil {
		t.Fatal(err)
	}
	out, _ := io.ReadAll(r)
	if strings.TrimSpace(string(out)) != "0.1.0-rfc0001" {
		t.Fatalf("stdout = %q", out)
	}
}

func TestHostRequiresConfig(t *testing.T) {
	err := run([]string{"host"})
	if err == nil || !strings.Contains(err.Error(), "--config") {
		t.Fatalf("err = %v", err)
	}
}

func TestAppApplyAndGet(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	c := reconcile.New(st, nil, nil, reconcile.Options{})
	srv := httptest.NewServer(controlHandler(nil, c))
	t.Cleanup(srv.Close)
	dir := t.TempDir()
	spec := filepath.Join(dir, "web.json")
	body := []byte(`{
	  "name": "web",
	  "kind": "App",
	  "lifecycle": "enabled",
	  "spec": {
	    "image": "img",
	    "memory_mib": 128,
	    "guest_port": 8080,
	    "idle_timeout": "60s",
	    "startup_timeout": "10s",
	    "min_replicas": 0,
	    "max_replicas": 1
	  }
	}`)
	if err := os.WriteFile(spec, body, 0o600); err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	err = run([]string{"app", "apply", "--file", spec, "--controller", srv.URL})
	w.Close()
	os.Stdout = old
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(r)
	if !bytes.Contains(got, []byte(`"name": "web"`)) || !bytes.Contains(got, []byte(`"resource_version": 1`)) {
		t.Fatalf("apply output = %s", got)
	}
	r, w, err = os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = w
	err = run([]string{"app", "get", "web", "--controller", srv.URL})
	w.Close()
	os.Stdout = old
	if err != nil {
		t.Fatal(err)
	}
	got, _ = io.ReadAll(r)
	if !bytes.Contains(got, []byte(`"uid": "app-web"`)) {
		t.Fatalf("get output = %s", got)
	}
}

func TestControllerRaftFlags(t *testing.T) {
	dir := t.TempDir()
	err := runController([]string{"--data-dir", dir, "--listen", "127.0.0.1:0", "--raft-bootstrap"})
	if err == nil || !strings.Contains(err.Error(), "--raft-id") {
		t.Fatalf("err = %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("data dir = %v", entries)
	}

	err = runController([]string{
		"--data-dir", dir,
		"--listen", "127.0.0.1:0",
		"--raft-bind", "127.0.0.1:7000",
		"--raft-id", "controller-1",
	})
	if !errors.Is(err, raftnode.ErrRecoveryRequired) {
		t.Fatalf("err = %v", err)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "raft", "raft-log.db")); !os.IsNotExist(statErr) {
		t.Fatalf("raft log created: %v", statErr)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "control.db")); !os.IsNotExist(statErr) {
		t.Fatalf("control store created: %v", statErr)
	}
}

type leaderStub struct {
	leader bool
}

func (s leaderStub) IsLeader() bool { return s.leader }

func TestControlMutationRequiresLeader(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	c := reconcile.New(st, nil, nil, reconcile.Options{})
	blocked := httptest.NewServer(controlHandler(leaderStub{leader: false}, c))
	t.Cleanup(blocked.Close)
	res := putControl(t, blocked.URL+"/v1/resources/app/web", `{}`)
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", res.StatusCode)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if !bytes.Contains(body, []byte("not leader")) {
		t.Fatalf("body = %s", body)
	}
	got, err := http.Get(blocked.URL + "/v1/resources")
	if err != nil {
		t.Fatal(err)
	}
	got.Body.Close()
	if got.StatusCode != http.StatusOK {
		t.Fatalf("list status = %d", got.StatusCode)
	}

	open := httptest.NewServer(controlHandler(leaderStub{leader: true}, c))
	t.Cleanup(open.Close)
	res = putControl(t, open.URL+"/v1/resources/app/web", `{
	  "name": "web",
	  "kind": "App",
	  "lifecycle": "enabled",
	  "spec": {
	    "image": "img",
	    "memory_mib": 128,
	    "guest_port": 8080,
	    "idle_timeout": "60s",
	    "startup_timeout": "10s",
	    "min_replicas": 0,
	    "max_replicas": 1
	  }
	}`)
	if res.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		t.Fatalf("status = %d body = %s", res.StatusCode, body)
	}
	res.Body.Close()
}

func putControl(t *testing.T, url, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPut, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return res
}
