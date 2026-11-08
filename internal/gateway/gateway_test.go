package gateway

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type fakeAct struct {
	addr     string
	err      error
	active   atomic.Int32
	acquires atomic.Int32
}

func (f *fakeAct) Acquire(context.Context) (string, func(), error) {
	f.acquires.Add(1)
	if f.err != nil {
		return "", nil, f.err
	}
	f.active.Add(1)
	return f.addr, func() { f.active.Add(-1) }, nil
}

func shortSock(t *testing.T) string {
	t.Helper()
	d, err := os.MkdirTemp("", "gw")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(d) })
	return filepath.Join(d, "s", "w.sock")
}

func start(t *testing.T, act Acquirer) string {
	t.Helper()
	g := &Gateway{ID: "w", SocketPath: shortSock(t), Act: act, WakeTimeout: time.Second, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if err := g.Listen(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = g.Serve(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return g.SocketPath
}

func TestForwardsAndHoldsLeasePerConnection(t *testing.T) {
	up := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "hello "+r.URL.Path)
	})}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = up.Serve(l) }()
	defer func() { _ = up.Close() }()

	act := &fakeAct{addr: l.Addr().String()}
	sock := start(t, act)
	fi, err := os.Stat(sock)
	if err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("socket mode = %v, %v", fi.Mode().Perm(), err)
	}
	c, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.WriteString(c, "GET /x HTTP/1.1\r\nHost: g\r\n\r\n")
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 100))
	if string(body) != "hello /x" {
		t.Fatalf("body = %q", body)
	}
	if act.active.Load() != 1 {
		t.Fatalf("lease not held while connection open: %d", act.active.Load())
	}
	_ = c.Close()
	deadline := time.Now().Add(2 * time.Second)
	for act.active.Load() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("lease not released after close")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestActivationFailureAnswers503(t *testing.T) {
	sock := start(t, &fakeAct{err: errors.New("boom")})
	c, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	_, _ = io.WriteString(c, "GET / HTTP/1.1\r\nHost: g\r\n\r\n")
	resp, err := http.ReadResponse(bufio.NewReader(c), nil)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 503 || resp.Header.Get("Retry-After") == "" || !strings.Contains(string(body), "retry") {
		t.Fatalf("got %d %q", resp.StatusCode, body)
	}
}

func TestListenRefusesNonSocket(t *testing.T) {
	p := shortSock(t)
	_ = os.MkdirAll(filepath.Dir(p), 0o700)
	if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	g := &Gateway{SocketPath: p}
	if err := g.Listen(); err == nil {
		t.Fatal("listened over a regular file")
	}
}
