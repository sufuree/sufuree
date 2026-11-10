package raftnode

import (
	"context"
	"io"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/sufuree/sufuree/internal/controlauth"
)

func peerTLS(t *testing.T, ca *controlauth.CA, caFile, dir string, p controlauth.Principal) *PeerTLS {
	t.Helper()
	is, err := ca.Issue(p, time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	prefix := filepath.Join(dir, p.Role+"-"+p.Name)
	if err := controlauth.WriteIssued(prefix, is); err != nil {
		t.Fatal(err)
	}
	s, c, err := controlauth.PeerTLS(prefix+".crt", prefix+".key", caFile)
	if err != nil {
		t.Fatal(err)
	}
	return &PeerTLS{Server: s, Client: c}
}

func TestTLSTransport(t *testing.T) {
	dir := t.TempDir()
	caDir := filepath.Join(dir, "ca")
	if err := controlauth.InitCA(caDir, time.Hour); err != nil {
		t.Fatal(err)
	}
	ca, err := controlauth.LoadCA(caDir)
	if err != nil {
		t.Fatal(err)
	}
	caFile := filepath.Join(caDir, "ca.crt")
	c1 := peerTLS(t, ca, caFile, dir, controlauth.Principal{Role: controlauth.RoleController, Name: "c1"})
	c2 := peerTLS(t, ca, caFile, dir, controlauth.Principal{Role: controlauth.RoleController, Name: "c2"})
	op := peerTLS(t, ca, caFile, dir, controlauth.Principal{Role: controlauth.RoleOperator, Name: "op"})

	// A single TLS voter elects itself and applies.
	n, err := Open(Config{DataDir: filepath.Join(dir, "raft"), NodeID: "controller-1", BindAddr: freeAddr(t), Bootstrap: true, TLS: c1}, (&record{}).fsm())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	waitLeader(t, n)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.Apply(ctx, []byte(`{"op":"put","payload":{"k":"v"}}`), 10*time.Second); err != nil {
		t.Fatal(err)
	}

	// The stream carries bytes between controllers and refuses others.
	s1, err := newTLSStream("127.0.0.1:0", c1)
	if err != nil {
		t.Fatal(err)
	}
	defer s1.Close()
	go func() {
		for {
			c, err := s1.Accept()
			if err != nil {
				return
			}
			go func() { _, _ = io.Copy(c, c); _ = c.Close() }()
		}
	}()
	s2, err := newTLSStream("127.0.0.1:0", c2)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	conn, err := s2.Dial(raft.ServerAddress(s1.Addr().String()), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 4)
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadFull(conn, buf); err != nil || string(buf) != "ping" {
		t.Fatalf("echo = %q, %v", buf, err)
	}
	_ = conn.Close()

	so, err := newTLSStream("127.0.0.1:0", op)
	if err != nil {
		t.Fatal(err)
	}
	defer so.Close()
	go func() {
		for {
			c, err := so.Accept()
			if err != nil {
				return
			}
			go func() { _, _ = c.Read(make([]byte, 1)); _ = c.Close() }()
		}
	}()
	bad, err := so.Dial(raft.ServerAddress(s1.Addr().String()), 5*time.Second)
	if err == nil {
		_, _ = bad.Write([]byte("ping"))
		_ = bad.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.ReadFull(bad, buf); err == nil {
			t.Fatal("an operator certificate exchanged Raft bytes with a controller")
		}
		_ = bad.Close()
	}
	start := time.Now()
	if _, err := s1.Dial(raft.ServerAddress(so.Addr().String()), 5*time.Second); err == nil || !strings.Contains(err.Error(), "controlauth: raft peer") {
		t.Fatalf("a controller dialled an operator as its peer: %v", err)
	} else if time.Since(start) > 3*time.Second {
		t.Fatalf("refused only by timeout: %v", err)
	}
}

func TestTLSAcceptDropsSilentClient(t *testing.T) {
	dir := t.TempDir()
	caDir := filepath.Join(dir, "ca")
	if err := controlauth.InitCA(caDir, time.Hour); err != nil {
		t.Fatal(err)
	}
	ca, err := controlauth.LoadCA(caDir)
	if err != nil {
		t.Fatal(err)
	}
	c1 := peerTLS(t, ca, filepath.Join(caDir, "ca.crt"), dir, controlauth.Principal{Role: controlauth.RoleController, Name: "c1"})
	old := handshakeTimeout
	handshakeTimeout = 300 * time.Millisecond
	defer func() { handshakeTimeout = old }()
	s, err := newTLSStream("127.0.0.1:0", c1)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	raw, err := net.Dial("tcp", s.Addr().String()) // connects, never handshakes
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	c, err := s.Accept()
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if _, err := c.Read(make([]byte, 1)); err == nil {
		t.Fatal("a silent client completed a handshake")
	}
	if d := time.Since(start); d > 3*time.Second {
		t.Fatalf("silent client held the connection for %v", d)
	}
}
