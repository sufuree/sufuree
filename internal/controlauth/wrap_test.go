package controlauth

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// fakeKMS seals with AES-GCM, binding the encryption context as AAD, as
// KMS does.
type fakeKMS struct {
	key   []byte
	calls int
}

func newFakeKMS(t *testing.T) *fakeKMS {
	k := make([]byte, 32)
	if _, err := rand.Read(k); err != nil {
		t.Fatal(err)
	}
	return &fakeKMS{key: k}
}

func aad(ec map[string]string) []byte {
	keys := make([]string, 0, len(ec))
	for k := range ec {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b bytes.Buffer
	for _, k := range keys {
		b.WriteString(k + "=" + ec[k] + ";")
	}
	return b.Bytes()
}

func (f *fakeKMS) gcm() cipher.AEAD {
	blk, _ := aes.NewCipher(f.key)
	g, _ := cipher.NewGCM(blk)
	return g
}

func (f *fakeKMS) Wrap(_ context.Context, pt []byte, ec map[string]string) ([]byte, string, error) {
	f.calls++
	g := f.gcm()
	nonce := make([]byte, g.NonceSize())
	_, _ = rand.Read(nonce)
	return g.Seal(nonce, nonce, pt, aad(ec)), "arn:aws:kms:us-west-1:000000000000:key/test", nil
}

func (f *fakeKMS) Unwrap(_ context.Context, ct []byte, ec map[string]string) ([]byte, error) {
	f.calls++
	g := f.gcm()
	if len(ct) < g.NonceSize() {
		return nil, errors.New("short")
	}
	return g.Open(nil, ct[:g.NonceSize()], ct[g.NonceSize():], aad(ec))
}

func TestWrappedCA(t *testing.T) {
	ctx := context.Background()
	kms := newFakeKMS(t)
	dir := filepath.Join(t.TempDir(), "ca")
	if err := InitCAWrapped(ctx, dir, 24*time.Hour, kms); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(dir, "ca.key")); err == nil {
		t.Fatal("a wrapped CA wrote a plaintext key")
	}
	raw, err := os.ReadFile(filepath.Join(dir, "ca.key.kms"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "PRIVATE KEY") {
		t.Fatal("ca.key.kms holds the key in the clear")
	}
	if st, _ := os.Stat(filepath.Join(dir, "ca.key.kms")); st.Mode().Perm() != 0o600 {
		t.Fatalf("ca.key.kms mode %v", st.Mode().Perm())
	}
	if id, err := WrappedKeyID(dir); err != nil || id != "arn:aws:kms:us-west-1:000000000000:key/test" {
		t.Fatalf("WrappedKeyID = %q, %v", id, err)
	}
	if err := InitCA(dir, time.Hour); err == nil {
		t.Fatal("InitCA replaced a wrapped CA")
	}
	ca, err := LoadCAWrapped(ctx, dir, kms)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ca.Issue(Principal{RoleOperator, "op"}, time.Hour, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCA(dir); err == nil {
		t.Fatal("a wrapped CA loaded without a KMS key")
	}
	if _, err := LoadCAWrapped(ctx, dir, newFakeKMS(t)); err == nil {
		t.Fatal("a wrapped CA unwrapped under another key")
	}

	// The wrapped key is bound to its certificate: beside another CA's
	// certificate it does not unwrap.
	other := filepath.Join(t.TempDir(), "other")
	if err := InitCAWrapped(ctx, other, 24*time.Hour, kms); err != nil {
		t.Fatal(err)
	}
	cert, _ := os.ReadFile(filepath.Join(other, "ca.crt"))
	if err := os.WriteFile(filepath.Join(dir, "ca.crt"), cert, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCAWrapped(ctx, dir, kms); err == nil {
		t.Fatal("a wrapped key unwrapped beside another CA's certificate")
	}

	// A KMS key given for a plaintext CA is refused (no silent downgrade).
	plain := filepath.Join(t.TempDir(), "plain")
	if err := InitCA(plain, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCAWrapped(ctx, plain, kms); err == nil {
		t.Fatal("a plaintext CA key was accepted as KMS custody")
	}
	var wk wrappedKey
	_ = json.Unmarshal(raw, &wk)
	if wk.Version != 1 || len(wk.Ciphertext) == 0 {
		t.Fatalf("wrapped file = %+v", wk)
	}
}

func TestLoadCARefusesMismatchedKey(t *testing.T) {
	a, b := filepath.Join(t.TempDir(), "a"), filepath.Join(t.TempDir(), "b")
	for _, d := range []string{a, b} {
		if err := InitCA(d, time.Hour); err != nil {
			t.Fatal(err)
		}
	}
	cert, _ := os.ReadFile(filepath.Join(b, "ca.crt"))
	if err := os.WriteFile(filepath.Join(a, "ca.crt"), cert, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCA(a); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("mismatched key and certificate: %v", err)
	}
}

func issueTo(t *testing.T, ca *CA, dir string, p Principal) (cert, key string) {
	t.Helper()
	is, err := ca.Issue(p, time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	prefix := filepath.Join(dir, p.Role+"-"+p.Name)
	if err := WriteIssued(prefix, is); err != nil {
		t.Fatal(err)
	}
	return prefix + ".crt", prefix + ".key"
}

func TestPeerTLS(t *testing.T) {
	dir := t.TempDir()
	caDir := filepath.Join(dir, "ca")
	if err := InitCA(caDir, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	ca, err := LoadCA(caDir)
	if err != nil {
		t.Fatal(err)
	}
	caFile := filepath.Join(caDir, "ca.crt")
	c1, k1 := issueTo(t, ca, dir, Principal{RoleController, "c1"})
	c2, k2 := issueTo(t, ca, dir, Principal{RoleController, "c2"})
	co, ko := issueTo(t, ca, dir, Principal{RoleOperator, "op"})
	srv, _, err := PeerTLS(c1, k1, caFile)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := tls.Listen("tcp", "127.0.0.1:0", srv)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() { _ = c.(*tls.Conn).Handshake(); _ = c.Close() }()
		}
	}()
	dial := func(cfg *tls.Config) error {
		c, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", ln.Addr().String(), cfg)
		if err != nil {
			return err
		}
		defer c.Close()
		// TLS 1.3 reports a refused client certificate on the first read.
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, err = c.Read(make([]byte, 1))
		if errors.Is(err, os.ErrDeadlineExceeded) || err != nil && strings.Contains(err.Error(), "EOF") {
			return nil
		}
		return err
	}
	_, peer, err := PeerTLS(c2, k2, caFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := dial(peer); err != nil {
		t.Fatalf("controller peer refused: %v", err)
	}
	// An operator certificate from the same CA is not a Raft peer, either
	// way round.
	_, opCli, err := PeerTLS(co, ko, caFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := dial(opCli); err == nil {
		t.Fatal("an operator certificate joined as a Raft peer")
	}
	opSrv, _, _ := PeerTLS(co, ko, caFile)
	ln2, err := tls.Listen("tcp", "127.0.0.1:0", opSrv)
	if err != nil {
		t.Fatal(err)
	}
	defer ln2.Close()
	go func() {
		if c, err := ln2.Accept(); err == nil {
			_ = c.(*tls.Conn).Handshake()
			_ = c.Close()
		}
	}()
	if _, err := tls.Dial("tcp", ln2.Addr().String(), peer); err == nil {
		t.Fatal("a controller dialled an operator as a Raft peer")
	}
	// A controller certificate from another CA is refused.
	otherDir := filepath.Join(dir, "other")
	if err := InitCA(otherDir, time.Hour); err != nil {
		t.Fatal(err)
	}
	oca, _ := LoadCA(otherDir)
	cx, kx := issueTo(t, oca, otherDir, Principal{RoleController, "c1"})
	_, foreign, err := PeerTLS(cx, kx, caFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := dial(foreign); err == nil {
		t.Fatal("a controller from another CA joined as a Raft peer")
	}
}
