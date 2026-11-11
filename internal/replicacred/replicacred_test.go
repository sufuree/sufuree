package replicacred

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testKey(b byte) []byte { return bytes.Repeat([]byte{b}, KeySize) }

func TestDeriveIsStableAndSeparatesInstallationsAndVersions(t *testing.T) {
	t.Parallel()
	d1, err := NewDeriver(testKey(1), 1)
	if err != nil {
		t.Fatal(err)
	}
	again, _ := NewDeriver(testKey(1), 1)
	d2, _ := NewDeriver(testKey(1), 2)
	other, _ := NewDeriver(testKey(2), 1)
	a, _ := d1.For("inst-a")
	if b, _ := again.For("inst-a"); a != b {
		t.Fatal("the same key, version and installation derived different credentials")
	}
	if !strings.HasPrefix(a.AccessKeyID, AccessKeyPrefix) || len(a.AccessKeyID) != 20 || len(a.SecretAccessKey) != 43 {
		t.Fatalf("credential shape: %d-char access key, %d-char secret", len(a.AccessKeyID), len(a.SecretAccessKey))
	}
	for name, d := range map[string]*Deriver{"installation": d1, "version": d2, "key": other} {
		inst := "inst-a"
		if name == "installation" {
			inst = "inst-b"
		}
		c, _ := d.For(inst)
		if c.AccessKeyID == a.AccessKeyID || c.SecretAccessKey == a.SecretAccessKey {
			t.Errorf("a different %s derived the same credentials", name)
		}
	}
}

func TestNewDeriverRefusesBadKeysAndVersions(t *testing.T) {
	t.Parallel()
	if _, err := NewDeriver(testKey(1)[:16], 1); err == nil {
		t.Error("a short key was accepted")
	}
	if _, err := NewDeriver(testKey(1), 0); err == nil {
		t.Error("version 0 was accepted")
	}
}

func TestInstallationsThatCouldEscapeThePrefixAreRefused(t *testing.T) {
	t.Parallel()
	d, _ := NewDeriver(testKey(1), 1)
	for _, id := range []string{"", "a/b", "..", "a..b", ".hidden", "-x", "a*", "a b", strings.Repeat("a", 129)} {
		if _, err := d.For(id); err == nil {
			t.Errorf("installation %q was accepted", id)
		}
		if _, err := Identity("zati", id, Credentials{}); err == nil {
			t.Errorf("identity for %q was accepted", id)
		}
	}
	for _, b := range []string{"", "a/b", "a*", "a:b"} {
		if _, err := Identity(b, "inst-a", Credentials{}); err == nil {
			t.Errorf("bucket %q was accepted", b)
		}
	}
}

func TestIdentityIsLimitedToTheInstallationPrefix(t *testing.T) {
	t.Parallel()
	doc, err := Identity("zati", "inst-a", Credentials{AccessKeyID: "AK", SecretAccessKey: "SK"})
	if err != nil {
		t.Fatal(err)
	}
	var got identity
	if err := json.Unmarshal(doc, &got); err != nil {
		t.Fatal(err)
	}
	want := []string{"Read:zati/inst/inst-a/*", "Write:zati/inst/inst-a/*", "List:zati/inst/inst-a/*", "Tagging:zati/inst/inst-a/*",
		"Write:zati/hints/inst-a/*"}
	if got.Name != "sufuree-inst-a" || strings.Join(got.Actions, ",") != strings.Join(want, ",") {
		t.Fatalf("identity = %+v", got)
	}
	if len(got.Credentials) != 1 || got.Credentials[0] != (identityCredit{AccessKey: "AK", SecretKey: "SK", Status: "Active"}) {
		t.Fatalf("credentials = %+v", got.Credentials)
	}
}

type fakeKMS struct{ fail bool }

func (fakeKMS) Wrap(_ context.Context, p []byte, ec map[string]string) ([]byte, string, error) {
	if ec["purpose"] != Purpose {
		return nil, "", errors.New("wrong purpose")
	}
	out := append([]byte("wrapped:"), p...)
	return out, "arn:aws:kms:us-west-1:1:key/k", nil
}

func (f fakeKMS) Unwrap(_ context.Context, ct []byte, ec map[string]string) ([]byte, error) {
	if f.fail || ec["purpose"] != Purpose {
		return nil, errors.New("AccessDenied")
	}
	return bytes.TrimPrefix(ct, []byte("wrapped:")), nil
}

func TestKeyFiles(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()
	wrapped, plain := filepath.Join(dir, "w.json"), filepath.Join(dir, "p.json")
	if err := InitKey(ctx, wrapped, fakeKMS{}); err != nil {
		t.Fatal(err)
	}
	if err := InitKey(ctx, plain, nil); err != nil {
		t.Fatal(err)
	}
	if err := InitKey(ctx, plain, nil); err == nil {
		t.Fatal("an existing key file was replaced")
	}
	if st, _ := os.Stat(wrapped); st.Mode().Perm() != 0o600 {
		t.Fatalf("key file mode %v", st.Mode().Perm())
	}
	wb, _ := os.ReadFile(wrapped)
	pb, _ := os.ReadFile(plain)
	if id, err := KeyID(wb); err != nil || id == "" {
		t.Fatalf("wrapped KeyID = %q, %v", id, err)
	}
	if id, err := KeyID(pb); err != nil || id != "" {
		t.Fatalf("plaintext KeyID = %q, %v", id, err)
	}
	if k, err := LoadKey(ctx, wb, fakeKMS{}); err != nil || len(k) != KeySize {
		t.Fatalf("wrapped load: %d bytes, %v", len(k), err)
	}
	if k, err := LoadKey(ctx, pb, nil); err != nil || len(k) != KeySize {
		t.Fatalf("plaintext load: %d bytes, %v", len(k), err)
	}
	if _, err := LoadKey(ctx, wb, nil); err == nil {
		t.Error("a wrapped key loaded without an unwrapper")
	}
	if _, err := LoadKey(ctx, pb, fakeKMS{}); err == nil {
		t.Error("a plaintext key loaded where KMS was expected")
	}
	if _, err := LoadKey(ctx, wb, fakeKMS{fail: true}); err == nil {
		t.Error("a key loaded although KMS refused")
	}
	if _, err := LoadKey(ctx, []byte(`{"version":1,"key":"00","kms_key_id":"x","ciphertext":"AA=="}`), nil); err == nil {
		t.Error("a file both wrapped and plaintext was accepted")
	}
}

func verifyToken(tok string, key []byte) error {
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		return fmt.Errorf("token has %d parts", len(parts))
	}
	m := hmac.New(sha256.New, key)
	m.Write([]byte(parts[0] + "." + parts[1]))
	if base64.RawURLEncoding.EncodeToString(m.Sum(nil)) != parts[2] {
		return errors.New("token signature does not verify")
	}
	p, _ := base64.RawURLEncoding.DecodeString(parts[1])
	var claims struct{ Exp int64 }
	if err := json.Unmarshal(p, &claims); err != nil || claims.Exp <= time.Now().Unix() {
		return fmt.Errorf("claims %s", p)
	}
	return nil
}

// fakeFiler serves identity files the way the SeaweedFS filer does, and
// checks every request's token.
type fakeFiler struct {
	t           *testing.T
	read, write []byte
	mu          sync.Mutex
	files       map[string][]byte
	puts, dels  int
}

func (f *fakeFiler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := f.write
	if r.Method == http.MethodGet {
		key = f.read
	}
	if err := verifyToken(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), key); err != nil {
		f.t.Errorf("%s %s: %v", r.Method, r.URL.Path, err)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	switch r.Method {
	case http.MethodGet:
		b, ok := f.files[r.URL.Path]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(b)
	case http.MethodPut:
		b, _ := io.ReadAll(r.Body)
		f.files[r.URL.Path] = b
		f.puts++
		w.WriteHeader(http.StatusCreated)
	case http.MethodDelete:
		delete(f.files, r.URL.Path)
		f.dels++
		w.WriteHeader(http.StatusNoContent)
	}
}

func TestFilerRegistrar(t *testing.T) {
	t.Parallel()
	ff := &fakeFiler{t: t, read: []byte("read-key-0123456789"), write: []byte("write-key-0123456789"), files: map[string][]byte{}}
	srv := httptest.NewServer(ff)
	t.Cleanup(srv.Close)
	var live atomic.Bool
	live.Store(true)
	probes := 0
	reg := &FilerRegistrar{BaseURL: srv.URL, ReadKey: ff.read, WriteKey: ff.write, ProbeTimeout: 300 * time.Millisecond,
		Probe: func(context.Context, string, string, Credentials) error {
			probes++
			if !live.Load() {
				return errors.New("InvalidAccessKeyId")
			}
			return nil
		}}
	ctx := context.Background()
	c := Credentials{AccessKeyID: "AK", SecretAccessKey: "SK"}
	doc, _ := Identity("zati", "inst-a", c)
	if err := reg.Register(ctx, "zati", "inst-a", doc, c); err != nil {
		t.Fatal(err)
	}
	if got := ff.files["/etc/iam/identities/sufuree-inst-a.json"]; !bytes.Equal(got, doc) || ff.puts != 1 {
		t.Fatalf("registered %q after %d puts", got, ff.puts)
	}
	if err := reg.Register(ctx, "zati", "inst-a", doc, c); err != nil || ff.puts != 1 {
		t.Fatalf("an unchanged identity was rewritten (%d puts, %v)", ff.puts, err)
	}
	// A registration the store doesn't take is removed, so it can't block
	// every other identity's reload.
	live.Store(false)
	c2 := Credentials{AccessKeyID: "AK2", SecretAccessKey: "SK2"}
	doc2, _ := Identity("zati", "inst-a", c2)
	err := reg.Register(ctx, "zati", "inst-a", doc2, c2)
	if err == nil || !strings.Contains(err.Error(), "removed") || strings.Contains(err.Error(), "SK2") {
		t.Fatalf("unaccepted registration: %v", err)
	}
	if _, ok := ff.files["/etc/iam/identities/sufuree-inst-a.json"]; ok || ff.dels != 1 {
		t.Fatal("an identity the store did not accept was left in place")
	}
	if probes < 3 {
		t.Fatalf("the probe ran %d times; it must retry while the store reloads", probes)
	}
	// Remove deletes a registered identity, and a missing one is no error.
	live.Store(true)
	if err := reg.Register(ctx, "zati", "inst-a", doc, c); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := reg.Remove(ctx, "inst-a"); err != nil {
			t.Fatalf("Remove %d: %v", i, err)
		}
	}
	if _, ok := ff.files["/etc/iam/identities/sufuree-inst-a.json"]; ok {
		t.Fatal("Remove left the identity in place")
	}
}

type countingRegistrar struct {
	calls   atomic.Int32
	gate    chan struct{}
	err     error
	mu      sync.Mutex
	events  []string
	removed []string
}

func (r *countingRegistrar) Register(_ context.Context, _, installation string, _ []byte, _ Credentials) error {
	r.calls.Add(1)
	if r.gate != nil {
		<-r.gate
	}
	r.mu.Lock()
	r.events = append(r.events, "register "+installation)
	r.mu.Unlock()
	return r.err
}

func (r *countingRegistrar) Remove(_ context.Context, installation string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, "remove "+installation)
	r.removed = append(r.removed, installation)
	return nil
}

func TestServiceRevokeWaitsForRegistrationThenRefusesIssue(t *testing.T) {
	t.Parallel()
	d, _ := NewDeriver(testKey(1), 1)
	reg := &countingRegistrar{gate: make(chan struct{})}
	s, err := NewService(d, reg, "zati", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	issued := make(chan error, 1)
	go func() {
		_, err := s.Issue(context.Background(), "inst-a")
		issued <- err
	}()
	for reg.calls.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	revoked := make(chan error, 1)
	go func() { revoked <- s.Revoke(context.Background(), "inst-a") }()
	select {
	case err := <-revoked:
		t.Fatalf("Revoke returned before the registration finished: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(reg.gate)
	if err := <-revoked; err != nil {
		t.Fatal(err)
	}
	<-issued
	reg.mu.Lock()
	got := strings.Join(reg.events, ",")
	reg.mu.Unlock()
	if got != "register inst-a,remove inst-a" {
		t.Fatalf("events = %s", got)
	}
	if _, err := s.Issue(context.Background(), "inst-a"); !errors.Is(err, ErrRevoked) {
		t.Fatalf("Issue after Revoke = %v", err)
	}
	if n := reg.calls.Load(); n != 1 {
		t.Fatalf("registered %d times", n)
	}
	if _, err := s.Issue(context.Background(), "inst-b"); err != nil {
		t.Fatalf("another installation: %v", err)
	}
	if err := s.Revoke(context.Background(), "../x"); err == nil {
		t.Fatal("Revoke accepted an invalid installation")
	}
}

func TestServiceRegistersOnceAndSharesConcurrentRegistrations(t *testing.T) {
	t.Parallel()
	d, _ := NewDeriver(testKey(1), 1)
	reg := &countingRegistrar{gate: make(chan struct{})}
	s, err := NewService(d, reg, "zati", time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	want, _ := d.For("inst-a")
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c, err := s.Issue(context.Background(), "inst-a")
			if err == nil && c != want {
				err = errors.New("wrong credentials")
			}
			errs <- err
		}()
	}
	for reg.calls.Load() == 0 {
		time.Sleep(time.Millisecond)
	}
	close(reg.gate)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Issue(context.Background(), "inst-a"); err != nil {
		t.Fatal(err)
	}
	if n := reg.calls.Load(); n != 1 {
		t.Fatalf("registered %d times", n)
	}
}

func TestServiceReturnsNoCredentialsWhenRegistrationFails(t *testing.T) {
	t.Parallel()
	d, _ := NewDeriver(testKey(1), 1)
	reg := &countingRegistrar{err: errors.New("filer down")}
	s, _ := NewService(d, reg, "zati", time.Hour)
	if c, err := s.Issue(context.Background(), "inst-a"); err == nil || c != (Credentials{}) {
		t.Fatalf("Issue = %+v, %v", c, err)
	}
	if _, err := s.Issue(context.Background(), "inst-a"); err == nil || reg.calls.Load() != 2 {
		t.Fatal("a failed registration was remembered as done")
	}
}

func (r *ctxRegistrar) Remove(context.Context, string) error { return nil }

type ctxRegistrar struct {
	entered chan struct{}
	gate    chan struct{}
	ctxs    chan error
}

func (r *ctxRegistrar) Register(ctx context.Context, _, _ string, _ []byte, _ Credentials) error {
	close(r.entered)
	<-r.gate
	r.ctxs <- ctx.Err()
	return ctx.Err()
}

func TestAFirstCallerThatGivesUpDoesNotFailTheOthers(t *testing.T) {
	t.Parallel()
	d, _ := NewDeriver(testKey(1), 1)
	reg := &ctxRegistrar{entered: make(chan struct{}), gate: make(chan struct{}), ctxs: make(chan error, 1)}
	s, _ := NewService(d, reg, "zati", time.Hour)
	first, cancel := context.WithCancel(context.Background())
	firstErr := make(chan error, 1)
	go func() {
		_, err := s.Issue(first, "inst-a")
		firstErr <- err
	}()
	<-reg.entered // the first caller started the registration
	waiterErr := make(chan error, 1)
	go func() {
		_, err := s.Issue(context.Background(), "inst-a")
		waiterErr <- err
	}()
	cancel()
	close(reg.gate)
	if err := <-reg.ctxs; err != nil {
		t.Fatalf("the registration ran under the first caller's cancelled context: %v", err)
	}
	if err := <-firstErr; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("first caller: %v", err)
	}
	if err := <-waiterErr; err != nil {
		t.Fatalf("waiter: %v", err)
	}
}
