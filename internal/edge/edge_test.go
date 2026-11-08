package edge

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeAct struct {
	addr string
	err  error
	n    atomic.Int32
}

func (f *fakeAct) Acquire(*http.Request, string) (string, func(), error) {
	f.n.Add(1)
	if f.err != nil {
		return "", nil, f.err
	}
	return f.addr, func() {}, nil
}

func TestHealthzDoesNotWake(t *testing.T) {
	t.Parallel()
	act := &fakeAct{err: errWake}
	srv := httptest.NewServer(&Server{Activator: act})
	t.Cleanup(srv.Close)
	res, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != 200 || string(body) != "ok\n" {
		t.Fatalf("status=%d body=%q", res.StatusCode, body)
	}
	if act.n.Load() != 0 {
		t.Fatalf("healthz acquisitions = %d", act.n.Load())
	}
}

var errWake = errString("should not wake")

type errString string

func (e errString) Error() string { return string(e) }

func TestPathAndHostRouting(t *testing.T) {
	t.Parallel()
	var gotPath, gotBody, gotHost string
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		gotHost = r.Host
		w.Header().Set("X-Up", "1")
		w.WriteHeader(201)
		_, _ = io.WriteString(w, "pong")
	}))
	t.Cleanup(up.Close)
	act := &fakeAct{addr: up.Listener.Addr().String()}
	srv := httptest.NewServer(&Server{Activator: act, Hosts: map[string]string{"app.example": "web"}})
	t.Cleanup(srv.Close)

	res, err := http.Post(srv.URL+"/svc/web/v1/items?x=1", "text/plain", stringsReader("hi"))
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 201 || string(body) != "pong" || res.Header.Get("X-Up") != "1" {
		t.Fatalf("path proxy status=%d body=%q header=%s", res.StatusCode, body, res.Header.Get("X-Up"))
	}
	if gotPath != "/v1/items" || gotBody != "hi" {
		t.Fatalf("upstream path=%q body=%q", gotPath, gotBody)
	}
	if act.n.Load() != 1 {
		t.Fatalf("acquires=%d", act.n.Load())
	}

	req, err := http.NewRequest(http.MethodGet, srv.URL+"/health", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Host = "app.example"
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 201 || gotPath != "/health" || gotHost == "" {
		t.Fatalf("host route status=%d path=%q host=%q", res.StatusCode, gotPath, gotHost)
	}
}

func TestActivationFailureIs503(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(&Server{
		Activator:  &fakeAct{err: errString("boot")},
		RetryAfter: 3 * time.Second,
	})
	t.Cleanup(srv.Close)
	res, err := http.Get(srv.URL + "/svc/web/")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status=%d", res.StatusCode)
	}
	if res.Header.Get("Retry-After") != "3" {
		t.Fatalf("Retry-After=%q", res.Header.Get("Retry-After"))
	}
}

func TestMaxConcurrency429(t *testing.T) {
	t.Parallel()
	started := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	act := activatorFunc(func(*http.Request, string) (string, func(), error) {
		calls.Add(1)
		started <- struct{}{}
		<-release
		return "127.0.0.1:1", func() {}, nil
	})
	srv := httptest.NewServer(&Server{Activator: act, MaxConcurrent: 1})
	t.Cleanup(srv.Close)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		res, err := http.Get(srv.URL + "/svc/web/x")
		if err != nil {
			t.Errorf("first: %v", err)
			return
		}
		res.Body.Close()
	}()
	<-started
	res, err := http.Get(srv.URL + "/svc/web/y")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status=%d", res.StatusCode)
	}
	if calls.Load() != 1 {
		t.Fatalf("second request woke the activator, calls=%d", calls.Load())
	}
	hz, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	hz.Body.Close()
	if hz.StatusCode != 200 {
		t.Fatalf("healthz during saturation = %d", hz.StatusCode)
	}
	close(release)
	wg.Wait()
}

type activatorFunc func(*http.Request, string) (string, func(), error)

func (f activatorFunc) Acquire(r *http.Request, service string) (string, func(), error) {
	return f(r, service)
}

func stringsReader(s string) *stringReader { return &stringReader{s: s} }

type stringReader struct{ s string }

func (r *stringReader) Read(p []byte) (int, error) {
	if len(r.s) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.s)
	r.s = r.s[n:]
	return n, nil
}
