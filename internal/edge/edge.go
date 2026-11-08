// Package edge is the version-one public HTTP gateway. It routes by Host
// header or by the /svc/<name>/ path prefix, asks an activator to admit the
// workload, and only then forwards bytes. /healthz is answered locally and
// does not activate anything.
package edge

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Activator admits one named service. release must be called when the
// forwarded request finishes; it may be nil. A non-nil error means nothing
// was forwarded.
type Activator interface {
	Acquire(r *http.Request, service string) (addr string, release func(), err error)
}

// Server is an http.Handler. MaxConcurrent <= 0 does not cap admission.
// MaxBodyBytes <= 0 defaults to 1 MiB. RetryAfter is the 503 delay; zero
// means one second.
type Server struct {
	Activator     Activator
	Hosts         map[string]string // lowercase host, no port → service name
	MaxConcurrent int
	MaxBodyBytes  int64
	RetryAfter    time.Duration
	Client        *http.Client

	mu   sync.Mutex
	open int
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
		return
	}
	if s.MaxConcurrent > 0 && !s.admit() {
		http.Error(w, "too many requests\n", http.StatusTooManyRequests)
		return
	}
	if s.MaxConcurrent > 0 {
		defer s.release()
	}
	service, path, ok := s.route(r)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if s.Activator == nil {
		writeRetry(w, s.retryAfter(), "service unavailable\n")
		return
	}
	addr, done, err := s.Activator.Acquire(r, service)
	if err != nil {
		writeRetry(w, s.retryAfter(), "service unavailable\n")
		return
	}
	if done != nil {
		defer done()
	}
	if err := s.forward(w, r, addr, path); err != nil && !errors.Is(err, errBodyTooLarge) {
		if r.Context().Err() != nil {
			return
		}
		http.Error(w, "bad gateway\n", http.StatusBadGateway)
	}
}

func (s *Server) route(r *http.Request) (service, path string, ok bool) {
	const pfx = "/svc/"
	if strings.HasPrefix(r.URL.Path, pfx) {
		rest := strings.TrimPrefix(r.URL.Path, pfx)
		name, sub, found := strings.Cut(rest, "/")
		if name == "" || strings.Contains(name, "/") {
			return "", "", false
		}
		if !found || sub == "" {
			return name, "/", true
		}
		return name, "/" + sub, true
	}
	host := r.Host
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.ToLower(host)
	if s.Hosts == nil {
		return "", "", false
	}
	name, ok := s.Hosts[host]
	if !ok || name == "" {
		return "", "", false
	}
	return name, r.URL.Path, true
}

func (s *Server) admit() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.open >= s.MaxConcurrent {
		return false
	}
	s.open++
	return true
}

func (s *Server) release() {
	s.mu.Lock()
	s.open--
	s.mu.Unlock()
}

func (s *Server) retryAfter() time.Duration {
	if s.RetryAfter > 0 {
		return s.RetryAfter
	}
	return time.Second
}

func (s *Server) maxBody() int64 {
	if s.MaxBodyBytes > 0 {
		return s.MaxBodyBytes
	}
	return 1 << 20
}

var errBodyTooLarge = errors.New("edge: body too large")

func (s *Server) forward(w http.ResponseWriter, r *http.Request, addr, path string) error {
	if r.ContentLength > s.maxBody() {
		http.Error(w, "request entity too large\n", http.StatusRequestEntityTooLarge)
		return errBodyTooLarge
	}
	body := http.MaxBytesReader(w, r.Body, s.maxBody())
	target := "http://" + addr + path
	if r.URL.RawQuery != "" {
		target += "?" + r.URL.RawQuery
	}
	req, err := http.NewRequestWithContext(r.Context(), r.Method, target, body)
	if err != nil {
		return err
	}
	copyHeader(req.Header, r.Header)
	req.Host = addr
	client := s.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			http.Error(w, "request entity too large\n", http.StatusRequestEntityTooLarge)
			return errBodyTooLarge
		}
		return err
	}
	defer resp.Body.Close()
	copyHeader(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	_, err = io.Copy(w, resp.Body)
	return err
}

func writeRetry(w http.ResponseWriter, d time.Duration, msg string) {
	sec := int(d / time.Second)
	if sec < 1 {
		sec = 1
	}
	w.Header().Set("Retry-After", strconv.Itoa(sec))
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = io.WriteString(w, msg)
}

func copyHeader(dst, src http.Header) {
	for k, vs := range src {
		if hopByHop(k) {
			continue
		}
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
}

func hopByHop(k string) bool {
	switch strings.ToLower(k) {
	case "connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade":
		return true
	default:
		return false
	}
}

// Handler returns s so callers can pass it to http.Server.
func Handler(s *Server) http.Handler {
	if s == nil {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, fmt.Sprintf("edge: nil server\n"), http.StatusServiceUnavailable)
		})
	}
	return s
}
