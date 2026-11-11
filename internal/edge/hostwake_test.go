package edge

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

type stubHost struct {
	addr string
	n    int
}

func (s *stubHost) Acquire(context.Context) (string, func(), error) {
	s.n++
	return s.addr, func() {}, nil
}

func TestHostWakeProxiesThroughActivator(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "guest")
	}))
	t.Cleanup(up.Close)
	host := &stubHost{addr: up.Listener.Addr().String()}
	srv := httptest.NewServer(&Server{
		Activator: &HostWake{Service: "web", Act: host, Timeout: time.Second},
	})
	t.Cleanup(srv.Close)

	res, err := http.Get(srv.URL + "/svc/web/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 200 || string(body) != "guest" || host.n != 1 {
		t.Fatalf("status=%d body=%q acquires=%d", res.StatusCode, body, host.n)
	}

	res, err = http.Get(srv.URL + "/svc/other/")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 503 {
		t.Fatalf("wrong service status=%d", res.StatusCode)
	}
}
