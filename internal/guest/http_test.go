package guest

import (
	"context"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/sufuree/sufuree/internal/workload"
)

func TestHTTPBootReadyAndStop(t *testing.T) {
	h := &HTTP{}
	g, err := h.Boot(context.Background(), workload.Spec{
		ID: "w", GuestPort: 8080, GenerationEnvKey: "GEN",
	}, 3, map[string]string{"GEN": "3"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Stop(0) })
	client := &http.Client{Timeout: 3 * time.Second}
	res, err := client.Get("http://" + g.Addr() + "/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 200 || string(body) != "ok\n" || res.Header.Get("X-Generation") != "3" {
		t.Fatalf("status=%d body=%q gen=%q", res.StatusCode, body, res.Header.Get("X-Generation"))
	}
	if err := g.Stop(time.Second); err != nil {
		t.Fatal(err)
	}
	select {
	case <-g.Exited():
	case <-time.After(2 * time.Second):
		t.Fatal("guest did not exit")
	}
}

func waitStarted(t *testing.T, started <-chan struct{}, errc <-chan error) {
	t.Helper()
	select {
	case <-started:
	case err := <-errc:
		t.Fatalf("request failed before handler started: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for handler start")
	}
}

func TestHTTPStopWaitsForHandlerThenSignalsExit(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	h := &HTTP{Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
		_, _ = io.WriteString(w, "done")
	})}
	g, err := h.Boot(context.Background(), workload.Spec{ID: "w", GenerationEnvKey: "GEN"}, 1, map[string]string{"GEN": "1"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Stop(0) })

	client := &http.Client{Timeout: 5 * time.Second}
	errc := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		res, err := client.Get("http://" + g.Addr() + "/")
		if err != nil {
			errc <- err
			return
		}
		res.Body.Close()
		errc <- nil
	}()
	waitStarted(t, started, errc)

	stopDone := make(chan error, 1)
	go func() { stopDone <- g.Stop(time.Second) }()

	select {
	case <-g.Exited():
		t.Fatal("Exited closed while handler still draining")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	wg.Wait()
	if err := <-errc; err != nil {
		t.Fatal(err)
	}
	if err := <-stopDone; err != nil {
		t.Fatal(err)
	}
	select {
	case <-g.Exited():
	case <-time.After(time.Second):
		t.Fatal("Exited did not close after Stop")
	}
}

func TestHTTPStopForceClosesAfterGrace(t *testing.T) {
	started := make(chan struct{})
	h := &HTTP{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	})}
	g, err := h.Boot(context.Background(), workload.Spec{ID: "w", GenerationEnvKey: "GEN"}, 1, map[string]string{"GEN": "1"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Stop(0) })

	client := &http.Client{Timeout: 5 * time.Second}
	errCh := make(chan error, 1)
	go func() {
		res, err := client.Get("http://" + g.Addr() + "/")
		if err != nil {
			errCh <- err
			return
		}
		res.Body.Close()
		errCh <- nil
	}()
	waitStarted(t, started, errCh)

	stopErr := g.Stop(30 * time.Millisecond)
	if stopErr == nil {
		t.Fatal("Stop: want timeout error when handler exceeds grace")
	}
	select {
	case <-g.Exited():
	case <-time.After(time.Second):
		t.Fatal("Exited did not close after forced Stop")
	}
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("client request survived force-close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("client did not observe force-close")
	}
}
