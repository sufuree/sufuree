package guest

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/sufuree/sufuree/internal/workload"
)

// HTTP boots an in-process HTTP guest on 127.0.0.1. It is the fixed-host
// qualification stand-in for QEMU/KVM: same Launcher/Guest contract, no
// unikernel image, ops, or KVM. Production hosts use QEMU.
type HTTP struct {
	// Handler serves the guest. nil answers 200 with X-Generation set from
	// the boot env's GenerationEnvKey (or "0").
	Handler http.Handler
}

// Boot implements Launcher.
func (h *HTTP) Boot(_ context.Context, w workload.Spec, generation uint64, env map[string]string, _ map[string][]byte) (Guest, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	gen := env[w.GenerationEnvKey]
	if gen == "" {
		gen = fmt.Sprintf("%d", generation)
	}
	handler := h.Handler
	if handler == nil {
		handler = http.HandlerFunc(func(rw http.ResponseWriter, _ *http.Request) {
			rw.Header().Set("X-Generation", gen)
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("ok\n"))
		})
	}
	srv := &http.Server{Handler: handler}
	g := &httpGuest{ln: ln, srv: srv, addr: ln.Addr().String(), exited: make(chan struct{})}
	go func() {
		_ = srv.Serve(ln)
		close(g.exited)
	}()
	return g, nil
}

type httpGuest struct {
	ln     net.Listener
	srv    *http.Server
	addr   string
	exited chan struct{}
	once   sync.Once
}

func (g *httpGuest) Addr() string            { return g.addr }
func (g *httpGuest) Exited() <-chan struct{} { return g.exited }

func (g *httpGuest) Stop(grace time.Duration) error {
	var err error
	g.once.Do(func() {
		if grace <= 0 {
			err = g.srv.Close()
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), grace)
		defer cancel()
		err = g.srv.Shutdown(ctx)
	})
	return err
}
