// Package gateway exposes each workload on a host-local Unix socket (mode
// 0600) and forwards each connection to the workload's guest, holding an
// activity lease for the connection's lifetime. The socket's file mode is
// the caller authorization boundary in this single-host slice, which keeps
// the per-installation socket contract existing edges already use.
package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Acquirer is the activator surface the gateway needs.
type Acquirer interface {
	Acquire(ctx context.Context) (addr string, release func(), err error)
}

// Gateway serves one workload's socket.
type Gateway struct {
	ID          string
	SocketPath  string
	Act         Acquirer
	WakeTimeout time.Duration // bounded wait for activation
	Logger      *slog.Logger

	l  net.Listener
	wg sync.WaitGroup
}

// Listen binds the socket: parent 0700, socket 0600, a stale socket file
// replaced, anything else at the path refused.
func (g *Gateway) Listen() error {
	if err := os.MkdirAll(filepath.Dir(g.SocketPath), 0o700); err != nil {
		return err
	}
	if fi, err := os.Lstat(g.SocketPath); err == nil {
		if fi.Mode()&os.ModeSocket == 0 {
			return fmt.Errorf("gateway: %s exists and is not a socket", g.SocketPath)
		}
		if err := os.Remove(g.SocketPath); err != nil {
			return err
		}
	}
	l, err := net.Listen("unix", g.SocketPath)
	if err != nil {
		return err
	}
	if err := os.Chmod(g.SocketPath, 0o600); err != nil {
		_ = l.Close()
		return err
	}
	g.l = l
	return nil
}

// Serve accepts until ctx ends, then closes the socket and waits for open
// connections to finish.
func (g *Gateway) Serve(ctx context.Context) error {
	go func() { <-ctx.Done(); _ = g.l.Close() }()
	for {
		c, err := g.l.Accept()
		if err != nil {
			g.wg.Wait()
			_ = os.Remove(g.SocketPath)
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		g.wg.Add(1)
		go func() {
			defer g.wg.Done()
			g.handle(c)
		}()
	}
}

// unavailable is written when activation fails: the edge speaks HTTP, so a
// defined retryable status beats a bare connection reset.
const unavailable = "HTTP/1.1 503 Service Unavailable\r\nContent-Type: text/plain\r\nRetry-After: 2\r\nContent-Length: 28\r\nConnection: close\r\n\r\nworkload is starting; retry\n"

func (g *Gateway) handle(c net.Conn) {
	defer func() { _ = c.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), g.WakeTimeout)
	addr, release, err := g.Act.Acquire(ctx)
	cancel()
	if err != nil {
		g.Logger.Warn("activation failed", "workload", g.ID, "err", err)
		_, _ = io.WriteString(c, unavailable)
		return
	}
	defer release()
	up, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		g.Logger.Warn("guest dial failed", "workload", g.ID, "err", err)
		_, _ = io.WriteString(c, unavailable)
		return
	}
	defer func() { _ = up.Close() }()
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(up, c); closeWrite(up); done <- struct{}{} }()
	go func() { _, _ = io.Copy(c, up); closeWrite(c); done <- struct{}{} }()
	<-done
	<-done
}

func closeWrite(c net.Conn) {
	if cw, ok := c.(interface{ CloseWrite() error }); ok {
		_ = cw.CloseWrite()
	}
}
