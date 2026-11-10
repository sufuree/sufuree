package raftnode

import (
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// PeerTLS authenticates Raft peers both ways: Server for accepted
// connections, Client for dialled ones. Both must verify the peer's
// certificate (controlauth.PeerTLS builds them).
type PeerTLS struct {
	Server *tls.Config
	Client *tls.Config
}

// tlsStream is a raft.StreamLayer over mutually authenticated TLS.
type tlsStream struct {
	ln     net.Listener
	client *tls.Config
}

func newTLSStream(bind string, p *PeerTLS) (*tlsStream, error) {
	if p == nil || p.Server == nil || p.Client == nil {
		return nil, errors.New("raftnode: peer TLS needs server and client configs")
	}
	ln, err := tls.Listen("tcp", bind, p.Server)
	if err != nil {
		return nil, err
	}
	return &tlsStream{ln: ln, client: p.Client}, nil
}

// handshakeTimeout bounds an accepted connection's TLS handshake, so a
// peer that connects and never authenticates doesn't hold it open.
var handshakeTimeout = 10 * time.Second // var for tests

// Accept returns a connection that completes its handshake, under a
// deadline, on first use. The handshake isn't done here: one slow client
// must not stall the accept loop.
func (s *tlsStream) Accept() (net.Conn, error) {
	c, err := s.ln.Accept()
	if err != nil {
		return nil, err
	}
	tc, ok := c.(*tls.Conn)
	if !ok {
		_ = c.Close()
		return nil, errors.New("raftnode: accepted a non-TLS connection")
	}
	if err := tc.SetDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		_ = tc.Close()
		return nil, err
	}
	return &handshakeConn{Conn: tc}, nil
}

// handshakeConn handshakes on first Read or Write, then clears the
// handshake deadline.
type handshakeConn struct {
	*tls.Conn
	once sync.Once
	err  error
}

func (c *handshakeConn) handshake() error {
	c.once.Do(func() {
		if c.err = c.Conn.Handshake(); c.err != nil {
			_ = c.Conn.Close()
			return
		}
		c.err = c.Conn.SetDeadline(time.Time{})
	})
	return c.err
}

func (c *handshakeConn) Read(b []byte) (int, error) {
	if err := c.handshake(); err != nil {
		return 0, err
	}
	return c.Conn.Read(b)
}

func (c *handshakeConn) Write(b []byte) (int, error) {
	if err := c.handshake(); err != nil {
		return 0, err
	}
	return c.Conn.Write(b)
}

func (s *tlsStream) Close() error   { return s.ln.Close() }
func (s *tlsStream) Addr() net.Addr { return s.ln.Addr() }

// Dial connects and completes the handshake (so the peer is verified)
// before Raft writes to it.
func (s *tlsStream) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	d := &tls.Dialer{NetDialer: &net.Dialer{Timeout: timeout}, Config: s.client}
	return d.Dial("tcp", string(address))
}
