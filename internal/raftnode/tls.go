package raftnode

import (
	"crypto/tls"
	"errors"
	"net"
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

func (s *tlsStream) Accept() (net.Conn, error) { return s.ln.Accept() }
func (s *tlsStream) Close() error              { return s.ln.Close() }
func (s *tlsStream) Addr() net.Addr            { return s.ln.Addr() }

// Dial connects and completes the handshake (so the peer is verified)
// before Raft writes to it.
func (s *tlsStream) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	d := &tls.Dialer{NetDialer: &net.Dialer{Timeout: timeout}, Config: s.client}
	return d.Dial("tcp", string(address))
}
