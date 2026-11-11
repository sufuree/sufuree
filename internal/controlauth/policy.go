package controlauth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path"
	"strings"
)

type ctxKey struct{}

// FromContext returns the authenticated principal of a request that passed
// Middleware.
func FromContext(ctx context.Context) (Principal, bool) {
	p, ok := ctx.Value(ctxKey{}).(Principal)
	return p, ok
}

// Allowed reports whether p may call method on urlPath. It is default deny:
// a route not listed here is refused to everyone.
func Allowed(p Principal, method, urlPath string) bool {
	clean := path.Clean(urlPath)
	if clean != urlPath && clean+"/" != urlPath {
		return false // refuse ambiguous spellings rather than normalize them
	}
	seg := strings.Split(strings.Trim(clean, "/"), "/")
	if len(seg) < 2 || seg[0] != "v1" {
		return false
	}
	is := func(roles ...string) bool {
		for _, r := range roles {
			if p.Role == r {
				return true
			}
		}
		return false
	}
	get, post, put := method == http.MethodGet, method == http.MethodPost, method == http.MethodPut
	switch seg[1] {
	case "resources":
		// GET /v1/resources, GET|PUT|DELETE /v1/resources/{kind}/{name}.
		// DELETE does no more than a PUT to lifecycle deleting.
		switch {
		case len(seg) == 2 && get:
			return is(RoleOperator, RoleProductClient)
		case len(seg) == 4 && (get || put || method == http.MethodDelete):
			return is(RoleOperator, RoleProductClient)
		}
	case "operations", "warm":
		return len(seg) == 2 && get && is(RoleOperator)
	case "hosts":
		if len(seg) == 2 {
			return get && is(RoleOperator)
		}
		self := p.Role == RoleHost && p.Name == seg[2]
		switch {
		case len(seg) == 4 && seg[3] == "assignments" && get:
			return self || is(RoleOperator)
		case len(seg) == 5 && (seg[3] == "credentials" || seg[3] == "boot") && post:
			return self
		case len(seg) == 4 && seg[3] == "progress" && post:
			return self
		}
	case "installations":
		// /v1/installations/{id}/floor[/lower-request|/lower-approve]
		if len(seg) < 4 || seg[3] != "floor" {
			return false
		}
		switch {
		case len(seg) == 4 && get:
			return is(RoleOperator, RoleProductClient)
		case len(seg) == 5 && seg[4] == "lower-request" && post:
			return is(RoleProductClient)
		case len(seg) == 5 && seg[4] == "lower-approve" && post:
			return is(RoleOperator)
		}
	}
	return false
}

// Middleware requires a verified client certificate naming one principal
// that Allowed admits for the request, and puts the principal in the
// request context. The server's tls.Config must verify client certificates
// (ServerTLS).
func Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil || len(r.TLS.VerifiedChains) == 0 || len(r.TLS.VerifiedChains[0]) == 0 {
			http.Error(w, "a verified client certificate is required", http.StatusUnauthorized)
			return
		}
		p, err := FromCert(r.TLS.VerifiedChains[0][0])
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}
		// Percent-encoded paths are refused: the policy and the router
		// must see the same segments.
		if r.URL.RawPath != "" || p.Role == RoleController || !Allowed(p, r.Method, r.URL.Path) {
			http.Error(w, fmt.Sprintf("%s may not %s %s", p, r.Method, r.URL.Path), http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), ctxKey{}, p)))
	})
}

func certPool(caFile string) (*x509.CertPool, error) {
	b, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(b) {
		return nil, fmt.Errorf("controlauth: %s holds no certificate", caFile)
	}
	return pool, nil
}

// ServerTLS is the control API's TLS: its own certificate, TLS 1.3, and a
// required client certificate from the controller CA.
func ServerTLS(certFile, keyFile, clientCAFile string) (*tls.Config, error) {
	cert, err := loadKeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	pool, err := certPool(clientCAFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
	}, nil
}

// TLSFlags are a client's --tls-cert, --tls-key and --tls-ca.
type TLSFlags struct{ Cert, Key, CA string }

// Set reports whether any of the flags is set.
func (f TLSFlags) Set() bool { return f.Cert != "" || f.Key != "" || f.CA != "" }

// Client returns an HTTP client for the control API at baseURL. An https
// URL needs all three flags; an http URL (development only) must not have
// any, so a typo cannot silently send a certificate key in the clear.
func (f TLSFlags) Client(baseURL string) (*http.Client, error) {
	https := strings.HasPrefix(baseURL, "https://")
	if !https {
		if f.Set() {
			return nil, errors.New("--tls-* flags need an https:// controller URL")
		}
		return http.DefaultClient, nil
	}
	if f.Cert == "" || f.Key == "" || f.CA == "" {
		return nil, errors.New("an https:// controller needs --tls-cert, --tls-key and --tls-ca")
	}
	cert, err := loadKeyPair(f.Cert, f.Key)
	if err != nil {
		return nil, err
	}
	pool, err := certPool(f.CA)
	if err != nil {
		return nil, err
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, RootCAs: pool}
	return &http.Client{Transport: tr}, nil
}

// loadKeyPair loads a certificate and its key; the key must be a regular
// 0600 file, opened without following a symlink.
func loadKeyPair(certFile, keyFile string) (tls.Certificate, error) {
	keyPEM, err := readPrivate(keyFile)
	if err != nil {
		return tls.Certificate{}, err
	}
	certPEM, err := os.ReadFile(certFile)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.X509KeyPair(certPEM, keyPEM)
}

// PeerTLS returns the Raft peer TLS configs: TLS 1.3 both ways, this
// controller's certificate, and a peer that must present a certificate
// from the controller CA naming a controller principal. Peers are dialled
// by address, so the peer is verified by CA and role, not host name.
func PeerTLS(certFile, keyFile, caFile string) (server, client *tls.Config, err error) {
	cert, err := loadKeyPair(certFile, keyFile)
	if err != nil {
		return nil, nil, err
	}
	pool, err := certPool(caFile)
	if err != nil {
		return nil, nil, err
	}
	verify := func(usage x509.ExtKeyUsage) func(tls.ConnectionState) error {
		return func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("controlauth: raft peer sent no certificate")
			}
			inter := x509.NewCertPool()
			for _, c := range cs.PeerCertificates[1:] {
				inter.AddCert(c)
			}
			if _, err := cs.PeerCertificates[0].Verify(x509.VerifyOptions{Roots: pool, Intermediates: inter, KeyUsages: []x509.ExtKeyUsage{usage}}); err != nil {
				return fmt.Errorf("controlauth: raft peer: %w", err)
			}
			p, err := FromCert(cs.PeerCertificates[0])
			if err != nil {
				return err
			}
			if p.Role != RoleController {
				return fmt.Errorf("controlauth: raft peer %s is not a controller", p)
			}
			return nil
		}
	}
	server = &tls.Config{
		MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert},
		ClientAuth:       tls.RequireAnyClientCert,
		VerifyConnection: verify(x509.ExtKeyUsageClientAuth),
	}
	client = &tls.Config{
		MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert},
		InsecureSkipVerify: true, // replaced by VerifyConnection: CA and role, not host name
		VerifyConnection:   verify(x509.ExtKeyUsageServerAuth),
	}
	return server, client, nil
}
