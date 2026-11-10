// Package controlauth authenticates callers of the Sufuree control API with
// mutual TLS. A controller CA issues every certificate; each client
// certificate names exactly one principal as a URI SAN,
// sufuree://<role>/<name>, and a default-deny route policy decides what
// each role may call (Policy).
//
// Roles:
//   - host: a host agent; it may call only /v1/hosts/{id}/... for its own id.
//   - operator: a Sufuree (Zati) operator; manages resources and approves
//     floor lowerings.
//   - product-client: a product such as Zati acting for its own
//     authenticated customers; manages its resources and requests floor
//     lowerings on an owner's behalf. Customer identity stays with it.
//   - controller: the control API's own server certificate; never a client.
//
// The CA key is a local file in this version (created by `sufuree ca
// init`, mode 0600), for development and local qualification. RFC-0001's
// KMS-wrapped custody is required before PHI. There is no revocation list
// yet: certificates are short-lived (default 30 days) and a compromised one
// is contained by rotating the CA.
package controlauth

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"time"
)

// Roles.
const (
	RoleHost          = "host"
	RoleOperator      = "operator"
	RoleProductClient = "product-client"
	RoleController    = "controller"
)

// Scheme is the principal URI scheme.
const Scheme = "sufuree"

var namePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9.@_-]{0,127}$`)

// Principal is an authenticated caller.
type Principal struct {
	Role string
	Name string
}

func (p Principal) String() string { return p.Role + ":" + p.Name }

// URI returns the principal's certificate SAN.
func (p Principal) URI() *url.URL {
	return &url.URL{Scheme: Scheme, Host: p.Role, Path: "/" + p.Name}
}

// Validate checks the role and name.
func (p Principal) Validate() error {
	switch p.Role {
	case RoleHost, RoleOperator, RoleProductClient, RoleController:
	default:
		return fmt.Errorf("controlauth: unknown role %q", p.Role)
	}
	if !namePattern.MatchString(p.Name) {
		return fmt.Errorf("controlauth: principal name %q is not valid", p.Name)
	}
	return nil
}

// FromCert returns the one sufuree:// principal a certificate names.
func FromCert(c *x509.Certificate) (Principal, error) {
	var found []Principal
	for _, u := range c.URIs {
		if u.Scheme != Scheme {
			continue
		}
		if u.User != nil || u.RawQuery != "" || u.Fragment != "" || len(u.Path) < 2 {
			return Principal{}, errors.New("controlauth: malformed principal URI")
		}
		p := Principal{Role: u.Host, Name: u.Path[1:]}
		if err := p.Validate(); err != nil {
			return Principal{}, err
		}
		found = append(found, p)
	}
	if len(found) != 1 {
		return Principal{}, fmt.Errorf("controlauth: certificate names %d principals, want 1", len(found))
	}
	return found[0], nil
}

// CA file names inside a CA directory.
const (
	caCertFile = "ca.crt"
	caKeyFile  = "ca.key"
)

// CA is a loaded controller CA.
type CA struct {
	Cert *x509.Certificate
	Key  crypto.Signer
	PEM  []byte
}

// InitCA creates a new CA in dir (0700) and refuses to replace one.
func InitCA(dir string, validFor time.Duration) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return err
	}
	if _, err := os.Lstat(filepath.Join(dir, caKeyFile)); err == nil {
		return errors.New("controlauth: a CA already exists here; refusing to replace it")
	} else if !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	serial, err := newSerial()
	if err != nil {
		return err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "sufuree control CA"},
		NotBefore:             now.Add(-5 * time.Minute),
		NotAfter:              now.Add(validFor),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return err
	}
	if err := writeExclusive(filepath.Join(dir, caKeyFile), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		return err
	}
	return writeExclusive(filepath.Join(dir, caCertFile), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644)
}

// LoadCA reads a CA directory. The key must be readable only by its owner.
func LoadCA(dir string) (*CA, error) {
	certPEM, err := os.ReadFile(filepath.Join(dir, caCertFile))
	if err != nil {
		return nil, err
	}
	keyPath := filepath.Join(dir, caKeyFile)
	st, err := os.Lstat(keyPath)
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() || st.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("controlauth: %s must be a regular file readable only by its owner", keyPath)
	}
	keyPEM, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, err
	}
	cert, err := parseCert(certPEM)
	if err != nil {
		return nil, err
	}
	kb, _ := pem.Decode(keyPEM)
	if kb == nil {
		return nil, errors.New("controlauth: CA key is not PEM")
	}
	k, err := x509.ParsePKCS8PrivateKey(kb.Bytes)
	if err != nil {
		return nil, err
	}
	signer, ok := k.(crypto.Signer)
	if !ok {
		return nil, errors.New("controlauth: CA key cannot sign")
	}
	return &CA{Cert: cert, Key: signer, PEM: certPEM}, nil
}

// Issued is a new certificate and its private key, PEM encoded.
type Issued struct {
	CertPEM []byte
	KeyPEM  []byte
}

// Issue creates a certificate for p, valid for validFor. Server names (DNS
// names or IP addresses) are added for a controller certificate.
func (ca *CA) Issue(p Principal, validFor time.Duration, serverNames []string) (Issued, error) {
	if err := p.Validate(); err != nil {
		return Issued{}, err
	}
	if len(serverNames) > 0 && p.Role != RoleController {
		return Issued{}, errors.New("controlauth: only a controller certificate carries server names")
	}
	if validFor <= 0 || validFor > 397*24*time.Hour {
		return Issued{}, errors.New("controlauth: validity must be positive and at most 397 days")
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return Issued{}, err
	}
	serial, err := newSerial()
	if err != nil {
		return Issued{}, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: p.String()},
		NotBefore:    now.Add(-5 * time.Minute),
		NotAfter:     now.Add(validFor),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		URIs:         []*url.URL{p.URI()},
	}
	if p.Role == RoleController {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		for _, n := range serverNames {
			if ip := net.ParseIP(n); ip != nil {
				tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
			} else {
				tmpl.DNSNames = append(tmpl.DNSNames, n)
			}
		}
	}
	if tmpl.NotAfter.After(ca.Cert.NotAfter) {
		tmpl.NotAfter = ca.Cert.NotAfter
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.Cert, &key.PublicKey, ca.Key)
	if err != nil {
		return Issued{}, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return Issued{}, err
	}
	return Issued{
		CertPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		KeyPEM:  pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	}, nil
}

// WriteIssued writes <prefix>.crt (0644) and <prefix>.key (0600), refusing
// to replace either.
func WriteIssued(prefix string, is Issued) error {
	if err := writeExclusive(prefix+".key", is.KeyPEM, 0o600); err != nil {
		return err
	}
	return writeExclusive(prefix+".crt", is.CertPEM, 0o644)
}

func parseCert(b []byte) (*x509.Certificate, error) {
	blk, _ := pem.Decode(b)
	if blk == nil || blk.Type != "CERTIFICATE" {
		return nil, errors.New("controlauth: not a PEM certificate")
	}
	return x509.ParseCertificate(blk.Bytes)
}

func newSerial() (*big.Int, error) {
	return rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
}

func writeExclusive(path string, data []byte, mode os.FileMode) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	_, werr := f.Write(data)
	serr := f.Sync()
	cerr := f.Close()
	if err := errors.Join(werr, serr, cerr); err != nil {
		_ = os.Remove(path)
		return err
	}
	return nil
}
