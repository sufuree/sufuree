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
//   - controller: a controller's certificate: the control API's server
//     certificate and its Raft peer identity; never a control API caller.
//
// CA key custody: with an AWS KMS key (`sufuree ca init --kms-key-id`),
// the key exists on disk only wrapped by KMS (ca.key.kms), bound to the CA
// certificate by the encryption context, and is unwrapped in memory only
// to issue (RFC-0001 envelope custody; required for PHI). Without one, it
// is a local 0600 file, for development and local qualification. There is
// no revocation list yet: certificates are short-lived (default 30 days)
// and a compromised one is contained by rotating the CA.
package controlauth

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"syscall"
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

// caKind separates the CAs this package keeps. A CA of one kind is never
// loaded as, or issues certificates of, the other: the kind is its
// certificate's common name and its wrapped key's KMS purpose.
type caKind struct {
	commonName string
	purpose    string
}

var (
	controlKind = caKind{commonName: "sufuree control CA", purpose: "sufuree-control-ca"}
	// storeKind is the object store's CA (store.go).
	storeKind = caKind{commonName: "sufuree store CA", purpose: StoreCAPurpose}
)

// CA is a loaded CA: the controller CA, or the object store's (store.go).
type CA struct {
	Cert *x509.Certificate
	Key  crypto.Signer
	PEM  []byte
	kind caKind
}

// InitCA creates a new CA in dir (0700) with its key in a local 0600 file
// (development and local qualification only) and refuses to replace one.
func InitCA(dir string, validFor time.Duration) error {
	return InitCAWrapped(context.Background(), dir, validFor, nil)
}

// InitCAWrapped creates a new CA in dir. With a wrapper, the key is stored
// only encrypted (ca.key.kms), bound to the CA certificate; it never
// touches the disk in plaintext.
func InitCAWrapped(ctx context.Context, dir string, validFor time.Duration, w KeyWrapper) error {
	return initCA(ctx, dir, validFor, w, controlKind)
}

func initCA(ctx context.Context, dir string, validFor time.Duration, w KeyWrapper, kind caKind) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return err
	}
	for _, f := range []string{caKeyFile, caKeyWrappedFile} {
		if _, err := os.Lstat(filepath.Join(dir, f)); err == nil {
			return errors.New("controlauth: a CA already exists here; refusing to replace it")
		} else if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
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
		Subject:               pkix.Name{CommonName: kind.commonName},
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
	// The certificate first (replacing a stale one from a failed init), the
	// key last: a CA exists only once its key does.
	certPath := filepath.Join(dir, caCertFile)
	_ = os.Remove(certPath)
	if err := writeExclusive(certPath, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o644); err != nil {
		return err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	if w == nil {
		return writeExclusive(filepath.Join(dir, caKeyFile), keyPEM, 0o600)
	}
	ct, keyID, err := w.Wrap(ctx, keyPEM, wrapContext(kind, der))
	if err != nil {
		return err
	}
	blob, err := encodeWrapped(keyID, ct)
	if err != nil {
		return err
	}
	return writeExclusive(filepath.Join(dir, caKeyWrappedFile), blob, 0o600)
}

// LoadCA reads a CA directory whose key is a local file. The key must be
// readable only by its owner.
func LoadCA(dir string) (*CA, error) {
	return LoadCAWrapped(context.Background(), dir, nil)
}

// Wrapped reports whether dir's CA key is KMS-wrapped.
func Wrapped(dir string) bool {
	_, err := os.Lstat(filepath.Join(dir, caKeyWrappedFile))
	return err == nil
}

// LoadCAWrapped reads a CA directory, unwrapping a wrapped key with w. A
// wrapped key needs w; a plaintext key is refused when w is set, so a
// custody downgrade is not silent. The key must match the certificate.
func LoadCAWrapped(ctx context.Context, dir string, w KeyWrapper) (*CA, error) {
	return loadCA(ctx, dir, w, controlKind)
}

func loadCA(ctx context.Context, dir string, w KeyWrapper, kind caKind) (*CA, error) {
	certPEM, err := os.ReadFile(filepath.Join(dir, caCertFile))
	if err != nil {
		return nil, err
	}
	cert, err := parseCert(certPEM)
	if err != nil {
		return nil, err
	}
	if cert.Subject.CommonName != kind.commonName {
		return nil, fmt.Errorf("controlauth: %s holds %q, not the %s", dir, cert.Subject.CommonName, kind.commonName)
	}
	var keyPEM []byte
	if Wrapped(dir) {
		if w == nil {
			return nil, errors.New("controlauth: the CA key is KMS-wrapped; a KMS key is required")
		}
		blob, err := readPrivate(filepath.Join(dir, caKeyWrappedFile))
		if err != nil {
			return nil, err
		}
		wk, err := decodeWrapped(blob)
		if err != nil {
			return nil, err
		}
		if keyPEM, err = w.Unwrap(ctx, wk.Ciphertext, wrapContext(kind, cert.Raw)); err != nil {
			return nil, err
		}
	} else {
		if w != nil {
			return nil, errors.New("controlauth: a KMS key was given but the CA key is a plaintext file")
		}
		if keyPEM, err = readPrivate(filepath.Join(dir, caKeyFile)); err != nil {
			return nil, err
		}
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
	if pub, ok := signer.Public().(interface{ Equal(crypto.PublicKey) bool }); !ok || !pub.Equal(cert.PublicKey) {
		return nil, errors.New("controlauth: the CA key does not match the CA certificate")
	}
	return &CA{Cert: cert, Key: signer, PEM: certPEM, kind: kind}, nil
}

// Issued is a new certificate and its private key, PEM encoded.
type Issued struct {
	CertPEM []byte
	KeyPEM  []byte
}

// Issue creates a certificate for p, valid for validFor. Server names (DNS
// names or IP addresses) are added for a controller certificate.
func (ca *CA) Issue(p Principal, validFor time.Duration, serverNames []string) (Issued, error) {
	if ca.kind != controlKind {
		return Issued{}, errors.New("controlauth: only the control CA issues principals")
	}
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
		// Server auth for the control API; client auth too, for Raft peers
		// (the control API middleware still refuses this role as a caller).
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
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
	if err := writeExclusive(prefix+".crt", is.CertPEM, 0o644); err != nil {
		_ = os.Remove(prefix + ".key") // no orphan key without its certificate
		return err
	}
	return nil
}

// ReadPrivate reads a private key file as readPrivate does, for other
// packages' secrets.
func ReadPrivate(path string) ([]byte, error) { return readPrivate(path) }

// readPrivate opens path once without following a symlink and checks the
// opened file is regular and readable only by its owner.
func readPrivate(path string) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() || st.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("controlauth: %s must be a regular file readable only by its owner", path)
	}
	return io.ReadAll(io.LimitReader(f, 1<<16))
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
