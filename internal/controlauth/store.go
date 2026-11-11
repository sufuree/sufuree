package controlauth

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"regexp"
	"time"
)

// The store CA.
//
// The object store (SeaweedFS) authenticates its components to each other,
// and Sufuree's controller to its filer, with certificates from a private
// CA of its own. That CA is kept the way the control CA is: its key exists
// on disk only wrapped by a KMS key (required for PHI), and each
// certificate is issued on its own, so one component's can be replaced
// without touching the others.
//
// It is a separate trust domain from the control CA. The two differ in
// their certificate's common name and their wrapped key's KMS purpose, and
// neither loads as, nor issues certificates of, the other.

// StoreCAPurpose is the KMS encryption context purpose a wrapped store CA
// key is bound to. Its KMS key's policy allows no other.
const StoreCAPurpose = "sufuree-store-ca"

// storeNamePattern is a store certificate's name: the component it
// authenticates. SeaweedFS admits gRPC peers by this name
// (allowed_commonNames, a comma-separated list), so it carries no comma,
// space or dot.
// hostnamePattern is a DNS name: dot-separated labels of letters, digits
// and inner hyphens. A name that isn't one, such as one with a space, is
// refused rather than issued as a SAN no client would match.
var hostnamePattern = regexp.MustCompile(`^(?i:[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?)(\.(?i:[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?))*$`)

var storeNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)

// InitStoreCA creates a new store CA in dir, as InitCAWrapped does a
// control CA.
func InitStoreCA(ctx context.Context, dir string, validFor time.Duration, w KeyWrapper) error {
	return initCA(ctx, dir, validFor, w, storeKind)
}

// LoadStoreCA reads a store CA directory, as LoadCAWrapped does a control
// CA's. A control CA's directory is refused.
func LoadStoreCA(ctx context.Context, dir string, w KeyWrapper) (*CA, error) {
	return loadCA(ctx, dir, w, storeKind)
}

// IssueStore creates a store certificate named name (its common name),
// valid for validFor. With server names (DNS names or IP addresses) it is
// a component's certificate, for serving and for calling other components;
// without, it is a client's only (an operator's, or Sufuree's controller).
func (ca *CA) IssueStore(name string, validFor time.Duration, serverNames []string) (Issued, error) {
	if ca.kind != storeKind {
		return Issued{}, errors.New("controlauth: only the store CA issues store certificates")
	}
	if !storeNamePattern.MatchString(name) {
		return Issued{}, fmt.Errorf("controlauth: store certificate name %q must match %s", name, storeNamePattern)
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
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: name},
		NotBefore:             now.Add(-5 * time.Minute),
		NotAfter:              now.Add(validFor),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	if len(serverNames) > 0 {
		tmpl.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}
		for _, n := range serverNames {
			if ip := net.ParseIP(n); ip != nil {
				tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
			} else if hostnamePattern.MatchString(n) && len(n) <= 253 {
				tmpl.DNSNames = append(tmpl.DNSNames, n)
			} else {
				return Issued{}, fmt.Errorf("controlauth: server name %q is neither an IP address nor a hostname", n)
			}
		}
	}
	if !ca.Cert.NotAfter.After(now.Add(time.Hour)) {
		return Issued{}, fmt.Errorf("controlauth: the store CA expires %s; replace it before issuing", ca.Cert.NotAfter.UTC().Format(time.RFC3339))
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
