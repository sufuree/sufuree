package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// testPKI writes a CA, a server certificate for 127.0.0.1 and a client
// certificate to dir, and returns the CA pool and the server certificate.
func testPKI(t *testing.T, dir string) (*x509.CertPool, tls.Certificate) {
	t.Helper()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, _ := x509.ParseCertificate(caDER)
	leaf := func(serial int64, cn string, usage x509.ExtKeyUsage) ([]byte, *ecdsa.PrivateKey) {
		k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		tmpl := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: cn},
			NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
			IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1)}, ExtKeyUsage: []x509.ExtKeyUsage{usage},
			KeyUsage: x509.KeyUsageDigitalSignature}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &k.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		return der, k
	}
	srvDER, srvKey := leaf(2, "filer", x509.ExtKeyUsageServerAuth)
	cliDER, cliKey := leaf(3, "admin", x509.ExtKeyUsageClientAuth)
	keyPEM := func(k *ecdsa.PrivateKey) string {
		b, _ := x509.MarshalECPrivateKey(k)
		return string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: b}))
	}
	certPEM := func(der []byte) string {
		return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	}
	writeFile(t, dir, "ca.crt", certPEM(caDER), 0o644)
	writeFile(t, dir, "client.crt", certPEM(cliDER), 0o644)
	writeFile(t, dir, "client.key", keyPEM(cliKey), 0o600)
	writeFile(t, dir, "client-open.key", keyPEM(cliKey), 0o644)
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	return pool, tls.Certificate{Certificate: [][]byte{srvDER}, PrivateKey: srvKey}
}

// TestFilerClientPresentsItsCertificate checks the filer client against a
// server that, like the SeaweedFS TLS profile's filer, requires a client
// certificate from its CA.
func TestFilerClientPresentsItsCertificate(t *testing.T) {
	dir := t.TempDir()
	pool, srvCert := testPKI(t, dir)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(r.TLS.PeerCertificates[0].Subject.CommonName))
	}))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{srvCert}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert}
	srv.StartTLS()
	defer srv.Close()
	ca := filepath.Join(dir, "ca.crt")

	hc, err := httpClient(ca, filepath.Join(dir, "client.crt"), filepath.Join(dir, "client.key"))
	if err != nil {
		t.Fatal(err)
	}
	res, err := hc.Get(srv.URL)
	if err != nil {
		t.Fatalf("with the client certificate: %v", err)
	}
	_ = res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status %d", res.StatusCode)
	}

	bare, err := httpClient(ca, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if res, err := bare.Get(srv.URL); err == nil {
		_ = res.Body.Close()
		t.Fatal("the server admitted a client without a certificate")
	}

	for name, args := range map[string][3]string{
		"cert without key":     {ca, filepath.Join(dir, "client.crt"), ""},
		"key without cert":     {ca, "", filepath.Join(dir, "client.key")},
		"world-readable key":   {ca, filepath.Join(dir, "client.crt"), filepath.Join(dir, "client-open.key")},
		"mismatched cert, key": {ca, ca, filepath.Join(dir, "client.key")},
	} {
		if _, err := httpClient(args[0], args[1], args[2]); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := httpClient(ca, filepath.Join(dir, "client.crt"), ""); err == nil || !strings.Contains(err.Error(), "both") {
		t.Errorf("half a client certificate: %v", err)
	}
}
