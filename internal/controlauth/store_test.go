package controlauth

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func leaf(t *testing.T, is Issued) *x509.Certificate {
	t.Helper()
	b, _ := pem.Decode(is.CertPEM)
	c, err := x509.ParseCertificate(b.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestStoreCAIssuesComponentAndClientCertificates(t *testing.T) {
	ctx := context.Background()
	kms := newFakeKMS(t)
	dir := filepath.Join(t.TempDir(), "store")
	if err := InitStoreCA(ctx, dir, 24*time.Hour, kms); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(dir, "ca.key")); err == nil {
		t.Fatal("a wrapped store CA wrote a plaintext key")
	}
	if err := InitStoreCA(ctx, dir, time.Hour, kms); err == nil {
		t.Fatal("InitStoreCA replaced a CA")
	}
	ca, err := LoadStoreCA(ctx, dir, kms)
	if err != nil {
		t.Fatal(err)
	}
	if ca.Cert.Subject.CommonName != "sufuree store CA" || !ca.Cert.IsCA || !ca.Cert.MaxPathLenZero {
		t.Fatalf("store CA certificate: %+v", ca.Cert.Subject)
	}

	filer, err := ca.IssueStore("filer", time.Hour, []string{"127.0.0.1", "filer.internal"})
	if err != nil {
		t.Fatal(err)
	}
	fc := leaf(t, filer)
	if fc.Subject.CommonName != "filer" || fc.IsCA || len(fc.URIs) != 0 ||
		len(fc.IPAddresses) != 1 || len(fc.DNSNames) != 1 || fc.DNSNames[0] != "filer.internal" ||
		len(fc.ExtKeyUsage) != 2 {
		t.Fatalf("component certificate: CN %q, IPs %v, DNS %v, EKU %v", fc.Subject.CommonName, fc.IPAddresses, fc.DNSNames, fc.ExtKeyUsage)
	}
	client, err := ca.IssueStore("sufuree", time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	cc := leaf(t, client)
	if cc.Subject.CommonName != "sufuree" || len(cc.ExtKeyUsage) != 1 || cc.ExtKeyUsage[0] != x509.ExtKeyUsageClientAuth || len(cc.IPAddresses)+len(cc.DNSNames) != 0 {
		t.Fatalf("client certificate: EKU %v, IPs %v, DNS %v", cc.ExtKeyUsage, cc.IPAddresses, cc.DNSNames)
	}
	// A certificate never outlives its CA.
	long, err := ca.IssueStore("admin", 300*24*time.Hour, nil)
	if err != nil || leaf(t, long).NotAfter.After(ca.Cert.NotAfter) {
		t.Fatalf("a certificate outlives its CA: %v", err)
	}
	for _, bad := range []string{"", "a,b", "a b", "Filer", "a.b", "-a", strings.Repeat("a", 64), "sufuree:x"} {
		if _, err := ca.IssueStore(bad, time.Hour, nil); err == nil {
			t.Errorf("issued a store certificate named %q", bad)
		}
	}
	if _, err := ca.IssueStore("filer", 400*24*time.Hour, nil); err == nil {
		t.Error("issued a certificate valid for more than 397 days")
	}
	if _, err := ca.IssueStore("filer", time.Hour, []string{""}); err == nil {
		t.Error("issued a certificate with an empty server name")
	}

	// The two certificates work as a mutual-TLS server and client, and the
	// client-only one is refused as a server.
	pool := x509.NewCertPool()
	pool.AddCert(ca.Cert)
	pair := func(is Issued) tls.Certificate {
		c, err := tls.X509KeyPair(is.CertPEM, is.KeyPEM)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	serve := func(is Issued) *httptest.Server {
		srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(r.TLS.PeerCertificates[0].Subject.CommonName))
		}))
		srv.TLS = &tls.Config{Certificates: []tls.Certificate{pair(is)}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert}
		srv.StartTLS()
		t.Cleanup(srv.Close)
		return srv
	}
	hc := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{pair(client)}}}}
	res, err := hc.Get(serve(filer).URL)
	if err != nil {
		t.Fatalf("mutual TLS with store certificates: %v", err)
	}
	_ = res.Body.Close()
	if res, err := hc.Get(serve(client).URL); err == nil {
		_ = res.Body.Close()
		t.Fatal("a client-only store certificate was accepted as a server")
	}
}

// TestStoreAndControlCAsAreSeparate: neither CA loads as, or issues
// certificates of, the other, wrapped or not.
func TestStoreAndControlCAsAreSeparate(t *testing.T) {
	ctx := context.Background()
	kms := newFakeKMS(t)
	for name, w := range map[string]KeyWrapper{"wrapped": kms, "plaintext": nil} {
		store, control := filepath.Join(t.TempDir(), "store"), filepath.Join(t.TempDir(), "control")
		if err := InitStoreCA(ctx, store, time.Hour, w); err != nil {
			t.Fatal(err)
		}
		if err := InitCAWrapped(ctx, control, time.Hour, w); err != nil {
			t.Fatal(err)
		}
		if _, err := LoadCAWrapped(ctx, store, w); err == nil {
			t.Errorf("%s: a store CA loaded as the control CA", name)
		}
		if _, err := LoadStoreCA(ctx, control, w); err == nil {
			t.Errorf("%s: the control CA loaded as a store CA", name)
		}
		sca, err := LoadStoreCA(ctx, store, w)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := sca.Issue(Principal{RoleOperator, "op"}, time.Hour, nil); err == nil {
			t.Errorf("%s: a store CA issued a control principal", name)
		}
		cca, err := LoadCAWrapped(ctx, control, w)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cca.IssueStore("filer", time.Hour, []string{"127.0.0.1"}); err == nil {
			t.Errorf("%s: the control CA issued a store certificate", name)
		}
	}
	// The wrapped key is bound to its kind's purpose: a store CA's wrapped
	// key does not unwrap as a control CA's, even beside a certificate
	// renamed to match.
	if wrapContext(storeKind, []byte("x"))["purpose"] != StoreCAPurpose || wrapContext(controlKind, []byte("x"))["purpose"] != "sufuree-control-ca" {
		t.Fatal("the KMS purposes changed; the key policies name them")
	}
}
