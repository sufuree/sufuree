package controlauth

import (
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCAInitIssueAndPrincipal(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "ca")
	if err := InitCA(dir, 24*time.Hour); err != nil {
		t.Fatal(err)
	}
	if err := InitCA(dir, 24*time.Hour); err == nil {
		t.Fatal("InitCA replaced an existing CA")
	}
	if st, _ := os.Stat(filepath.Join(dir, "ca.key")); st.Mode().Perm() != 0o600 {
		t.Fatalf("ca.key mode %v", st.Mode().Perm())
	}
	ca, err := LoadCA(dir)
	if err != nil {
		t.Fatal(err)
	}
	is, err := ca.Issue(Principal{RoleHost, "host-a"}, time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	blk, _ := pem.Decode(is.CertPEM)
	cert, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca.Cert)
	if _, err := cert.Verify(x509.VerifyOptions{Roots: pool, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		t.Fatalf("issued cert does not verify: %v", err)
	}
	p, err := FromCert(cert)
	if err != nil || p != (Principal{RoleHost, "host-a"}) {
		t.Fatalf("principal = %+v, %v", p, err)
	}
	if _, err := ca.Issue(Principal{RoleHost, "host-a"}, time.Hour, []string{"example.com"}); err == nil {
		t.Fatal("a host certificate got server names")
	}
	if _, err := ca.Issue(Principal{"root", "x"}, time.Hour, nil); err == nil {
		t.Fatal("unknown role issued")
	}
	if err := os.Chmod(filepath.Join(dir, "ca.key"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadCA(dir); err == nil {
		t.Fatal("a world-readable CA key was loaded")
	}
}

func TestAllowed(t *testing.T) {
	host := Principal{RoleHost, "host-a"}
	op := Principal{RoleOperator, "alice"}
	pc := Principal{RoleProductClient, "zati"}
	cases := []struct {
		p            Principal
		method, path string
		want         bool
	}{
		{host, "POST", "/v1/hosts/host-a/boot/inst-1", true},
		{host, "POST", "/v1/hosts/host-b/boot/inst-1", false},
		{host, "POST", "/v1/hosts/host-a/progress", true},
		{host, "POST", "/v1/hosts/host-a/credentials/inst-1", true},
		{host, "GET", "/v1/hosts/host-a/assignments", true},
		{host, "GET", "/v1/hosts/host-b/assignments", false},
		{host, "GET", "/v1/hosts", false},
		{host, "PUT", "/v1/resources/App/web", false},
		{host, "GET", "/v1/installations/inst-1/floor", false},
		{host, "POST", "/v1/hosts/host-b/../host-a/boot/inst-1", false},
		{host, "POST", "/v1/hosts/host-a//boot/inst-1", false},
		{op, "GET", "/v1/hosts/host-a/assignments", true},
		{op, "POST", "/v1/hosts/host-a/boot/inst-1", false},
		{op, "PUT", "/v1/resources/App/web", true},
		{op, "GET", "/v1/operations", true},
		{op, "POST", "/v1/installations/inst-1/floor/lower-approve", true},
		{op, "POST", "/v1/installations/inst-1/floor/lower-request", false},
		{op, "POST", "/v1/installations/inst-1/floor/lower", false},
		{pc, "POST", "/v1/installations/inst-1/floor/lower-request", true},
		{pc, "POST", "/v1/installations/inst-1/floor/lower-approve", false},
		{pc, "PUT", "/v1/resources/App/web", true},
		{pc, "GET", "/v1/operations", false},
		{pc, "DELETE", "/v1/resources/App/web", false},
		{op, "GET", "/healthz", false},
		{op, "GET", "/v1/unknown", false},
	}
	for _, c := range cases {
		if got := Allowed(c.p, c.method, c.path); got != c.want {
			t.Errorf("%s %s %s = %v, want %v", c.p, c.method, c.path, got, c.want)
		}
	}
}

func TestClientFlagsRefuseMismatch(t *testing.T) {
	if _, err := (TLSFlags{Cert: "a"}).Client("http://127.0.0.1:1"); err == nil {
		t.Fatal("TLS flags accepted for an http URL")
	}
	if _, err := (TLSFlags{}).Client("https://127.0.0.1:1"); err == nil {
		t.Fatal("an https URL accepted without TLS flags")
	}
	if c, err := (TLSFlags{}).Client("http://127.0.0.1:1"); err != nil || c != http.DefaultClient {
		t.Fatalf("plain http: %v", err)
	}
}
