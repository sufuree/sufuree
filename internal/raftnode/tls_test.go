package raftnode

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sufuree/sufuree/internal/controlauth"
)

// issueTLS issues a principal certificate and returns the transport TLS config
// plus the parsed leaf, for role-binding assertions.
func issueTLS(t *testing.T, ca *controlauth.CA, caFile, dir string, p controlauth.Principal) (*tls.Config, *x509.Certificate) {
	t.Helper()
	is, err := ca.Issue(p, time.Hour, nil)
	if err != nil {
		t.Fatal(err)
	}
	prefix := filepath.Join(dir, p.Role+"-"+p.Name)
	if err := controlauth.WriteIssued(prefix, is); err != nil {
		t.Fatal(err)
	}
	cfg, err := controlauth.RaftTransportTLS(prefix+".crt", prefix+".key", caFile)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(prefix + ".crt")
	if err != nil {
		t.Fatal(err)
	}
	block, _ := pem.Decode(raw)
	if block == nil {
		t.Fatal("certificate is not PEM")
	}
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return cfg, leaf
}

func TestTLSVoterApplies(t *testing.T) {
	dir := t.TempDir()
	caDir := filepath.Join(dir, "ca")
	if err := controlauth.InitCA(caDir, time.Hour); err != nil {
		t.Fatal(err)
	}
	ca, err := controlauth.LoadCA(caDir)
	if err != nil {
		t.Fatal(err)
	}
	cfg, _ := issueTLS(t, ca, filepath.Join(caDir, "ca.crt"), dir, controlauth.Principal{Role: controlauth.RoleController, Name: "c1"})

	rec := &record{}
	n, err := Open(Config{
		DataDir: filepath.Join(dir, "raft"), NodeID: "controller-1",
		BindAddr: freeAddr(t), Bootstrap: true, TLS: cfg,
	}, rec.fsm())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	waitLeader(t, n)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := n.Apply(ctx, []byte(`{"op":"put","payload":{"k":"v"}}`), 10*time.Second); err != nil {
		t.Fatal(err)
	}
	waitRecord(t, rec, "put {\"k\":\"v\"}")
}

func TestRaftTransportTLSRoleBinding(t *testing.T) {
	dir := t.TempDir()
	caDir := filepath.Join(dir, "ca")
	if err := controlauth.InitCA(caDir, time.Hour); err != nil {
		t.Fatal(err)
	}
	ca, err := controlauth.LoadCA(caDir)
	if err != nil {
		t.Fatal(err)
	}
	caFile := filepath.Join(caDir, "ca.crt")
	ctrlCfg, ctrlLeaf := issueTLS(t, ca, caFile, dir, controlauth.Principal{Role: controlauth.RoleController, Name: "c1"})
	opCfg, opLeaf := issueTLS(t, ca, caFile, dir, controlauth.Principal{Role: controlauth.RoleOperator, Name: "op"})

	if err := ctrlCfg.VerifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{ctrlLeaf}}); err != nil {
		t.Fatalf("controller peer rejected: %v", err)
	}
	err = opCfg.VerifyConnection(tls.ConnectionState{PeerCertificates: []*x509.Certificate{opLeaf}})
	if err == nil || !strings.Contains(err.Error(), "not a controller") {
		t.Fatalf("operator peer accepted: %v", err)
	}
	if err := ctrlCfg.VerifyConnection(tls.ConnectionState{}); err == nil {
		t.Fatal("a peer with no certificate was accepted")
	}
}
