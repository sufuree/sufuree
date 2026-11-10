package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/sufuree/sufuree/internal/bootfloor"
	"github.com/sufuree/sufuree/internal/controlauth"
	"github.com/sufuree/sufuree/internal/controlraft"
	"github.com/sufuree/sufuree/internal/reconcile"
	"github.com/sufuree/sufuree/internal/resource"
	"github.com/sufuree/sufuree/internal/store"
)

func seedWorld(t *testing.T, st *store.Store) {
	t.Helper()
	t0 := time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)
	err := st.Update(func(tx *store.Tx) error {
		if err := tx.PutMeta(resource.ClusterMeta{DeploymentUID: "dep-1", RecoveryIncarnation: 1, OwnerEpoch: 1}); err != nil {
			return err
		}
		if _, err := tx.ApplyResource(resource.Resource{
			UID: "inst-1", Name: "clinic", Generation: 1, Kind: resource.KindApp, Lifecycle: resource.LifecycleEnabled,
			Spec: resource.AppSpec{Image: "/img/zati", MemoryMiB: 256, GuestPort: 8080,
				IdleTimeout: resource.Duration{Duration: time.Minute}, StartupTimeout: resource.Duration{Duration: 20 * time.Second}, MaxReplicas: 1},
		}); err != nil {
			return err
		}
		if _, err := tx.ApplyResource(resource.Resource{
			UID: "pool-1", Name: "edge", Generation: 1, Kind: resource.KindPool, Lifecycle: resource.LifecycleEnabled,
			Spec: resource.PoolSpec{MaxHosts: 2, QuietPeriod: resource.Duration{Duration: time.Minute}, WarmMode: resource.WarmObserve},
		}); err != nil {
			return err
		}
		for _, h := range []string{"host-a", "host-b"} {
			if err := tx.PutHost(resource.Host{UID: h, PoolUID: "pool-1", ProviderID: "i-" + h, MemoryMiB: 4096, Ready: true, ObservedAt: t0}); err != nil {
				return err
			}
		}
		return tx.PutPlacement(resource.Placement{UID: "p1", ResourceUID: "inst-1", HostUID: "host-a", Replica: "inst-1",
			Generation: 1, OwnerEpoch: 1, MemoryMiB: 256, Phase: resource.PhaseReady, UpdatedAt: t0})
	})
	if err != nil {
		t.Fatal(err)
	}
}

type authFixture struct {
	url string
	dir string
	st  *store.Store
}

func (f authFixture) client(t *testing.T, role, name string) *http.Client {
	t.Helper()
	prefix := filepath.Join(f.dir, role+"-"+name)
	if err := run([]string{"ca", "issue", "--dir", filepath.Join(f.dir, "ca"), "--role", role, "--name", name, "--out", prefix}); err != nil {
		t.Fatal(err)
	}
	c, err := controlauth.TLSFlags{Cert: prefix + ".crt", Key: prefix + ".key", CA: filepath.Join(f.dir, "ca", "ca.crt")}.Client(f.url)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func newAuthFixture(t *testing.T) authFixture {
	t.Helper()
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca")
	if err := run([]string{"ca", "init", "--dir", ca}); err != nil {
		t.Fatal(err)
	}
	srvPrefix := filepath.Join(dir, "controller")
	if err := run([]string{"ca", "issue", "--dir", ca, "--role", "controller", "--name", "control", "--server-name", "127.0.0.1", "--out", srvPrefix}); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(filepath.Join(dir, "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	seedWorld(t, st)
	j := &controlraft.Journal{Store: st}
	c := reconcile.New(j, nil, nil, reconcile.Options{})
	srv := httptest.NewUnstartedServer(controlHandlerAuth(nil, c, j, true))
	if srv.TLS, err = controlauth.ServerTLS(srvPrefix+".crt", srvPrefix+".key", filepath.Join(ca, "ca.crt")); err != nil {
		t.Fatal(err)
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return authFixture{url: srv.URL, dir: dir, st: st}
}

func call(t *testing.T, c *http.Client, method, url string, body any) (int, string) {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, url, r)
	res, err := c.Do(req)
	if err != nil {
		return 0, err.Error()
	}
	defer res.Body.Close()
	out, _ := io.ReadAll(res.Body)
	return res.StatusCode, string(out)
}

func TestControlAPIRequiresAnAuthorizedCertificate(t *testing.T) {
	f := newAuthFixture(t)
	hostA := f.client(t, "host", "host-a")
	hostB := f.client(t, "host", "host-b")
	op := f.client(t, "operator", "alice")

	// No client certificate: the TLS handshake itself fails.
	plain, _ := controlauth.TLSFlags{}.Client("http://x")
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = hostA.Transport.(*http.Transport).TLSClientConfig.Clone()
	tr.TLSClientConfig.Certificates = nil
	plain = &http.Client{Transport: tr}
	if code, _ := call(t, plain, "GET", f.url+"/v1/resources", nil); code != 0 {
		t.Fatalf("a caller without a certificate got status %d", code)
	}
	// A certificate from another CA is refused too.
	other := t.TempDir()
	if err := run([]string{"ca", "init", "--dir", other}); err != nil {
		t.Fatal(err)
	}
	if err := run([]string{"ca", "issue", "--dir", other, "--role", "operator", "--name", "mallory", "--out", filepath.Join(other, "m")}); err != nil {
		t.Fatal(err)
	}
	forged, err := controlauth.TLSFlags{Cert: filepath.Join(other, "m.crt"), Key: filepath.Join(other, "m.key"), CA: filepath.Join(f.dir, "ca", "ca.crt")}.Client(f.url)
	if err != nil {
		t.Fatal(err)
	}
	if code, _ := call(t, forged, "GET", f.url+"/v1/resources", nil); code != 0 {
		t.Fatalf("a foreign-CA certificate got status %d", code)
	}

	ctx := context.Background()
	a := &bootfloor.Client{BaseURL: f.url, HostUID: "host-a", HTTP: hostA}
	if g, err := a.Boot(ctx, "inst-1", bootfloor.Entry{}); err != nil || g.Generation != 1 {
		t.Fatalf("host-a boot = %+v, %v", g, err)
	}
	// host-b cannot speak as host-a, nor read its assignments.
	spoof := &bootfloor.Client{BaseURL: f.url, HostUID: "host-a", HTTP: hostB}
	if _, err := spoof.Boot(ctx, "inst-1", bootfloor.Entry{}); err == nil {
		t.Fatal("host-b got a grant as host-a")
	}
	if code, _ := call(t, hostB, "GET", f.url+"/v1/hosts/host-a/assignments", nil); code != http.StatusForbidden {
		t.Fatalf("host-b read host-a's assignments: %d", code)
	}
	if code, _ := call(t, hostA, "GET", f.url+"/v1/resources", nil); code != http.StatusForbidden {
		t.Fatalf("a host listed resources: %d", code)
	}
	if code, body := call(t, op, "GET", f.url+"/v1/resources", nil); code != http.StatusOK {
		t.Fatalf("operator list: %d %s", code, body)
	}
}

func TestFloorLoweringNeedsTwoPrincipals(t *testing.T) {
	f := newAuthFixture(t)
	hostA := f.client(t, "host", "host-a")
	op := f.client(t, "operator", "alice")
	zati := f.client(t, "product-client", "zati")
	a := &bootfloor.Client{BaseURL: f.url, HostUID: "host-a", HTTP: hostA}
	if _, err := a.Boot(context.Background(), "inst-1", bootfloor.Entry{Generation: 9, Epoch: 8, Seq: 5}); err != nil {
		t.Fatal(err)
	}
	base := f.url + "/v1/installations/inst-1/floor"
	if code, _ := call(t, op, "POST", base+"/lower", bootfloor.Lowering{}); code != http.StatusForbidden {
		t.Fatalf("the unauthenticated single-call lowering is still served: %d", code)
	}
	req := bootfloor.LowerRequestBody{Owner: "owner-42", ExpectIncarnation: 1, Epoch: 7, Seq: 1, Reason: "restore", Reference: "INC-9"}
	if code, _ := call(t, op, "POST", base+"/lower-request", req); code != http.StatusForbidden {
		t.Fatalf("an operator requested a lowering: %d", code)
	}
	if code, body := call(t, zati, "POST", base+"/lower-request", req); code != http.StatusOK {
		t.Fatalf("product-client request: %d %s", code, body)
	}
	appr := bootfloor.ApproveBody{ExpectIncarnation: 1, Epoch: 7, Seq: 1, Reference: "INC-9"}
	if code, _ := call(t, zati, "POST", base+"/lower-approve", appr); code != http.StatusForbidden {
		t.Fatalf("the requester approved its own lowering: %d", code)
	}
	wrong := appr
	wrong.Seq = 0
	if code, _ := call(t, op, "POST", base+"/lower-approve", wrong); code != http.StatusConflict {
		t.Fatalf("an approval naming a different mark: %d", code)
	}
	code, body := call(t, op, "POST", base+"/lower-approve", appr)
	if code != http.StatusOK {
		t.Fatalf("approval: %d %s", code, body)
	}
	var rec resource.BootFloor
	if err := json.Unmarshal([]byte(body), &rec); err != nil {
		t.Fatal(err)
	}
	if rec.Incarnation != 2 || rec.ProgressEpoch != 7 || rec.ProgressSeq != 1 || rec.Pending != nil || rec.Lowered == nil ||
		rec.Lowered.RequestedBy != "product-client:zati for owner owner-42" || rec.Lowered.ApprovedBy != "operator:alice" {
		t.Fatalf("lowered record = %+v (audit %+v)", rec, rec.Lowered)
	}
	if code, _ := call(t, op, "POST", base+"/lower-approve", appr); code != http.StatusConflict {
		t.Fatalf("an approval was replayed: %d", code)
	}
}
