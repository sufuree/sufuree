package reconcile

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/sufuree/sufuree/internal/executor"
	"github.com/sufuree/sufuree/internal/replicacred"
	"github.com/sufuree/sufuree/internal/resource"
	"github.com/sufuree/sufuree/internal/store"
)

type fakeReplica struct {
	mu     sync.Mutex
	issued []string
	err    error
}

func (f *fakeReplica) Issue(_ context.Context, installation string) (replicacred.Credentials, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.issued = append(f.issued, installation)
	if f.err != nil {
		return replicacred.Credentials{}, f.err
	}
	return replicacred.Credentials{AccessKeyID: "AK-" + installation, SecretAccessKey: "SK-" + installation}, nil
}

func seedPlacement(t *testing.T, c *Controller, st *store.Store, host, replica string, phase resource.Phase) {
	t.Helper()
	pool := applyPool(t, c)
	app := applyApp(t, c)
	now := time.Now().UTC()
	err := st.Update(func(tx *store.Tx) error {
		if _, err := tx.GetHost(host); errors.Is(err, store.ErrNotFound) {
			if err := tx.PutHost(resource.Host{UID: host, PoolUID: pool.UID, ProviderID: "i-" + host, MemoryMiB: 4096, Ready: true, ObservedAt: now}); err != nil {
				return err
			}
		}
		return tx.PutPlacement(resource.Placement{UID: "p-" + replica, ResourceUID: app.UID, HostUID: host, Replica: replica,
			Generation: 1, OwnerEpoch: 1, MemoryMiB: 128, Phase: phase, UpdatedAt: now})
	})
	if err != nil {
		t.Fatal(err)
	}
}

func postCreds(t *testing.T, url string) (*http.Response, credentialResponse) {
	t.Helper()
	res, err := http.Post(url, "application/json", bytes.NewBufferString(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var out credentialResponse
	if res.StatusCode == http.StatusOK {
		if err := json.NewDecoder(res.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
	}
	return res, out
}

func TestCredentialsRouteIssuesReplicaCredentialsOnlyForHeldInstallations(t *testing.T) {
	t.Parallel()
	c, st := newController(t, &executor.Memory{}, time.Now, 0)
	seedPlacement(t, c, st, "h1", "inst-1", resource.PhaseReady)
	rep := &fakeReplica{}
	srv := httptest.NewServer(HandlerWith(c, rep))
	t.Cleanup(srv.Close)

	res, out := postCreds(t, srv.URL+"/v1/hosts/h1/credentials/inst-1")
	if res.StatusCode != http.StatusOK || out.Replica == nil || out.Replica.AccessKeyID != "AK-inst-1" {
		t.Fatalf("held: %s %+v", res.Status, out)
	}
	if res.Header.Get("Cache-Control") != "no-store" {
		t.Fatal("credentials response is cacheable")
	}
	for _, path := range []string{"/v1/hosts/h2/credentials/inst-1", "/v1/hosts/h1/credentials/inst-2"} {
		if res, _ := postCreds(t, srv.URL+path); res.StatusCode != http.StatusForbidden {
			t.Errorf("%s: %s, want 403", path, res.Status)
		}
	}
	if len(rep.issued) != 1 {
		t.Fatalf("credentials issued for %v; only a held installation may be issued", rep.issued)
	}

	rep.err = errors.New("filer down")
	if res, _ := postCreds(t, srv.URL+"/v1/hosts/h1/credentials/inst-1"); res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("issue failure: %s, want 503", res.Status)
	}
}

func TestCredentialsRouteWithoutReplicaIssuerOmitsReplica(t *testing.T) {
	t.Parallel()
	c, st := newController(t, &executor.Memory{}, time.Now, 0)
	seedPlacement(t, c, st, "h1", "inst-1", resource.PhaseReady)
	srv := httptest.NewServer(Handler(c))
	t.Cleanup(srv.Close)
	res, out := postCreds(t, srv.URL+"/v1/hosts/h1/credentials/inst-1")
	if res.StatusCode != http.StatusOK || out.Replica != nil {
		t.Fatalf("%s %+v", res.Status, out)
	}
}
