package reconcile

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/sufuree/sufuree/internal/demand"
	"github.com/sufuree/sufuree/internal/executor"
	"github.com/sufuree/sufuree/internal/replicacred"
	"github.com/sufuree/sufuree/internal/resource"
	"github.com/sufuree/sufuree/internal/store"
)

type fakeRevoker struct {
	mu      sync.Mutex
	revoked []string
	err     error
}

func (f *fakeRevoker) Revoke(_ context.Context, installation string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.revoked = append(f.revoked, installation)
	return nil
}

func (f *fakeRevoker) calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.revoked...)
}

func doDelete(t *testing.T, url string, ifMatch string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if ifMatch != "" {
		req.Header.Set("If-Match", ifMatch)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = res.Body.Close()
	return res
}

func TestDeleteAppReleasesItRemovesItAndRevokesItsIdentity(t *testing.T) {
	t.Parallel()
	c, st := newController(t, &executor.Memory{}, time.Now, 0)
	rev := &fakeRevoker{err: errors.New("filer down")}
	c.revoker = rev
	seedPlacement(t, c, st, "h1", "app-web", resource.PhaseReady)
	srv := httptest.NewServer(Handler(c))
	t.Cleanup(srv.Close)
	ctx := context.Background()

	app, err := c.GetResource(ctx, resource.KindApp, "web")
	if err != nil {
		t.Fatal(err)
	}
	if res := doDelete(t, srv.URL+"/v1/resources/Pool/main", ""); res.StatusCode != http.StatusBadRequest {
		t.Fatalf("DELETE pool = %d", res.StatusCode)
	}
	stale := strconv.FormatUint(app.ResourceVersion+7, 10)
	if res := doDelete(t, srv.URL+"/v1/resources/App/web", stale); res.StatusCode != http.StatusConflict {
		t.Fatalf("DELETE with a stale If-Match = %d", res.StatusCode)
	}
	for i := 0; i < 2; i++ { // a repeat is accepted and changes nothing
		if res := doDelete(t, srv.URL+"/v1/resources/App/web", ""); res.StatusCode != http.StatusAccepted {
			t.Fatalf("DELETE %d = %d", i, res.StatusCode)
		}
	}
	got, err := c.GetResource(ctx, resource.KindApp, "web")
	if err != nil || got.Lifecycle != resource.LifecycleDeleting || got.Generation != app.Generation {
		t.Fatalf("after DELETE: %+v, %v", got, err)
	}

	// The revoker fails: the app is gone, its revocation stays pending.
	for i := 0; i < 5; i++ {
		if err := c.ReconcileOnce(ctx); err != nil {
			t.Fatal(err)
		}
		c.revokeOnce(ctx)
	}
	if _, err := c.GetResource(ctx, resource.KindApp, "web"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("the deleted app is still there: %v", err)
	}
	if res, err := http.Get(srv.URL + "/v1/resources/App/web"); err != nil || res.StatusCode != http.StatusNotFound {
		t.Fatalf("GET after delete = %v, %v", res.StatusCode, err)
	}
	r := revocation(t, st, app.UID)
	if !r.Pending() || r.ResourceUID != app.UID || len(queued(t, st)) != 1 {
		t.Fatalf("revocation = %+v, queue %v", r, queued(t, st))
	}

	// The revoker recovers: the next pass removes the identity, once.
	rev.mu.Lock()
	rev.err = nil
	rev.mu.Unlock()
	for i := 0; i < 2; i++ {
		if err := c.ReconcileOnce(ctx); err != nil {
			t.Fatal(err)
		}
		c.revokeOnce(ctx)
	}
	if got := rev.calls(); len(got) != 1 || got[0] != app.UID {
		t.Fatalf("revoked %v", got)
	}
	if r := revocation(t, st, app.UID); r.Pending() || len(queued(t, st)) != 0 {
		t.Fatal("the revocation was not recorded done")
	}

	// The name can't come back: the new app would get the old
	// installation's credentials and replica data.
	if _, err := c.ApplyResource(ctx, resource.Resource{
		Name: "web", Kind: resource.KindApp, Lifecycle: resource.LifecycleEnabled, Spec: app.Spec,
	}); !errors.Is(err, store.ErrRevoked) {
		t.Fatalf("re-creating a deleted app: %v", err)
	}
}

func TestDeleteWaitsForExpiredClaimsOnly(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	c, st := newController(t, &executor.Memory{}, func() time.Time { return now }, 0)
	c.revoker = &fakeRevoker{}
	app := applyApp(t, c)
	claim := func(uid string, deadline time.Time) {
		t.Helper()
		if err := st.Update(func(tx *store.Tx) error {
			return tx.PutClaim(resource.ActivationClaim{UID: uid, ResourceUID: app.UID, Generation: app.Generation,
				Count: 1, CreatedAt: now.Add(-time.Minute), Deadline: deadline})
		}); err != nil {
			t.Fatal(err)
		}
	}
	claim("c-old", now.Add(-time.Second))
	claim("c-live", now.Add(time.Minute))
	ctx := context.Background()
	if _, err := c.DeleteResource(ctx, resource.KindApp, "web", 0); err != nil {
		t.Fatal(err)
	}
	if err := c.finishDeletes(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetResource(ctx, resource.KindApp, "web"); err != nil {
		t.Fatalf("removed while a claim was live: %v", err)
	}
	now = now.Add(2 * time.Minute)
	if err := c.finishDeletes(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetResource(ctx, resource.KindApp, "web"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("not removed once its claims expired: %v", err)
	}
}

func revocation(t *testing.T, st *store.Store, installation string) resource.Revocation {
	t.Helper()
	var r resource.Revocation
	if err := st.View(func(tx *store.Tx) error {
		var err error
		r, err = tx.GetRevocation(installation)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return r
}

func queued(t *testing.T, st *store.Store) []string {
	t.Helper()
	var out []string
	if err := st.View(func(tx *store.Tx) error {
		got, _, err := tx.PendingRevocations("", 1000)
		for _, r := range got {
			out = append(out, r.Installation)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

// failingRevoker fails for the installations in fail.
type failingRevoker struct {
	fakeRevoker
	fail map[string]bool
}

func (f *failingRevoker) Revoke(ctx context.Context, installation string) error {
	if f.fail[installation] {
		return errors.New("filer refused")
	}
	return f.fakeRevoker.Revoke(ctx, installation)
}

func TestRevocationsThatKeepFailingDontStarveTheRest(t *testing.T) {
	t.Parallel()
	c, st := newController(t, &executor.Memory{}, time.Now, 0)
	fail := map[string]bool{}
	app := applyApp(t, c)
	// revokeBatch one-off replicas that always fail sort first; one that
	// works sorts after them.
	var ids []string
	for i := 0; i < revokeBatch; i++ {
		id := "r0" + strconv.Itoa(10+i)
		fail[id] = true
		ids = append(ids, id)
	}
	ids = append(ids, "r999")
	pool := applyPool(t, c)
	now := time.Now().UTC()
	if err := st.Update(func(tx *store.Tx) error {
		if err := tx.PutHost(resource.Host{UID: "h1", PoolUID: pool.UID, ProviderID: "i-h1", MemoryMiB: 4096, Ready: true, ObservedAt: now}); err != nil {
			return err
		}
		for _, id := range ids {
			if err := tx.PutPlacement(resource.Placement{UID: "p-" + id, ResourceUID: app.UID, HostUID: "h1", Replica: id,
				Generation: 1, OwnerEpoch: 1, MemoryMiB: 128, Phase: resource.PhaseReady, UpdatedAt: now}); err != nil {
				return err
			}
		}
		for _, id := range ids {
			if err := tx.DeletePlacement("p-" + id); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got := queued(t, st); len(got) != len(ids) {
		t.Fatalf("queued %v", got)
	}
	rev := &failingRevoker{fail: fail}
	c.revoker = rev
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		c.revokeOnce(ctx)
	}
	if got := rev.calls(); len(got) != 1 || got[0] != "r999" {
		t.Fatalf("revoked %v: the working revocation was starved", got)
	}
	// A one-off replica ID leaves no record once revoked; the failing
	// ones stay queued.
	if got := queued(t, st); len(got) != revokeBatch {
		t.Fatalf("queued after revoking: %v", got)
	}
	if err := st.View(func(tx *store.Tx) error {
		all, err := tx.ListRevocations()
		if len(all) != 0 {
			t.Errorf("one-off replica IDs left tombstones: %+v", all)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestRevokedInstallationGetsGoneFromTheCredentialsRoute(t *testing.T) {
	t.Parallel()
	c, st := newController(t, &executor.Memory{}, time.Now, 0)
	seedPlacement(t, c, st, "h1", "inst-1", resource.PhaseReady)
	srv := httptest.NewServer(HandlerWith(c, &fakeReplica{err: replicacred.ErrRevoked}))
	t.Cleanup(srv.Close)
	res, _ := postCreds(t, srv.URL+"/v1/hosts/h1/credentials/inst-1")
	if res.StatusCode != http.StatusGone {
		t.Fatalf("status %d, want 410", res.StatusCode)
	}
}

func TestEdgeRefusesADeletingAppWithoutAClaim(t *testing.T) {
	t.Parallel()
	c, st := newController(t, &executor.Memory{}, time.Now, 0)
	app := applyApp(t, c)
	ctx := context.Background()
	if _, err := c.DeleteResource(ctx, resource.KindApp, app.Name, 0); err != nil {
		t.Fatal(err)
	}
	act := &EdgeActivator{Controller: c, Demand: demand.New(demand.Bolt{Store: st}), Wait: 5 * time.Second}
	req := httptest.NewRequest(http.MethodGet, "http://edge/", nil)
	start := time.Now()
	if _, _, err := act.Acquire(req, app.Name); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("Acquire on a deleting app: %v", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("Acquire waited %s", d)
	}
	if err := st.View(func(tx *store.Tx) error {
		claims, err := tx.ListClaims()
		if len(claims) != 0 {
			t.Errorf("a claim was made: %+v", claims)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}
