package bootfloor_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sufuree/sufuree/internal/bootfloor"
	"github.com/sufuree/sufuree/internal/controlraft"
	"github.com/sufuree/sufuree/internal/raftnode"
	"github.com/sufuree/sufuree/internal/resource"
	"github.com/sufuree/sufuree/internal/store"
)

var t0 = time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC)

// world seeds one installation ("inst-1", max one replica) placed on
// host-a, and a second host that holds nothing.
func world(t *testing.T, st *store.Store) {
	t.Helper()
	err := st.Update(func(tx *store.Tx) error {
		if err := tx.PutMeta(resource.ClusterMeta{DeploymentUID: "dep-1", RecoveryIncarnation: 1, OwnerEpoch: 1}); err != nil {
			return err
		}
		if _, err := tx.ApplyResource(resource.Resource{
			UID: "inst-1", Name: "clinic", Generation: 1, Kind: resource.KindApp, Lifecycle: resource.LifecycleEnabled,
			Spec: resource.AppSpec{Image: "/img/zati", MemoryMiB: 256, GuestPort: 8080,
				IdleTimeout: resource.Duration{Duration: time.Minute}, StartupTimeout: resource.Duration{Duration: 20 * time.Second},
				MaxReplicas: 1},
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

func openStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	world(t, st)
	return st
}

func serve(t *testing.T, j bootfloor.Committer) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	bootfloor.Register(mux, j, func() time.Time { return t0 })
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func post(t *testing.T, url string, body any) (int, string) {
	t.Helper()
	b, _ := json.Marshal(body)
	res, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(res.Body)
	return res.StatusCode, buf.String()
}

func floorOf(t *testing.T, st *store.Store) resource.BootFloor {
	t.Helper()
	var f resource.BootFloor
	if err := st.View(func(tx *store.Tx) error {
		var err error
		f, err = tx.GetBootFloor("inst-1")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return f
}

// exercise runs the whole lifecycle against one journal.
func exercise(t *testing.T, st *store.Store, j bootfloor.Committer) {
	srv := serve(t, j)
	ctx := context.Background()
	a := &bootfloor.Client{BaseURL: srv.URL, HostUID: "host-a"}
	b := &bootfloor.Client{BaseURL: srv.URL, HostUID: "host-b"}

	if _, err := b.Boot(ctx, "inst-1", bootfloor.Entry{}); err == nil || !strings.Contains(err.Error(), "403") {
		t.Fatalf("a host without the placement got a grant: %v", err)
	}
	// Enrolment: host-a has been standalone and already issued H=50 with
	// floor 40:7. The first grant starts above both.
	g, err := a.Boot(ctx, "inst-1", bootfloor.Entry{Generation: 50, Epoch: 40, Seq: 7})
	if err != nil {
		t.Fatal(err)
	}
	if g.Generation != 51 || g.Incarnation != 1 || g.ProgressEpoch != 40 || g.ProgressSeq != 7 {
		t.Fatalf("enrolment grant = %+v", g)
	}
	// Every boot is a fresh commit.
	if g, err = a.Boot(ctx, "inst-1", bootfloor.Entry{Incarnation: 1, Generation: 51, Epoch: 40, Seq: 7}); err != nil || g.Generation != 52 {
		t.Fatalf("second grant = %+v, %v", g, err)
	}
	// Reports raise the floor; host-b's report is not taken.
	res, err := a.Report(ctx, []bootfloor.Entry{{Installation: "inst-1", Incarnation: 1, Generation: 52, Epoch: 52, Seq: 3}})
	if err != nil || res.Accepted != 1 {
		t.Fatalf("report = %+v, %v", res, err)
	}
	if res, err = b.Report(ctx, []bootfloor.Entry{{Installation: "inst-1", Incarnation: 1, Generation: 99, Epoch: 99, Seq: 9}}); err != nil || res.Accepted != 0 || len(res.NotHeld) != 1 {
		t.Fatalf("report from a host without the placement = %+v, %v", res, err)
	}
	if f := floorOf(t, st); f.Generation != 52 || f.ProgressEpoch != 52 || f.ProgressSeq != 3 {
		t.Fatalf("after reports = %+v", f)
	}

	// An intentional rollback needs two different people.
	lower := bootfloor.Lowering{ExpectIncarnation: 1, Epoch: 51, Seq: 1, RequestedBy: "owner@clinic.example",
		ApprovedBy: "owner@clinic.example", Reason: "restore", Reference: "INC-7"}
	if code, _ := post(t, srv.URL+"/v1/installations/inst-1/floor/lower", lower); code != http.StatusBadRequest {
		t.Fatalf("self-approved lowering: status %d", code)
	}
	lower.ApprovedBy = "operator@zati.run"
	if code, body := post(t, srv.URL+"/v1/installations/inst-1/floor/lower", lower); code != http.StatusOK {
		t.Fatalf("lowering: %d %s", code, body)
	}
	f := floorOf(t, st)
	if f.Incarnation != 2 || !f.StartOpen || f.ProgressEpoch != 51 || f.ProgressSeq != 1 || f.Lowered == nil || !f.Lowered.At.Equal(t0) {
		t.Fatalf("after lowering = %+v", f)
	}
	// A host that missed the rollback cannot raise the floor back.
	if _, err := a.Report(ctx, []bootfloor.Entry{{Installation: "inst-1", Incarnation: 1, Generation: 52, Epoch: 52, Seq: 9}}); err != nil {
		t.Fatal(err)
	}
	if f := floorOf(t, st); f.ProgressEpoch != 51 || f.ProgressSeq != 1 {
		t.Fatalf("an old-incarnation report moved the floor: %+v", f)
	}
	// Its next boot gets the new incarnation and the lowered floor.
	// Its own record is past what the controller saw (55): the new
	// incarnation starts above it, so that guest's hints are discarded.
	g, err = a.Boot(ctx, "inst-1", bootfloor.Entry{Incarnation: 1, Generation: 55, Epoch: 54, Seq: 9})
	if err != nil || g.Generation != 56 || g.Incarnation != 2 || g.IncarnationFrom != 56 || g.ProgressEpoch != 51 || g.ProgressSeq != 1 {
		t.Fatalf("grant after lowering = %+v, %v", g, err)
	}
	// A host with a newer incarnation (the controller restored behind a
	// rollback) is refused, and its claim never lowers the floor, by boot
	// or by report.
	ahead := bootfloor.Entry{Incarnation: 3, IncarnationFrom: 60, Generation: 70, Epoch: 0, Seq: 0}
	if _, err := a.Boot(ctx, "inst-1", ahead); !errors.Is(err, bootfloor.ErrBehindHost) {
		t.Fatalf("boot from a host ahead of the controller: %v, want ErrBehindHost", err)
	}
	ahead.Installation = "inst-1"
	if res, err := a.Report(ctx, []bootfloor.Entry{ahead}); err != nil || len(res.Ahead) != 1 || res.Accepted != 0 {
		t.Fatalf("report from a host ahead = %+v, %v", res, err)
	}
	if f := floorOf(t, st); f.Incarnation != 2 || f.ProgressEpoch != 51 || f.ProgressSeq != 1 || f.Generation != 70 {
		t.Fatalf("a host claim moved the floor (only H may rise): %+v", f)
	}
	// An operator re-applies the missed lowering through the audited route.
	relower := bootfloor.Lowering{ExpectIncarnation: 2, TargetIncarnation: 3, Epoch: 51, Seq: 1,
		RequestedBy: "owner@clinic.example", ApprovedBy: "operator@zati.run", Reason: "re-apply after controller restore", Reference: "INC-8"}
	if code, body := post(t, srv.URL+"/v1/installations/inst-1/floor/lower", relower); code != http.StatusOK {
		t.Fatalf("re-lowering: %d %s", code, body)
	}
	g, err = a.Boot(ctx, "inst-1", ahead)
	if err != nil || g.Incarnation != 3 || g.Generation != 71 || g.ProgressEpoch != 51 {
		t.Fatalf("grant after re-lowering = %+v, %v", g, err)
	}
}

func TestLifecycleLocalJournal(t *testing.T) {
	st := openStore(t)
	exercise(t, st, &controlraft.Journal{Store: st})
}

func TestLifecycleThroughRaft(t *testing.T) {
	st := openStore(t)
	node := raftNode(t, st)
	exercise(t, st, &controlraft.Journal{Store: st, Node: node})
}

func raftNode(t *testing.T, st *store.Store) *raftnode.Node {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	node, err := raftnode.Open(raftnode.Config{DataDir: filepath.Join(t.TempDir(), "raft"), NodeID: "n1", BindAddr: addr, Bootstrap: true}, controlraft.StoreFSM(st))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close() })
	deadline := time.Now().Add(10 * time.Second)
	for !node.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("not leader")
		}
		time.Sleep(20 * time.Millisecond)
	}
	return node
}

// TestRestoreStoreKeepsFloors checks that a placement change committed as
// restore_store neither carries nor replaces boot floors.
func TestRestoreStoreKeepsFloors(t *testing.T) {
	st := openStore(t)
	j := &controlraft.Journal{Store: st, Node: raftNode(t, st)}
	srv := serve(t, j)
	a := &bootfloor.Client{BaseURL: srv.URL, HostUID: "host-a"}
	if _, err := a.Boot(context.Background(), "inst-1", bootfloor.Entry{Generation: 9, Epoch: 8, Seq: 1}); err != nil {
		t.Fatal(err)
	}
	var archive bytes.Buffer
	if err := st.Export(&archive); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(archive.String(), "bootfloors") {
		t.Fatal("the restore_store archive carries boot floors")
	}
	if err := j.Update(func(tx *store.Tx) error {
		p, err := tx.GetPlacement("p1")
		if err != nil {
			return err
		}
		p.Phase, p.UpdatedAt = resource.PhaseDraining, t0.Add(time.Second)
		return tx.PutPlacement(p)
	}); err != nil {
		t.Fatal(err)
	}
	if f := floorOf(t, st); f.Generation != 10 || f.ProgressEpoch != 8 {
		t.Fatalf("restore_store changed the floor: %+v", f)
	}
}

// TestRaftSnapshotCarriesFloors restores a snapshot into a fresh store.
func TestRaftSnapshotCarriesFloors(t *testing.T) {
	st := openStore(t)
	j := &controlraft.Journal{Store: st}
	srv := serve(t, j)
	a := &bootfloor.Client{BaseURL: srv.URL, HostUID: "host-a"}
	if _, err := a.Boot(context.Background(), "inst-1", bootfloor.Entry{Generation: 9, Epoch: 8, Seq: 1}); err != nil {
		t.Fatal(err)
	}
	fsm := controlraft.StoreFSM(st)
	snap, err := fsm.SnapshotFn()
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := store.Open(filepath.Join(t.TempDir(), "restored.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if err := controlraft.StoreFSM(fresh).RestoreFn(snap); err != nil {
		t.Fatal(err)
	}
	if f := floorOf(t, fresh); f.Generation != 10 || f.ProgressEpoch != 8 || f.ProgressSeq != 1 {
		t.Fatalf("restored floor = %+v", f)
	}
	// A snapshot from before boot floors (a bare archive, say from an old
	// leader mid-upgrade) still restores, and keeps the local floors.
	var archive bytes.Buffer
	if err := st.Export(&archive); err != nil {
		t.Fatal(err)
	}
	if err := controlraft.StoreFSM(fresh).RestoreFn(archive.Bytes()); err != nil {
		t.Fatalf("bare archive snapshot: %v", err)
	}
	if f := floorOf(t, fresh); f.Generation != 10 || f.ProgressEpoch != 8 {
		t.Fatalf("a bare archive snapshot wiped the floors: %+v", f)
	}
}

// TestHostClaimCannotCreateALaterIncarnation: with no controller record, a
// host claiming incarnation 5 and floor 0 is refused and seeds only H, so
// an honest host's next grant keeps incarnation 1 and its own floor.
func TestHostClaimCannotCreateALaterIncarnation(t *testing.T) {
	st := openStore(t)
	srv := serve(t, &controlraft.Journal{Store: st})
	ctx := context.Background()
	a := &bootfloor.Client{BaseURL: srv.URL, HostUID: "host-a"}
	claim := bootfloor.Entry{Installation: "inst-1", Incarnation: 5, IncarnationFrom: 9, Generation: 20}
	if _, err := a.Boot(ctx, "inst-1", claim); !errors.Is(err, bootfloor.ErrBehindHost) {
		t.Fatalf("boot with a claimed incarnation: %v, want ErrBehindHost", err)
	}
	if res, err := a.Report(ctx, []bootfloor.Entry{claim}); err != nil || len(res.Ahead) != 1 {
		t.Fatalf("report = %+v, %v", res, err)
	}
	if f := floorOf(t, st); f.Incarnation != 1 || f.Generation != 20 || f.ProgressEpoch != 0 || f.IncarnationFrom != 0 {
		t.Fatalf("seeded record = %+v", f)
	}
	g, err := a.Boot(ctx, "inst-1", bootfloor.Entry{Incarnation: 1, Generation: 18, Epoch: 17, Seq: 4})
	if err != nil || g.Incarnation != 1 || g.Generation != 21 || g.ProgressEpoch != 17 || g.ProgressSeq != 4 {
		t.Fatalf("honest host's grant = %+v, %v", g, err)
	}
}
