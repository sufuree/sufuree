// Package qual holds local (no-AWS) qualification checks for RFC-0001 slices.
package qual_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sufuree/sufuree/internal/bootscope"
	"github.com/sufuree/sufuree/internal/controlraft"
	"github.com/sufuree/sufuree/internal/demand"
	"github.com/sufuree/sufuree/internal/executor"
	"github.com/sufuree/sufuree/internal/raftnode"
	"github.com/sufuree/sufuree/internal/reconcile"
	"github.com/sufuree/sufuree/internal/resource"
	"github.com/sufuree/sufuree/internal/store"
)

// TestLocalRaftMutationsAndBootscope is a no-AWS qualification scenario:
// Raft journal mutations, memory executor capacity, assignment publish, and
// credential authorize. Writes a receipt under testdata/receipts when OK.
func TestLocalRaftMutationsAndBootscope(t *testing.T) {
	dir := t.TempDir()
	st, err := store.Open(filepath.Join(dir, "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	node, err := raftnode.Open(raftnode.Config{
		DataDir: filepath.Join(dir, "raft"), NodeID: "qual-1", BindAddr: addr, Bootstrap: true,
	}, controlraft.StoreFSM(st))
	if err != nil {
		t.Fatal(err)
	}
	defer node.Close()
	deadline := time.Now().Add(10 * time.Second)
	for !node.IsLeader() {
		if time.Now().After(deadline) {
			t.Fatal("not leader")
		}
		time.Sleep(20 * time.Millisecond)
	}

	journal := &controlraft.Journal{Store: st, Node: node}
	if err := journal.Update(func(tx *store.Tx) error {
		return tx.PutMeta(resource.ClusterMeta{
			DeploymentUID: "qual-local", RecoveryIncarnation: 1, OwnerEpoch: 1,
		})
	}); err != nil {
		t.Fatal(err)
	}

	mem := &executor.Memory{AllowEffects: true}
	dem := demand.New(demand.Bolt{Store: journal})
	c := reconcile.New(journal, mem, dem, reconcile.Options{
		Tick: time.Second, Backoff: time.Hour, Now: time.Now,
	})
	srv := httptest.NewServer(reconcile.Handler(c))
	defer srv.Close()

	ctx := context.Background()
	_, err = c.ApplyResource(ctx, resource.Resource{
		Name: "main", Kind: resource.KindPool, Lifecycle: resource.LifecycleEnabled,
		Spec: resource.PoolSpec{
			MinHosts: 0, MaxHosts: 4,
			QuietPeriod: resource.Duration{Duration: time.Millisecond},
			WarmMode:    resource.WarmOff,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	app, err := c.ApplyResource(ctx, resource.Resource{
		Name: "web", Kind: resource.KindApp, Lifecycle: resource.LifecycleEnabled,
		Spec: resource.AppSpec{
			Image: "img", MemoryMiB: 128, GuestPort: 8080,
			IdleTimeout:    resource.Duration{Duration: time.Hour},
			StartupTimeout: resource.Duration{Duration: time.Second},
			MinReplicas:    0, MaxReplicas: 1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := dem.Join(ctx, app.UID, "qual", time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := c.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.ReconcileOnce(ctx); err != nil {
		t.Fatal(err)
	}

	hosts, err := c.ListHosts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(hosts) != 1 {
		t.Fatalf("hosts = %d", len(hosts))
	}
	placements, err := c.ListPlacementsByHost(ctx, hosts[0].UID)
	if err != nil {
		t.Fatal(err)
	}
	as := bootscope.AssignmentsForHost(placements, hosts[0].UID)
	if len(as) != 1 {
		t.Fatalf("assignments = %+v", as)
	}

	res, err := http.Get(srv.URL + "/v1/hosts/" + hosts[0].UID + "/assignments")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("assignments status = %s", res.Status)
	}

	credRes, err := http.Post(srv.URL+"/v1/hosts/"+hosts[0].UID+"/credentials/"+as[0].Placement.Replica,
		"application/json", bytes.NewBufferString(`{"requested_tags":{}}`))
	if err != nil {
		t.Fatal(err)
	}
	defer credRes.Body.Close()
	if credRes.StatusCode != http.StatusOK {
		t.Fatalf("authorize: %s", credRes.Status)
	}
	var cred struct {
		SessionTags map[string]string `json:"session_tags"`
	}
	if err := json.NewDecoder(credRes.Body).Decode(&cred); err != nil {
		t.Fatal(err)
	}
	want := bootscope.TagValue(as[0].Placement.Replica)
	if cred.SessionTags[bootscope.ReplicaTagKey] != want {
		t.Fatalf("session_tags = %+v want replica %s", cred.SessionTags, want)
	}

	// Foreign installation must fail closed.
	bad, err := http.Post(srv.URL+"/v1/hosts/"+hosts[0].UID+"/credentials/not-held",
		"application/json", bytes.NewBufferString(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	defer bad.Body.Close()
	if bad.StatusCode != http.StatusForbidden {
		t.Fatalf("foreign installation status = %s", bad.Status)
	}

	receipt := map[string]any{
		"scenario":     "local-raft-mutations-bootscope",
		"aws_spend":    false,
		"raft":         true,
		"host":         hosts[0].UID,
		"installation": as[0].Placement.Replica,
		"replica_tag":  want,
		"qualified_at": time.Now().UTC().Format(time.RFC3339),
	}
	if dir := os.Getenv("SUFUREE_QUAL_RECEIPT_DIR"); dir != "" {
		writeReceipt(t, dir, receipt)
	}
}

func writeReceipt(t *testing.T, dir string, receipt map[string]any) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "local-raft-bootscope.json")
	buf, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(buf, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}
