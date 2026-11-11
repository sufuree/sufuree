package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/sufuree/sufuree/internal/resource"
)

func seedApp(t *testing.T, s *Store) {
	t.Helper()
	mustUpdate(t, s, func(tx *Tx) error {
		if _, err := tx.ApplyResource(exampleApp()); err != nil {
			return err
		}
		if _, err := tx.ApplyResource(examplePool()); err != nil {
			return err
		}
		if err := tx.PutMeta(resource.ClusterMeta{DeploymentUID: "dep-1", RecoveryIncarnation: 1, OwnerEpoch: 4}); err != nil {
			return err
		}
		return tx.PutHost(exampleHost("pool-1"))
	})
}

func revocations(t *testing.T, s *Store) []resource.Revocation {
	t.Helper()
	var out []resource.Revocation
	if err := s.View(func(tx *Tx) error {
		var err error
		out, err = tx.ListRevocations()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestReleasingAOneOffReplicaRecordsItsRevocation(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	seedApp(t, s)
	own := examplePlacement("app-1", "host-1")
	own.UID, own.Replica = "place-own", "app-1"
	moved := own
	moved.UID = "place-moved"
	oneOff := examplePlacement("app-1", "host-1")
	oneOff.UID, oneOff.Replica = "place-r", "r0001"
	twin := oneOff
	twin.UID = "place-r-twin"
	mustUpdate(t, s, func(tx *Tx) error {
		for _, p := range []resource.Placement{own, moved, oneOff, twin} {
			if err := tx.PutPlacement(p); err != nil {
				return err
			}
		}
		return nil
	})

	// The app's own UID survives every release, scale to zero included.
	mustUpdate(t, s, func(tx *Tx) error { return tx.DeletePlacement("place-own") })
	mustUpdate(t, s, func(tx *Tx) error { return tx.DeletePlacement("place-moved") })
	// A one-off ID is revoked when its last placement goes.
	mustUpdate(t, s, func(tx *Tx) error { return tx.DeletePlacement("place-r") })
	if got := pending(t, s); len(got) != 0 {
		t.Fatalf("revoked while held or for the app's own UID: %+v", got)
	}
	mustUpdate(t, s, func(tx *Tx) error { return tx.DeletePlacement("place-r-twin") })
	got := pending(t, s)
	if len(got) != 1 || got[0].Installation != "r0001" || got[0].ResourceUID != "app-1" || !got[0].Pending() {
		t.Fatalf("queue = %+v", got)
	}
	if got := revocations(t, s); len(got) != 0 {
		t.Fatalf("a one-off replica ID got a tombstone: %+v", got)
	}

	// Once revoked, a one-off ID leaves no record at all.
	at := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	mustUpdate(t, s, func(tx *Tx) error { return tx.MarkRevoked("r0001", at) })
	mustUpdate(t, s, func(tx *Tx) error { return tx.MarkRevoked("r0001", at.Add(time.Hour)) })
	if got, tomb := pending(t, s), revocations(t, s); len(got) != 0 || len(tomb) != 0 {
		t.Fatalf("after MarkRevoked: queue %+v, tombstones %+v", got, tomb)
	}
}

func pending(t *testing.T, s *Store) []resource.Revocation {
	t.Helper()
	var out []resource.Revocation
	if err := s.View(func(tx *Tx) error {
		var err error
		out, _, err = tx.PendingRevocations("", 1000)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestPendingRevocationsPagesFromACursor(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	mustUpdate(t, s, func(tx *Tx) error {
		for _, id := range []string{"r1", "r2", "r3", "r4"} {
			if err := tx.revoke(id, "app-1"); err != nil {
				return err
			}
		}
		return nil
	})
	page := func(after string, n int) (ids string, more bool) {
		t.Helper()
		if err := s.View(func(tx *Tx) error {
			got, m, err := tx.PendingRevocations(after, n)
			for _, r := range got {
				ids += r.Installation + ","
			}
			more = m
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return ids, more
	}
	for _, c := range []struct {
		after string
		n     int
		want  string
		more  bool
	}{
		{"", 2, "r1,r2,", true},
		{"r2", 2, "r3,r4,", false},
		{"r2x", 5, "r3,r4,", false},
		{"r4", 2, "", false},
	} {
		if got, more := page(c.after, c.n); got != c.want || more != c.more {
			t.Errorf("after %q limit %d: %q more=%v, want %q more=%v", c.after, c.n, got, more, c.want, c.more)
		}
	}
}

func TestDeletingAnAppRevokesItsUIDForGood(t *testing.T) {
	t.Parallel()
	s := newStore(t)
	seedApp(t, s)
	mustUpdate(t, s, func(tx *Tx) error {
		if err := tx.DeleteHost("host-1"); err != nil {
			return err
		}
		return tx.DeleteResource("pool-1")
	})
	if got := revocations(t, s); len(got) != 0 {
		t.Fatalf("a pool was revoked: %+v", got)
	}
	mustUpdate(t, s, func(tx *Tx) error { return tx.DeleteResource("app-1") })
	if got := revocations(t, s); len(got) != 1 || got[0].Installation != "app-1" || len(pending(t, s)) != 1 {
		t.Fatalf("revocations = %+v, queue %+v", got, pending(t, s))
	}
	// The tombstone outlives the queue entry.
	mustUpdate(t, s, func(tx *Tx) error { return tx.MarkRevoked("app-1", time.Now()) })
	if got := revocations(t, s); len(got) != 1 || got[0].Pending() || len(pending(t, s)) != 0 {
		t.Fatalf("after MarkRevoked: %+v", got)
	}
	err := s.Update(func(tx *Tx) error {
		_, err := tx.ApplyResource(exampleApp())
		return err
	})
	if !errors.Is(err, ErrRevoked) {
		t.Fatalf("re-creating a deleted app: %v", err)
	}
}

func TestImportAcceptsAnArchiveWithoutRevocations(t *testing.T) {
	t.Parallel()
	src := newStore(t)
	seedApp(t, src)
	mustUpdate(t, src, func(tx *Tx) error { return tx.DeleteResource("app-1") })
	var buf bytes.Buffer
	if err := src.Export(&buf); err != nil {
		t.Fatal(err)
	}
	var arch map[string]any
	if err := json.Unmarshal(buf.Bytes(), &arch); err != nil {
		t.Fatal(err)
	}
	var kept []any
	for _, b := range arch["buckets"].([]any) {
		if n := b.(map[string]any)["name"]; n != "revocations" && n != "revoke_queue" {
			kept = append(kept, b)
		}
	}
	arch["buckets"] = kept
	old, _ := json.Marshal(arch)

	dst := newStore(t)
	if err := dst.Import(bytes.NewReader(old)); err != nil {
		t.Fatalf("import of an archive from before revocations: %v", err)
	}
	if got := revocations(t, dst); len(got) != 0 {
		t.Fatalf("revocations = %+v", got)
	}
	// A current archive carries them.
	if err := dst.Import(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatal(err)
	}
	if got := revocations(t, dst); len(got) != 1 {
		t.Fatalf("revocations after import = %+v", got)
	}
}
