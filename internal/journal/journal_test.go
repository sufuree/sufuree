package journal

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestGenerationsAreDurableAndNeverRepeat(t *testing.T) {
	dir := t.TempDir()
	j, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for want := uint64(1); want <= 3; want++ {
		got, err := j.NextGeneration("w")
		if err != nil || got != want {
			t.Fatalf("NextGeneration = %d, %v; want %d", got, err, want)
		}
	}
	j2, _ := Open(dir) // a restarted agent
	if got, _ := j2.NextGeneration("w"); got != 4 {
		t.Fatalf("after restart = %d, want 4", got)
	}
	if got, _ := j2.NextGeneration("other"); got != 1 {
		t.Fatalf("independent workload = %d, want 1", got)
	}
}

func TestCorruptRecordRefusesToIssue(t *testing.T) {
	dir := t.TempDir()
	j, _ := Open(dir)
	if err := os.WriteFile(filepath.Join(dir, "w.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := j.NextGeneration("w"); err == nil {
		t.Fatal("issued a generation over a corrupt record")
	}
}

func TestProgressOnlyAdvancesAndIsDurable(t *testing.T) {
	dir := t.TempDir()
	j, _ := Open(dir)
	if _, err := j.NextGeneration("w"); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		epoch, seq uint64
		up         bool
	}{{2, 10, true}, {2, 10, false}, {2, 5, false}, {1, 99, false}, {2, 11, true}, {3, 1, true}} {
		_, up, err := j.AdvanceProgress("w", c.epoch, c.seq)
		if err != nil || up != c.up {
			t.Fatalf("AdvanceProgress(%d, %d) = %v, %v; want %v", c.epoch, c.seq, up, err, c.up)
		}
	}
	r, _ := Open(dir) // a restarted agent
	got, err := r.Get("w")
	if err != nil || got.ProgressEpoch != 3 || got.ProgressSeq != 1 || got.Generation != 1 || got.Version != 2 {
		t.Fatalf("after restart: %+v, %v", got, err)
	}
	// Issuing a generation keeps the floor.
	if _, err := r.NextGeneration("w"); err != nil {
		t.Fatal(err)
	}
	if got, _ := r.Get("w"); got.ProgressEpoch != 3 || got.ProgressSeq != 1 || got.Version != 2 {
		t.Fatalf("after NextGeneration: %+v", got)
	}
}

func TestAdoptControllerGrants(t *testing.T) {
	j, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.NextGeneration("w"); err != nil { // standalone history: H=1
		t.Fatal(err)
	}
	if _, _, err := j.AdvanceProgress("w", 1, 5); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Adopt("w", Grant{Generation: 1, Incarnation: 1}); !errors.Is(err, ErrNotAbove) {
		t.Fatalf("a grant not above the host's record: %v", err)
	}
	r, err := j.Adopt("w", Grant{Generation: 2, Incarnation: 1, Epoch: 1, Seq: 3})
	if err != nil || r.Generation != 2 || r.Incarnation != 1 || r.ProgressSeq != 5 || r.Version != 3 {
		t.Fatalf("same incarnation keeps the higher floor: %+v, %v", r, err)
	}
	r, err = j.Adopt("w", Grant{Generation: 3, Incarnation: 2, IncarnationFrom: 3, Epoch: 1, Seq: 1})
	if err != nil || r.Incarnation != 2 || r.IncarnationFrom != 3 || r.ProgressSeq != 1 {
		t.Fatalf("a newer incarnation replaces the floor: %+v, %v", r, err)
	}
	if _, err := j.Adopt("w", Grant{Generation: 4, Incarnation: 1}); !errors.Is(err, ErrNewerLocalIncarnation) {
		t.Fatalf("an older incarnation: %v", err)
	}
	j2, _ := Open(j.dir) // durable across restarts
	if r, err := j2.Get("w"); err != nil || r.Generation != 3 || r.Incarnation != 2 {
		t.Fatalf("after restart: %+v, %v", r, err)
	}
	// Progress keeps the version 3 record at version 3.
	if _, _, err := j2.AdvanceProgress("w", 3, 1); err != nil {
		t.Fatal(err)
	}
	if r, _ := j2.Get("w"); r.Version != 3 || r.Incarnation != 2 {
		t.Fatalf("AdvanceProgress downgraded the record: %+v", r)
	}
}
