package journal

import (
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
