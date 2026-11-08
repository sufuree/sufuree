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
