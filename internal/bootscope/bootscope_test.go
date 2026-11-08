package bootscope

import (
	"errors"
	"testing"
	"time"

	"github.com/sufuree/sufuree/internal/resource"
)

func TestPinSessionTagsForcesReplica(t *testing.T) {
	got, err := PinSessionTags("i1", map[string]string{"env": "prod"})
	if err != nil {
		t.Fatal(err)
	}
	if got[ReplicaTagKey] != "inst/i1" || got["env"] != "prod" {
		t.Fatalf("got %#v", got)
	}
}

func TestPinSessionTagsRejectsOtherInstallation(t *testing.T) {
	_, err := PinSessionTags("i1", map[string]string{ReplicaTagKey: "inst/other"})
	if !errors.Is(err, ErrTagMismatch) {
		t.Fatalf("err=%v", err)
	}
	_, err = PinSessionTags("i1", map[string]string{"alt": "inst/other"})
	if !errors.Is(err, ErrTagMismatch) {
		t.Fatalf("err=%v", err)
	}
}

func TestAuthorizeRequiresActivePlacement(t *testing.T) {
	now := time.Now().UTC()
	placements := []resource.Placement{
		{
			UID: "p1", ResourceUID: "r1", HostUID: "h1", Replica: "i1",
			Generation: 1, OwnerEpoch: 1, MemoryMiB: 512, Phase: resource.PhaseReady, UpdatedAt: now,
		},
		{
			UID: "p2", ResourceUID: "r1", HostUID: "h1", Replica: "i2",
			Generation: 1, OwnerEpoch: 1, MemoryMiB: 512, Phase: resource.PhaseDormant, UpdatedAt: now,
		},
	}
	if _, err := Authorize(placements, "h1", "i1", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := Authorize(placements, "h1", "i2", nil); !errors.Is(err, ErrNotHeld) {
		t.Fatalf("dormant should be refused: %v", err)
	}
	if _, err := Authorize(placements, "h2", "i1", nil); !errors.Is(err, ErrNotHeld) {
		t.Fatalf("other host should be refused: %v", err)
	}
}

func TestAssignmentsForHost(t *testing.T) {
	now := time.Now().UTC()
	placements := []resource.Placement{
		{
			UID: "p1", ResourceUID: "r1", HostUID: "h1", Replica: "i1",
			Generation: 1, OwnerEpoch: 1, MemoryMiB: 512, Phase: resource.PhaseReady, UpdatedAt: now,
		},
		{
			UID: "p2", ResourceUID: "r1", HostUID: "h1", Replica: "i2",
			Generation: 1, OwnerEpoch: 1, MemoryMiB: 512, Phase: resource.PhaseDormant, UpdatedAt: now,
		},
		{
			UID: "p3", ResourceUID: "r1", HostUID: "h2", Replica: "i3",
			Generation: 1, OwnerEpoch: 1, MemoryMiB: 512, Phase: resource.PhaseReady, UpdatedAt: now,
		},
	}
	got := AssignmentsForHost(placements, "h1")
	if len(got) != 1 || got[0].SessionTags[ReplicaTagKey] != "inst/i1" {
		t.Fatalf("got %#v", got)
	}
}
