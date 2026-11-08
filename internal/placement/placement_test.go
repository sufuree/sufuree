package placement

import (
	"testing"
	"time"
)

func host(id string, mem, used int) Host {
	return Host{ID: id, MemoryMiB: mem, UsedMiB: used, Ready: true}
}

func TestPacksOntoLeastSpare(t *testing.T) {
	t.Parallel()
	got, err := Plan(Input{
		Hosts: []Host{host("b", 1000, 100), host("a", 1000, 800)},
		Workloads: []Workload{{
			ResourceUID: "app1", Name: "web", MemoryMiB: 150, MaxReplicas: 1, PendingDemand: 1,
			Lifecycle: "enabled",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Op != OpPlaceReplica || got[0].HostID != "a" {
		t.Fatalf("decisions = %+v", got)
	}
}

func TestOneCapacityRequestForManyWaiters(t *testing.T) {
	t.Parallel()
	got, err := Plan(Input{
		MaxHosts: 4,
		Hosts:    []Host{host("h", 100, 0)},
		Workloads: []Workload{
			{ResourceUID: "b", MemoryMiB: 200, MaxReplicas: 1, PendingDemand: 3, Lifecycle: "enabled"},
			{ResourceUID: "a", MemoryMiB: 150, MaxReplicas: 1, PendingDemand: 9, Lifecycle: "enabled"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var caps int
	var mem int
	for _, d := range got {
		if d.Op == OpRequestCapacity {
			caps++
			mem = d.MemoryMiB
		}
		if d.Op == OpPlaceReplica {
			t.Fatalf("placed onto a host that cannot fit: %+v", d)
		}
	}
	if caps != 1 || mem != 200 {
		t.Fatalf("caps=%d mem=%d decisions=%+v", caps, mem, got)
	}
}

func TestUnknownCreateReservesCapacity(t *testing.T) {
	t.Parallel()
	got, err := Plan(Input{
		MaxHosts: 4,
		Ops:      []Operation{{ID: "op", Action: "create_host", Outcome: "unknown"}},
		Workloads: []Workload{{
			ResourceUID: "app", MemoryMiB: 128, MaxReplicas: 1, PendingDemand: 4, Lifecycle: "enabled",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range got {
		if d.Op == OpRequestCapacity {
			t.Fatalf("second capacity request: %+v", got)
		}
	}
}

func TestPreparedCreateAlsoBlocksAnother(t *testing.T) {
	t.Parallel()
	for _, outcome := range []string{"prepared", "dispatched"} {
		got, err := Plan(Input{
			Ops: []Operation{{ID: "op", Action: "create_host", Outcome: outcome}},
			Workloads: []Workload{{
				ResourceUID: "app", MemoryMiB: 64, MaxReplicas: 1, PendingDemand: 1, Lifecycle: "enabled",
			}},
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range got {
			if d.Op == OpRequestCapacity {
				t.Fatalf("outcome %s still requested capacity: %+v", outcome, got)
			}
		}
	}
}

func TestFailedCreateDoesNotReserve(t *testing.T) {
	t.Parallel()
	got, err := Plan(Input{
		MaxHosts: 4,
		Ops:      []Operation{{ID: "op", Action: "create_host", Outcome: "failed"}},
		Workloads: []Workload{{
			ResourceUID: "app", MemoryMiB: 64, MaxReplicas: 1, PendingDemand: 1, Lifecycle: "enabled",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Op != OpRequestCapacity {
		t.Fatalf("decisions = %+v", got)
	}
}

func TestMinHostsZeroTerminatesAfterQuiet(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC)
	got, err := Plan(Input{
		Now: now, MinHosts: 0, Quiet: time.Minute,
		Hosts: []Host{{
			ID: "h", MemoryMiB: 512, Ready: true,
			EmptySince: now.Add(-time.Minute),
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Op != OpDrainHost || got[1].Op != OpTerminateHost || got[1].HostID != "h" {
		t.Fatalf("decisions = %+v", got)
	}
}

func TestQuietAndUnknownBlockScaleIn(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC)
	empty := Host{ID: "h", MemoryMiB: 512, Ready: true, EmptySince: now.Add(-time.Second)}
	cases := []struct {
		name string
		in   Input
	}{
		{"within quiet", Input{Now: now, Quiet: time.Minute, Hosts: []Host{empty}}},
		{"idle unknown", Input{Now: now, Hosts: []Host{{ID: "h", MemoryMiB: 512, Ready: true}}}},
		{"unknown terminate", Input{
			Now: now, Quiet: 0,
			Hosts: []Host{{ID: "h", MemoryMiB: 512, Ready: true, EmptySince: now.Add(-time.Hour)}},
			Ops:   []Operation{{ID: "t", Action: "terminate_host", Outcome: "unknown", HostID: "h"}},
		}},
		{"min hosts floor", Input{
			Now: now, MinHosts: 1, Quiet: 0,
			Hosts: []Host{{ID: "h", MemoryMiB: 512, Ready: true, EmptySince: now.Add(-time.Hour)}},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Plan(tc.in)
			if err != nil {
				t.Fatal(err)
			}
			for _, d := range got {
				if d.Op == OpDrainHost || d.Op == OpTerminateHost {
					t.Fatalf("scaled in: %+v", got)
				}
			}
		})
	}
}

func TestReleaseDoesNotTerminateSamePlan(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC)
	got, err := Plan(Input{
		Now: now, Quiet: 0,
		Hosts: []Host{{ID: "h", MemoryMiB: 256, Ready: true, EmptySince: now.Add(-time.Hour)}},
		Workloads: []Workload{{
			ResourceUID: "app", MemoryMiB: 64, Lifecycle: "enabled", Idle: time.Second,
			Replicas: []Replica{{PlacementID: "p1", HostID: "h", Since: now.Add(-time.Second)}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Op != OpReleaseReplica {
		t.Fatalf("decisions = %+v", got)
	}
}

func TestProtectedReplicaAndUnknownHostAreNotReusable(t *testing.T) {
	t.Parallel()
	got, err := Plan(Input{
		Hosts: []Host{host("h", 512, 0)},
		Ops:   []Operation{{ID: "t", Action: "terminate_host", Outcome: "unknown", HostID: "h"}},
		Workloads: []Workload{{
			ResourceUID: "app", MemoryMiB: 64, Lifecycle: "suspended",
			Replicas: []Replica{{PlacementID: "p1", HostID: "h", Protected: true}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("protected replica was dropped or host reused: %+v", got)
	}
}

func TestDemandPlacesOneNotPerRequest(t *testing.T) {
	t.Parallel()
	got, err := Plan(Input{
		Hosts: []Host{host("h", 1024, 0)},
		Workloads: []Workload{{
			ResourceUID: "app", MemoryMiB: 128, MaxReplicas: 1, PendingDemand: 40, Lifecycle: "enabled",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Op != OpPlaceReplica {
		t.Fatalf("decisions = %+v", got)
	}
}

func TestMaxHostsBlocksCapacity(t *testing.T) {
	t.Parallel()
	got, err := Plan(Input{
		MaxHosts: 1,
		Hosts:    []Host{host("h", 64, 0)},
		Workloads: []Workload{{
			ResourceUID: "app", MemoryMiB: 128, MaxReplicas: 1, PendingDemand: 1, Lifecycle: "enabled",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range got {
		if d.Op == OpRequestCapacity {
			t.Fatalf("capacity at ceiling: %+v", got)
		}
	}
}

func TestMinReplicasWithoutDemand(t *testing.T) {
	t.Parallel()
	got, err := Plan(Input{
		Hosts: []Host{host("h", 1024, 0)},
		Workloads: []Workload{{
			ResourceUID: "app", MemoryMiB: 100, MinReplicas: 2, MaxReplicas: 2, Lifecycle: "enabled",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, d := range got {
		if d.Op == OpPlaceReplica && d.HostID == "h" {
			n++
		}
	}
	if n != 2 {
		t.Fatalf("placed %d: %+v", n, got)
	}
}

func TestTieBreaksByHostID(t *testing.T) {
	t.Parallel()
	got, err := Plan(Input{
		Hosts: []Host{host("b", 500, 0), host("a", 500, 0)},
		Workloads: []Workload{{
			ResourceUID: "app", MemoryMiB: 100, MaxReplicas: 1, PendingDemand: 1, Lifecycle: "enabled",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].HostID != "a" {
		t.Fatalf("decisions = %+v", got)
	}
}

func TestWarmObserveReportsShortfallWithoutBuying(t *testing.T) {
	t.Parallel()
	res, err := PlanResult(Input{
		MaxHosts: 4, WarmMode: WarmObserve, ReserveMiB: 512,
		Hosts: []Host{host("h", 600, 200)},
		Workloads: []Workload{{
			ResourceUID: "app", MemoryMiB: 100, MaxReplicas: 1, MinReplicas: 1, Lifecycle: "enabled",
			Replicas: []Replica{{PlacementID: "p1", HostID: "h", Since: time.Now()}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range res.Decisions {
		if d.Op == OpRequestCapacity {
			t.Fatalf("observe must not buy buffer: %+v", res.Decisions)
		}
	}
	if res.Buffer == nil || res.Buffer.ShortfallMiB <= 0 || res.Buffer.WouldBuy {
		t.Fatalf("buffer = %+v", res.Buffer)
	}
}

func TestWarmActiveBuysBufferWhenDemandMet(t *testing.T) {
	t.Parallel()
	res, err := PlanResult(Input{
		MaxHosts: 4, WarmMode: WarmActive, ReserveMiB: 512,
		Hosts: []Host{host("h", 600, 200)},
		Workloads: []Workload{{
			ResourceUID: "app", MemoryMiB: 100, MaxReplicas: 1, MinReplicas: 1, Lifecycle: "enabled",
			Replicas: []Replica{{PlacementID: "p1", HostID: "h", Since: time.Now()}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var bought bool
	for _, d := range res.Decisions {
		if d.Op == OpRequestCapacity && d.Reason == "warm capacity reserve" {
			bought = true
			if d.MemoryMiB != res.Buffer.ShortfallMiB {
				t.Fatalf("buy mem=%d buffer=%+v", d.MemoryMiB, res.Buffer)
			}
		}
	}
	if !bought || res.Buffer == nil || !res.Buffer.WouldBuy {
		t.Fatalf("decisions=%+v buffer=%+v", res.Decisions, res.Buffer)
	}
}
