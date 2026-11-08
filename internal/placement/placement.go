// Package placement decides where admitted work runs and when the pool
// grows or shrinks. It does not dispatch effects. The controller persists
// each decision before any executor call.
//
// Packing prefers the ready host that still fits and would have the least
// spare afterwards. One plan emits at most one capacity request, however
// many workloads are waiting. minHosts may be zero. Operations whose outcome
// is still unknown keep their reservations: they are not free capacity and
// they are not retried as a second launch or a second termination.
package placement

import (
	"fmt"
	"sort"
	"time"
)

// Op is one planner decision.
type Op string

const (
	OpPlaceReplica    Op = "place_replica"
	OpReleaseReplica  Op = "release_replica"
	OpRequestCapacity Op = "request_capacity"
	OpDrainHost       Op = "drain_host"
	OpTerminateHost   Op = "terminate_host"
)

// Decision is a desired transition. PlacementID is set for a release of an
// existing replica. MemoryMiB is the guest reservation for a placement and
// the size of the host to request for capacity.
type Decision struct {
	Op          Op
	ResourceUID string
	Name        string
	HostID      string
	PlacementID string
	MemoryMiB   int
	CPU         int
	Reason      string
}

// Host is one observed backend. MemoryMiB is allocatable capacity, not
// sampled RSS. UsedMiB is the sum of durable guest reservations. Ready is
// false until the host is enrolled. EmptySince is zero when the host is not
// empty or the idle observation has not been recorded; unknown idle is not
// permission to terminate.
type Host struct {
	ID         string
	MemoryMiB  int
	UsedMiB    int
	CPU        int
	UsedCPU    int
	Draining   bool
	Ready      bool
	EmptySince time.Time
}

// Replica is one durable placement. Since is when it was assigned. A zero
// Since is not proof the replica is idle.
type Replica struct {
	PlacementID string
	HostID      string
	Since       time.Time
	// Protected replicas cannot be released. An unknown cleanup is protected.
	Protected bool
}

// Workload is one application the planner may place.
type Workload struct {
	ResourceUID   string
	Name          string
	MemoryMiB     int
	CPU           int
	MinReplicas   int
	MaxReplicas   int // hard ceiling; 0 admits no replicas
	Idle          time.Duration
	Lifecycle     string
	PendingDemand int
	Replicas      []Replica
}

// Operation is an unfinished or recorded infrastructure effect.
type Operation struct {
	ID      string
	Action  string // create-host or terminate-host
	Outcome string // prepared, dispatched, confirmed, unknown, failed
	HostID  string
}

// WarmMode is the pool's spare-capacity policy from RFC-0001 §5.
type WarmMode string

const (
	WarmOff     WarmMode = "off"
	WarmObserve WarmMode = "observe"
	WarmActive  WarmMode = "active"
)

// BufferRecommendation is a computed warm-capacity proposal. Observe mode
// returns it without emitting RequestCapacity for the buffer. Active mode may
// emit RequestCapacity when ReadySpareMiB is below ReserveMiB and no create is
// already in flight.
type BufferRecommendation struct {
	Mode          WarmMode
	ReserveMiB    int // configured minimum bundle size
	ReadySpareMiB int // allocatable memory on ready non-draining hosts after placements
	ShortfallMiB  int
	WouldBuy      bool // true when active mode would request capacity for the buffer
	Reason        string
}

// Result is Plan's decisions plus an optional warm-capacity recommendation.
type Result struct {
	Decisions []Decision
	Buffer    *BufferRecommendation
}

// Input is a point-in-time view. MaxHosts is a hard ceiling; 0 admits no
// further host. MinHosts may be 0. DisableScaleIn leaves idle hosts in
// place when the pool policy is unknown.
type Input struct {
	Now            time.Time
	MinHosts       int
	MaxHosts       int
	Quiet          time.Duration
	DisableScaleIn bool
	// WarmMode and ReserveMiB are the demand-based warm-capacity minimum.
	// ReserveMiB is the operator-selected bundle floor during active demand;
	// zero means no buffer target. WarmOff ignores ReserveMiB.
	WarmMode   WarmMode
	ReserveMiB int
	Hosts      []Host
	Workloads  []Workload
	Ops        []Operation
}

type hostState struct {
	Host
	usedMiB     int
	usedCPU     int
	placements  int
	startN      int
	uncertain   bool
	terminating bool
}

// Plan returns decisions in application order: releases, placements, at most
// one capacity request, then drain and terminate for hosts that were already
// empty and past the quiet period. Releasing the last replica does not
// terminate the host in the same plan; the quiet period starts when emptiness
// is observed.
//
// Prefer PlanResult when the caller needs the warm-capacity recommendation.
func Plan(in Input) ([]Decision, error) {
	res, err := PlanResult(in)
	if err != nil {
		return nil, err
	}
	return res.Decisions, nil
}

// PlanResult is Plan plus the warm-capacity recommendation for the pool.
func PlanResult(in Input) (Result, error) {
	if in.MinHosts < 0 || in.MaxHosts < 0 {
		return Result{}, fmt.Errorf("placement: negative host bounds")
	}
	if in.Quiet < 0 {
		return Result{}, fmt.Errorf("placement: negative quiet period")
	}
	if in.ReserveMiB < 0 {
		return Result{}, fmt.Errorf("placement: negative reserve")
	}
	switch in.WarmMode {
	case "", WarmOff, WarmObserve, WarmActive:
	default:
		return Result{}, fmt.Errorf("placement: warm mode %q is not supported", in.WarmMode)
	}
	if in.WarmMode == "" {
		in.WarmMode = WarmOff
	}
	hosts := map[string]*hostState{}
	var order []string
	for _, h := range in.Hosts {
		if h.ID == "" {
			return Result{}, fmt.Errorf("placement: host missing id")
		}
		if _, ok := hosts[h.ID]; ok {
			return Result{}, fmt.Errorf("placement: duplicate host %s", h.ID)
		}
		st := &hostState{Host: h, usedMiB: h.UsedMiB, usedCPU: h.UsedCPU}
		hosts[h.ID] = st
		order = append(order, h.ID)
	}
	sort.Strings(order)

	inflightCreate := 0
	for _, op := range in.Ops {
		if !reserves(op.Outcome) {
			continue
		}
		switch {
		case isCreate(op.Action):
			inflightCreate++
		case isTerminate(op.Action):
			if st := hosts[op.HostID]; st != nil {
				st.uncertain = op.Outcome == "unknown"
				st.terminating = true
			}
		}
	}

	workloads := append([]Workload(nil), in.Workloads...)
	sort.Slice(workloads, func(i, j int) bool { return workloads[i].ResourceUID < workloads[j].ResourceUID })

	for _, h := range hosts {
		for _, w := range workloads {
			for _, r := range w.Replicas {
				if r.HostID == h.ID {
					h.placements++
					h.startN++
				}
			}
		}
	}

	var decisions []Decision
	unmetMiB := 0
	activeDemand := false
	for _, w := range workloads {
		if w.ResourceUID == "" {
			return Result{}, fmt.Errorf("placement: workload missing uid")
		}
		if w.MemoryMiB < 0 || w.CPU < 0 || w.MinReplicas < 0 || w.MaxReplicas < 0 {
			return Result{}, fmt.Errorf("placement: %s has a negative resource field", w.ResourceUID)
		}
		if w.PendingDemand > 0 || desired(w) > 0 {
			activeDemand = true
		}
		rels, err := releases(in.Now, w)
		if err != nil {
			return Result{}, err
		}
		decisions = append(decisions, rels...)
		for _, d := range rels {
			if st := hosts[d.HostID]; st != nil && st.placements > 0 {
				st.placements--
				st.usedMiB -= w.MemoryMiB
				st.usedCPU -= w.CPU
				if st.usedMiB < 0 {
					st.usedMiB = 0
				}
				if st.usedCPU < 0 {
					st.usedCPU = 0
				}
			}
		}
		need := desired(w) - kept(w, rels)
		if need <= 0 {
			continue
		}
		if w.MemoryMiB <= 0 {
			return Result{}, fmt.Errorf("placement: %s needs a replica but memory_mib is %d", w.ResourceUID, w.MemoryMiB)
		}
		for need > 0 {
			h := bestFit(hosts, order, w.MemoryMiB, w.CPU)
			if h == nil {
				if w.MemoryMiB > unmetMiB {
					unmetMiB = w.MemoryMiB
				}
				break
			}
			decisions = append(decisions, Decision{
				Op: OpPlaceReplica, ResourceUID: w.ResourceUID, Name: w.Name,
				HostID: h.ID, MemoryMiB: w.MemoryMiB, CPU: w.CPU,
				Reason: "pack onto existing host",
			})
			h.placements++
			h.usedMiB += w.MemoryMiB
			h.usedCPU += w.CPU
			need--
		}
	}

	usable := 0
	for _, id := range order {
		h := hosts[id]
		if !h.terminating {
			usable++
		}
	}
	capacityRequested := false
	shortFloor := in.MinHosts > usable+inflightCreate
	if (unmetMiB > 0 || shortFloor) && inflightCreate == 0 && underMax(in.MaxHosts, usable, inflightCreate) {
		mem := unmetMiB
		reason := "admitted demand does not fit"
		if mem == 0 {
			mem = 0
			reason = "pool is below min_hosts"
		}
		decisions = append(decisions, Decision{
			Op: OpRequestCapacity, MemoryMiB: mem, Reason: reason,
		})
		capacityRequested = true
	}

	buf := warmBuffer(in, hosts, order, activeDemand, unmetMiB == 0 && !shortFloor)
	if buf != nil && buf.WouldBuy && !capacityRequested && inflightCreate == 0 && underMax(in.MaxHosts, usable, inflightCreate) {
		decisions = append(decisions, Decision{
			Op: OpRequestCapacity, MemoryMiB: buf.ShortfallMiB, Reason: "warm capacity reserve",
		})
	}

	if in.DisableScaleIn {
		return Result{Decisions: decisions, Buffer: buf}, nil
	}
	for _, id := range order {
		h := hosts[id]
		if h.terminating || h.uncertain || h.placements > 0 || h.startN > 0 {
			continue
		}
		if h.EmptySince.IsZero() || in.Now.IsZero() || in.Now.Sub(h.EmptySince) < in.Quiet {
			continue
		}
		if usable <= in.MinHosts {
			break
		}
		if !h.Draining {
			decisions = append(decisions, Decision{
				Op: OpDrainHost, HostID: h.ID, Reason: "empty past quiet period",
			})
		}
		decisions = append(decisions, Decision{
			Op: OpTerminateHost, HostID: h.ID, Reason: "empty past quiet period",
		})
		usable--
	}
	return Result{Decisions: decisions, Buffer: buf}, nil
}

func warmBuffer(in Input, hosts map[string]*hostState, order []string, activeDemand, demandMet bool) *BufferRecommendation {
	if in.WarmMode == WarmOff || in.WarmMode == "" {
		return nil
	}
	spare := 0
	for _, id := range order {
		h := hosts[id]
		if !h.Ready || h.Draining || h.terminating || h.uncertain {
			continue
		}
		free := h.MemoryMiB - h.usedMiB
		if free > 0 {
			spare += free
		}
	}
	rec := &BufferRecommendation{
		Mode: in.WarmMode, ReserveMiB: in.ReserveMiB, ReadySpareMiB: spare,
	}
	if !activeDemand || in.ReserveMiB <= 0 {
		rec.Reason = "no active demand or zero reserve; buffer target is zero"
		return rec
	}
	if !demandMet {
		rec.Reason = "admitted demand is unmet; buffer waits"
		return rec
	}
	if spare >= in.ReserveMiB {
		rec.Reason = "ready spare meets reserve"
		return rec
	}
	rec.ShortfallMiB = in.ReserveMiB - spare
	rec.Reason = "ready spare below reserve"
	if in.WarmMode == WarmActive {
		rec.WouldBuy = true
	}
	return rec
}

func reserves(outcome string) bool {
	switch outcome {
	case "prepared", "dispatched", "unknown":
		return true
	default:
		return false
	}
}

func isCreate(action string) bool {
	return action == "create-host" || action == "create_host"
}

func isTerminate(action string) bool {
	return action == "terminate-host" || action == "terminate_host"
}

func underMax(maxHosts, usable, inflight int) bool {
	return usable+inflight < maxHosts
}

func desired(w Workload) int {
	if w.Lifecycle == "suspended" || w.Lifecycle == "deleting" {
		return 0
	}
	n := w.MinReplicas
	if w.PendingDemand > 0 && n < 1 {
		n = 1
	}
	if n > w.MaxReplicas {
		n = w.MaxReplicas
	}
	if n < 0 {
		return 0
	}
	return n
}

func kept(w Workload, rels []Decision) int {
	drop := map[string]bool{}
	for _, d := range rels {
		drop[d.PlacementID] = true
	}
	n := 0
	for _, r := range w.Replicas {
		if !drop[r.PlacementID] {
			n++
		}
	}
	return n
}

func releases(now time.Time, w Workload) ([]Decision, error) {
	want := desired(w)
	reps := append([]Replica(nil), w.Replicas...)
	sort.Slice(reps, func(i, j int) bool { return reps[i].PlacementID > reps[j].PlacementID })
	var out []Decision
	live := len(reps)
	for _, r := range reps {
		if live <= want {
			break
		}
		if r.Protected || !idleElapsed(now, w.Idle, r.Since) {
			continue
		}
		if r.PlacementID == "" || r.HostID == "" {
			return nil, fmt.Errorf("placement: %s replica missing id", w.ResourceUID)
		}
		out = append(out, Decision{
			Op: OpReleaseReplica, ResourceUID: w.ResourceUID, Name: w.Name,
			HostID: r.HostID, PlacementID: r.PlacementID, MemoryMiB: w.MemoryMiB,
			Reason: "above desired replicas",
		})
		live--
	}
	return out, nil
}

func idleElapsed(now time.Time, idle time.Duration, since time.Time) bool {
	if idle <= 0 || since.IsZero() || now.IsZero() {
		return false
	}
	return !now.Before(since.Add(idle))
}

func bestFit(hosts map[string]*hostState, order []string, mem, cpu int) *hostState {
	var best *hostState
	bestSpare := 0
	for _, id := range order {
		h := hosts[id]
		if !h.Ready || h.Draining || h.uncertain || h.terminating {
			continue
		}
		freeMem := h.MemoryMiB - h.usedMiB
		if freeMem < mem {
			continue
		}
		if cpu > 0 {
			if h.CPU <= 0 || h.CPU-h.usedCPU < cpu {
				continue
			}
		}
		spare := freeMem - mem
		if best == nil || spare < bestSpare || (spare == bestSpare && h.ID < best.ID) {
			best = h
			bestSpare = spare
		}
	}
	return best
}
