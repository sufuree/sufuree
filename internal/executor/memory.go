package executor

import (
	"context"
	"fmt"
	"sync"
)

// Memory is an in-process executor for planner tests and the explicit
// `sufuree controller --executor memory` development path.
//
// AllowEffects defaults to false. While it is false, CreateHost and
// TerminateHost return ErrEffectsDisabled and change nothing. Unimplemented
// effects always return an error, even when AllowEffects is true.
type Memory struct {
	AllowEffects bool

	mu    sync.Mutex
	hosts map[string]*memHost // provider ID
	byOp  map[string]string   // create operation ID → provider ID
	term  map[string]string   // terminate operation ID → provider ID
}

type memHost struct {
	desc HostDesc
}

var _ Executor = (*Memory)(nil)

// Describe returns a host by provider ID, host ID or create operation ID.
func (m *Memory) Describe(_ context.Context, id string) (HostDesc, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if h, ok := m.hosts[id]; ok {
		return cloneDesc(h.desc), nil
	}
	for _, h := range m.hosts {
		if h.desc.ID == id || h.desc.OperationID == id {
			return cloneDesc(h.desc), nil
		}
	}
	if pid, ok := m.byOp[id]; ok {
		return cloneDesc(m.hosts[pid].desc), nil
	}
	return HostDesc{}, ErrNotFound
}

// CreateHost records one running host when effects are enabled. The same
// OperationID returns the original provider identity.
func (m *Memory) CreateHost(_ context.Context, in CreateHostInput) (ProviderRef, error) {
	if !m.AllowEffects {
		return ProviderRef{}, ErrEffectsDisabled
	}
	if in.OperationID == "" {
		return ProviderRef{}, fmt.Errorf("executor: create host: operation id required")
	}
	if in.MemoryMiB <= 0 {
		return ProviderRef{}, fmt.Errorf("executor: create host %s: memory_mib required", in.OperationID)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensure()
	if pid, ok := m.byOp[in.OperationID]; ok {
		h := m.hosts[pid]
		return refOf(h.desc), nil
	}
	pid := "mem-" + in.OperationID
	desc := HostDesc{
		ID:          "host-" + in.OperationID,
		ProviderID:  pid,
		Shape:       in.Shape,
		MemoryMiB:   in.MemoryMiB,
		CPU:         in.CPU,
		State:       "running",
		OperationID: in.OperationID,
		Tags:        copyTags(in.Tags),
	}
	m.hosts[pid] = &memHost{desc: desc}
	m.byOp[in.OperationID] = pid
	return refOf(desc), nil
}

// TerminateHost marks a host terminated when effects are enabled. Repeating
// the same operation ID is a no-op success. A different operation for an
// already terminated host does not resurrect it.
func (m *Memory) TerminateHost(_ context.Context, in TerminateHostInput) error {
	if !m.AllowEffects {
		return ErrEffectsDisabled
	}
	if in.OperationID == "" {
		return fmt.Errorf("executor: terminate host: operation id required")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ensure()
	if pid, ok := m.term[in.OperationID]; ok {
		if m.hosts[pid].desc.State != "terminated" {
			return fmt.Errorf("executor: terminate %s: recorded host is not terminated", in.OperationID)
		}
		return nil
	}
	h := m.lookup(in.ProviderID, in.HostID)
	if h == nil {
		return fmt.Errorf("executor: terminate host %s: %w", in.ProviderID, ErrNotFound)
	}
	h.desc.State = "terminated"
	m.term[in.OperationID] = h.desc.ProviderID
	return nil
}

// AttachVolume is not implemented.
func (m *Memory) AttachVolume(context.Context, AttachVolumeInput) error {
	return notImplemented("attach_volume")
}

// DetachVolume is not implemented.
func (m *Memory) DetachVolume(context.Context, DetachVolumeInput) error {
	return notImplemented("detach_volume")
}

// CreateSnapshot is not implemented.
func (m *Memory) CreateSnapshot(context.Context, SnapshotInput) error {
	return notImplemented("create_snapshot")
}

// RestoreSnapshot is not implemented.
func (m *Memory) RestoreSnapshot(context.Context, RestoreInput) error {
	return notImplemented("restore_snapshot")
}

// ExpireSnapshot is not implemented.
func (m *Memory) ExpireSnapshot(context.Context, ExpireInput) error {
	return notImplemented("expire_snapshot")
}

// Running lists hosts whose state is running. It is an observation helper
// for tests and does not create or alter hosts.
func (m *Memory) Running() []HostDesc {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []HostDesc
	for _, h := range m.hosts {
		if h.desc.State == "running" {
			out = append(out, cloneDesc(h.desc))
		}
	}
	return out
}

func (m *Memory) ensure() {
	if m.hosts == nil {
		m.hosts = map[string]*memHost{}
		m.byOp = map[string]string{}
		m.term = map[string]string{}
	}
}

func (m *Memory) lookup(providerID, hostID string) *memHost {
	if providerID != "" {
		if h, ok := m.hosts[providerID]; ok {
			return h
		}
	}
	if hostID != "" {
		for _, h := range m.hosts {
			if h.desc.ID == hostID {
				return h
			}
		}
	}
	return nil
}

func refOf(d HostDesc) ProviderRef {
	return ProviderRef{ProviderID: d.ProviderID, Shape: d.Shape, MemoryMiB: d.MemoryMiB, CPU: d.CPU}
}

func cloneDesc(d HostDesc) HostDesc {
	d.Tags = copyTags(d.Tags)
	return d
}

func copyTags(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
