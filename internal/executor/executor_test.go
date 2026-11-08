package executor

import (
	"context"
	"errors"
	"testing"
)

func TestMemoryEffectsFailClosed(t *testing.T) {
	t.Parallel()
	m := &Memory{}
	ctx := context.Background()
	_, err := m.CreateHost(ctx, CreateHostInput{OperationID: "op1", MemoryMiB: 256})
	if !errors.Is(err, ErrEffectsDisabled) {
		t.Fatalf("CreateHost err = %v", err)
	}
	if err := m.TerminateHost(ctx, TerminateHostInput{OperationID: "op2", ProviderID: "mem-op1"}); !errors.Is(err, ErrEffectsDisabled) {
		t.Fatalf("TerminateHost err = %v", err)
	}
	if _, err := m.Describe(ctx, "mem-op1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Describe err = %v", err)
	}
	if len(m.Running()) != 0 {
		t.Fatalf("disabled executor recorded hosts: %+v", m.Running())
	}
}

func TestMemoryCreateIsIdempotentAndDescribable(t *testing.T) {
	t.Parallel()
	m := &Memory{AllowEffects: true}
	ctx := context.Background()
	in := CreateHostInput{OperationID: "op1", Shape: "test", MemoryMiB: 512, CPU: 1, Tags: map[string]string{"pool": "a"}}
	ref, err := m.CreateHost(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	again, err := m.CreateHost(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if again != ref {
		t.Fatalf("repeat create = %+v, want %+v", again, ref)
	}
	got, err := m.Describe(ctx, ref.ProviderID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != "running" || got.MemoryMiB != 512 || got.Tags["pool"] != "a" {
		t.Fatalf("desc = %+v", got)
	}
	byOp, err := m.Describe(ctx, "op1")
	if err != nil || byOp.ProviderID != ref.ProviderID {
		t.Fatalf("describe by op: %+v %v", byOp, err)
	}
	other, err := m.CreateHost(ctx, CreateHostInput{OperationID: "op2", MemoryMiB: 128})
	if err != nil {
		t.Fatal(err)
	}
	if other.ProviderID == ref.ProviderID {
		t.Fatal("second operation reused provider id")
	}
	if len(m.Running()) != 2 {
		t.Fatalf("running = %d", len(m.Running()))
	}
}

func TestMemoryTerminate(t *testing.T) {
	t.Parallel()
	m := &Memory{AllowEffects: true}
	ctx := context.Background()
	ref, err := m.CreateHost(ctx, CreateHostInput{OperationID: "c1", MemoryMiB: 64})
	if err != nil {
		t.Fatal(err)
	}
	in := TerminateHostInput{OperationID: "t1", ProviderID: ref.ProviderID}
	if err := m.TerminateHost(ctx, in); err != nil {
		t.Fatal(err)
	}
	if err := m.TerminateHost(ctx, in); err != nil {
		t.Fatal(err)
	}
	got, err := m.Describe(ctx, ref.ProviderID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != "terminated" {
		t.Fatalf("state = %s", got.State)
	}
	if len(m.Running()) != 0 {
		t.Fatalf("running after terminate: %+v", m.Running())
	}
	err = m.TerminateHost(ctx, TerminateHostInput{OperationID: "t-missing", ProviderID: "nope"})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing terminate err = %v", err)
	}
}

func TestUnimplementedEffects(t *testing.T) {
	t.Parallel()
	m := &Memory{AllowEffects: true}
	ctx := context.Background()
	cases := []struct {
		name string
		err  error
	}{
		{"attach_volume", m.AttachVolume(ctx, AttachVolumeInput{OperationID: "a"})},
		{"detach_volume", m.DetachVolume(ctx, DetachVolumeInput{OperationID: "a"})},
		{"create_snapshot", m.CreateSnapshot(ctx, SnapshotInput{OperationID: "a"})},
		{"restore_snapshot", m.RestoreSnapshot(ctx, RestoreInput{OperationID: "a"})},
		{"expire_snapshot", m.ExpireSnapshot(ctx, ExpireInput{OperationID: "a"})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !errors.Is(tc.err, ErrNotImplemented) {
				t.Fatalf("err = %v", tc.err)
			}
		})
	}
}

func TestCreateRequiresMemoryAndOperation(t *testing.T) {
	t.Parallel()
	m := &Memory{AllowEffects: true}
	ctx := context.Background()
	if _, err := m.CreateHost(ctx, CreateHostInput{MemoryMiB: 1}); err == nil {
		t.Fatal("missing operation id succeeded")
	}
	if _, err := m.CreateHost(ctx, CreateHostInput{OperationID: "x"}); err == nil {
		t.Fatal("missing memory succeeded")
	}
}
