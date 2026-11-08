// Package resource holds the declarative control-plane records from
// RFC-0001: desired spec, observed status, and the operation, placement,
// host, and activation identities a controller persists before effects.
//
// Decode refuses unknown JSON fields. Validate checks shape only. Neither
// reports a resource ready, nor that an effect has occurred.
package resource

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Kind is the declarative record type.
type Kind string

const (
	KindApp  Kind = "App"
	KindPool Kind = "Pool"
)

func (k Kind) Valid() bool { return k == KindApp || k == KindPool }

// Lifecycle is the caller's availability intention. It is not a phase.
type Lifecycle string

const (
	LifecycleEnabled   Lifecycle = "enabled"
	LifecycleSuspended Lifecycle = "suspended"
	LifecycleDeleting  Lifecycle = "deleting"
)

func (l Lifecycle) Valid() bool {
	switch l {
	case LifecycleEnabled, LifecycleSuspended, LifecycleDeleting:
		return true
	default:
		return false
	}
}

// Phase is observed condition. An observed generation alone is not readiness.
type Phase string

const (
	PhaseDormant          Phase = "dormant"
	PhaseStarting         Phase = "starting"
	PhaseReady            Phase = "ready"
	PhaseDraining         Phase = "draining"
	PhaseBudgetLimited    Phase = "budget_limited"
	PhaseRecoveryRequired Phase = "recovery_required"
)

func (p Phase) Valid() bool {
	switch p {
	case PhaseDormant, PhaseStarting, PhaseReady, PhaseDraining, PhaseBudgetLimited, PhaseRecoveryRequired:
		return true
	default:
		return false
	}
}

// WarmMode is the pool's spare-capacity policy. Only an explicit active
// mode may retain hosts for a buffer; this package does not set a default.
type WarmMode string

const (
	WarmOff     WarmMode = "off"
	WarmObserve WarmMode = "observe"
	WarmActive  WarmMode = "active"
)

func (m WarmMode) Valid() bool {
	switch m {
	case WarmOff, WarmObserve, WarmActive:
		return true
	default:
		return false
	}
}

// Spec is the desired configuration of one resource: an AppSpec or a PoolSpec.
type Spec interface{ isSpec() }

// AppSpec is one workload's desired capacity and wake policy.
type AppSpec struct {
	Image          string   `json:"image"`
	MemoryMiB      int      `json:"memory_mib"`
	GuestPort      int      `json:"guest_port"`
	IdleTimeout    Duration `json:"idle_timeout"`
	StartupTimeout Duration `json:"startup_timeout"`
	MinReplicas    int      `json:"min_replicas"`
	MaxReplicas    int      `json:"max_replicas"`
}

func (AppSpec) isSpec() {}

func (s *AppSpec) UnmarshalJSON(b []byte) error {
	type alias AppSpec
	var raw alias
	if err := decodeStrict(b, &raw); err != nil {
		return err
	}
	*s = AppSpec(raw)
	return nil
}

func (a AppSpec) Validate() error {
	if a.Image == "" || strings.TrimSpace(a.Image) != a.Image || strings.ContainsAny(a.Image, "\r\n\t") {
		return errors.New("app spec: image is required and must be a single line")
	}
	if a.MemoryMiB < 32 {
		return errors.New("app spec: memory_mib must be at least 32")
	}
	if a.GuestPort < 1 || a.GuestPort > 65535 {
		return errors.New("app spec: guest_port is out of range")
	}
	if a.IdleTimeout.Duration <= 0 || a.StartupTimeout.Duration <= 0 {
		return errors.New("app spec: idle_timeout and startup_timeout must be positive")
	}
	if a.MinReplicas < 0 || a.MaxReplicas < a.MinReplicas {
		return errors.New("app spec: min_replicas and max_replicas must satisfy 0 <= min <= max")
	}
	return nil
}

// PoolSpec is one capacity pool's host bounds and warm-capacity mode.
type PoolSpec struct {
	MinHosts    int      `json:"min_hosts"`
	MaxHosts    int      `json:"max_hosts"`
	QuietPeriod Duration `json:"quiet_period"`
	WarmMode    WarmMode `json:"warm_mode"`
	// ReserveMiB is the minimum ready spare memory the warm-capacity policy
	// aims to keep while demand is active. Zero means no buffer target.
	// Observe mode reports shortfall only; active mode may request capacity.
	ReserveMiB int `json:"reserve_mib"`
}

func (PoolSpec) isSpec() {}

func (s *PoolSpec) UnmarshalJSON(b []byte) error {
	type alias PoolSpec
	var raw alias
	if err := decodeStrict(b, &raw); err != nil {
		return err
	}
	*s = PoolSpec(raw)
	return nil
}

func (p PoolSpec) Validate() error {
	if p.MinHosts < 0 || p.MaxHosts < p.MinHosts {
		return errors.New("pool spec: min_hosts and max_hosts must satisfy 0 <= min <= max")
	}
	if p.QuietPeriod.Duration <= 0 {
		return errors.New("pool spec: quiet_period must be positive")
	}
	if !p.WarmMode.Valid() {
		return fmt.Errorf("pool spec: warm_mode %q is not supported", p.WarmMode)
	}
	if p.ReserveMiB < 0 {
		return errors.New("pool spec: reserve_mib must be zero or more")
	}
	return nil
}

// Status is what a controller has observed. The zero status means no
// observation has been recorded.
type Status struct {
	ObservedGeneration uint64    `json:"observed_generation"`
	Phase              Phase     `json:"phase"`
	ReadyReplicas      int       `json:"ready_replicas"`
	Message            string    `json:"message"`
	UpdatedAt          time.Time `json:"updated_at"`
}

func (s *Status) UnmarshalJSON(b []byte) error {
	if bytes.Equal(bytes.TrimSpace(b), []byte("null")) {
		*s = Status{}
		return nil
	}
	type alias Status
	var raw alias
	if err := decodeStrict(b, &raw); err != nil {
		return err
	}
	*s = Status(raw)
	return nil
}

func (s Status) empty() bool {
	return s.Phase == "" && s.ObservedGeneration == 0 && s.ReadyReplicas == 0 && s.Message == "" && s.UpdatedAt.IsZero()
}

func (s Status) Validate() error {
	if s.empty() {
		return nil
	}
	if !s.Phase.Valid() {
		return fmt.Errorf("resource: status phase %q is not supported", s.Phase)
	}
	if s.ObservedGeneration < 1 {
		return errors.New("resource: status observed_generation must be at least 1")
	}
	if s.ReadyReplicas < 0 {
		return errors.New("resource: status ready_replicas must be zero or more")
	}
	if s.UpdatedAt.IsZero() {
		return errors.New("resource: status updated_at is required")
	}
	if strings.ContainsAny(s.Message, "\r\n") || len(s.Message) > 1024 {
		return errors.New("resource: status message must be one line of at most 1024 bytes")
	}
	switch s.Phase {
	case PhaseDormant:
		if s.ReadyReplicas != 0 {
			return errors.New("resource: dormant status cannot report ready replicas")
		}
	case PhaseReady:
		if s.ReadyReplicas < 1 {
			return errors.New("resource: ready status requires at least one ready replica")
		}
	}
	return nil
}

// Resource is one declarative app or pool. Generation changes only when Spec
// changes. ResourceVersion is the compare-and-swap token for the whole record.
type Resource struct {
	UID             string    `json:"uid"`
	Name            string    `json:"name"`
	Generation      uint64    `json:"generation"`
	ResourceVersion uint64    `json:"resource_version"`
	Kind            Kind      `json:"kind"`
	Lifecycle       Lifecycle `json:"lifecycle"`
	Spec            Spec      `json:"spec"`
	Status          Status    `json:"status"`
}

func (r Resource) MarshalJSON() ([]byte, error) {
	specVal := normalizeSpec(r.Spec)
	var spec json.RawMessage
	if specVal == nil {
		spec = []byte("null")
	} else {
		buf, err := json.Marshal(specVal)
		if err != nil {
			return nil, err
		}
		spec = buf
	}
	wire := struct {
		UID             string          `json:"uid"`
		Name            string          `json:"name"`
		Generation      uint64          `json:"generation"`
		ResourceVersion uint64          `json:"resource_version"`
		Kind            Kind            `json:"kind"`
		Lifecycle       Lifecycle       `json:"lifecycle"`
		Spec            json.RawMessage `json:"spec"`
		Status          Status          `json:"status"`
	}{
		UID:             r.UID,
		Name:            r.Name,
		Generation:      r.Generation,
		ResourceVersion: r.ResourceVersion,
		Kind:            r.Kind,
		Lifecycle:       r.Lifecycle,
		Spec:            spec,
		Status:          r.Status,
	}
	return json.Marshal(wire)
}

func (r *Resource) UnmarshalJSON(b []byte) error {
	var wire struct {
		UID             string          `json:"uid"`
		Name            string          `json:"name"`
		Generation      uint64          `json:"generation"`
		ResourceVersion uint64          `json:"resource_version"`
		Kind            Kind            `json:"kind"`
		Lifecycle       Lifecycle       `json:"lifecycle"`
		Spec            json.RawMessage `json:"spec"`
		Status          Status          `json:"status"`
	}
	if err := decodeStrict(b, &wire); err != nil {
		return err
	}
	spec, err := decodeSpec(wire.Kind, wire.Spec)
	if err != nil {
		return err
	}
	*r = Resource{
		UID:             wire.UID,
		Name:            wire.Name,
		Generation:      wire.Generation,
		ResourceVersion: wire.ResourceVersion,
		Kind:            wire.Kind,
		Lifecycle:       wire.Lifecycle,
		Spec:            spec,
		Status:          wire.Status,
	}
	return nil
}

func decodeSpec(kind Kind, raw json.RawMessage) (Spec, error) {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, errors.New("resource: spec is required")
	}
	switch kind {
	case KindApp:
		var spec AppSpec
		if err := decodeStrict(raw, &spec); err != nil {
			return nil, fmt.Errorf("resource: app spec: %w", err)
		}
		return spec, nil
	case KindPool:
		var spec PoolSpec
		if err := decodeStrict(raw, &spec); err != nil {
			return nil, fmt.Errorf("resource: pool spec: %w", err)
		}
		return spec, nil
	default:
		return nil, fmt.Errorf("resource: kind %q is not supported", kind)
	}
}

func normalizeSpec(s Spec) Spec {
	switch v := s.(type) {
	case nil:
		return nil
	case AppSpec:
		return v
	case *AppSpec:
		if v == nil {
			return nil
		}
		return *v
	case PoolSpec:
		return v
	case *PoolSpec:
		if v == nil {
			return nil
		}
		return *v
	default:
		return v
	}
}

// Normalize returns r with a concrete AppSpec or PoolSpec value, so a pointer
// and a value compare as the same configuration.
func Normalize(r Resource) Resource {
	r.Spec = normalizeSpec(r.Spec)
	return r
}

func (r Resource) Validate() error {
	if err := checkID("uid", r.UID); err != nil {
		return err
	}
	if err := checkID("name", r.Name); err != nil {
		return err
	}
	if r.Generation < 1 {
		return errors.New("resource: generation must be at least 1")
	}
	if !r.Lifecycle.Valid() {
		return fmt.Errorf("resource: lifecycle %q is not supported", r.Lifecycle)
	}
	if err := r.Status.Validate(); err != nil {
		return err
	}
	spec := normalizeSpec(r.Spec)
	var max int
	switch r.Kind {
	case KindApp:
		app, ok := spec.(AppSpec)
		if !ok {
			return errors.New("resource: app spec is required")
		}
		if err := app.Validate(); err != nil {
			return err
		}
		max = app.MaxReplicas
	case KindPool:
		pool, ok := spec.(PoolSpec)
		if !ok {
			return errors.New("resource: pool spec is required")
		}
		if err := pool.Validate(); err != nil {
			return err
		}
		max = pool.MaxHosts
	default:
		return fmt.Errorf("resource: kind %q is not supported", r.Kind)
	}
	if !r.Status.empty() && r.Status.ReadyReplicas > max {
		return fmt.Errorf("resource: ready_replicas %d exceeds the spec maximum %d", r.Status.ReadyReplicas, max)
	}
	return nil
}
