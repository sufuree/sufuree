package resource

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
	"time"
)

const digest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

var (
	t0 = time.Date(2026, 11, 2, 15, 4, 5, 0, time.UTC)
	t1 = t0.Add(time.Minute)
)

func validApp() Resource {
	return Resource{
		UID:        "app-1",
		Name:       "api",
		Generation: 1,
		Kind:       KindApp,
		Lifecycle:  LifecycleEnabled,
		Spec: AppSpec{
			Image:          "/img/api",
			MemoryMiB:      512,
			GuestPort:      8080,
			IdleTimeout:    Duration{Duration: 60 * time.Second},
			StartupTimeout: Duration{Duration: 20 * time.Second},
			MinReplicas:    0,
			MaxReplicas:    4,
		},
		Status: Status{
			ObservedGeneration: 1,
			Phase:              PhaseDormant,
			ReadyReplicas:      0,
			Message:            "idle",
			UpdatedAt:          t0,
		},
	}
}

func validPool() Resource {
	return Resource{
		UID:        "pool-1",
		Name:       "edge",
		Generation: 1,
		Kind:       KindPool,
		Lifecycle:  LifecycleEnabled,
		Spec: PoolSpec{
			MinHosts:    0,
			MaxHosts:    2,
			QuietPeriod: Duration{Duration: 300 * time.Second},
			WarmMode:    WarmObserve,
		},
		Status: Status{
			ObservedGeneration: 1,
			Phase:              PhaseReady,
			ReadyReplicas:      1,
			UpdatedAt:          t0,
		},
	}
}

func validOperation() Operation {
	return Operation{
		OperationID:        "op-1",
		ResourceUID:        "app-1",
		OwnerEpoch:         1,
		ExpectedGeneration: 1,
		Action:             "provision-host",
		InputDigest:        digest,
		Outcome:            OutcomePrepared,
		CreatedAt:          t0,
		UpdatedAt:          t0,
	}
}

func validPlacement() Placement {
	return Placement{
		UID:         "place-1",
		ResourceUID: "app-1",
		HostUID:     "host-1",
		Replica:     "rep-1",
		VolumeUID:   "vol-1",
		Generation:  1,
		OwnerEpoch:  1,
		MemoryMiB:   512,
		Phase:       PhaseStarting,
		UpdatedAt:   t0,
	}
}

func validHost() Host {
	return Host{
		UID:        "host-1",
		PoolUID:    "pool-1",
		ProviderID: "i-abc123",
		MemoryMiB:  2048,
		Ready:      false,
		Draining:   false,
		ObservedAt: t0,
	}
}

func validClaim() ActivationClaim {
	return ActivationClaim{
		UID:         "claim-1",
		ResourceUID: "app-1",
		Generation:  1,
		Count:       2,
		CreatedAt:   t0,
		Deadline:    t1,
	}
}

func validMeta() ClusterMeta {
	return ClusterMeta{DeploymentUID: "dep-1", RecoveryIncarnation: 1, OwnerEpoch: 1}
}

func TestValidate(t *testing.T) {
	app := validApp()
	pool := validPool()
	cases := []struct {
		name string
		fn   func() error
	}{
		{"app", func() error { return app.Validate() }},
		{"pool", func() error { return pool.Validate() }},
		{"operation", func() error { return validOperation().Validate() }},
		{"placement", func() error { return validPlacement().Validate() }},
		{"host", func() error { return validHost().Validate() }},
		{"claim", func() error { return validClaim().Validate() }},
		{"meta", func() error { return validMeta().Validate() }},
		{"zero status", func() error {
			r := app
			r.Status = Status{}
			return r.Validate()
		}},
		{"bad uid", func() error {
			r := app
			r.UID = "App"
			return r.Validate()
		}},
		{"generation zero", func() error {
			r := app
			r.Generation = 0
			return r.Validate()
		}},
		{"bad lifecycle", func() error {
			r := app
			r.Lifecycle = "paused"
			return r.Validate()
		}},
		{"kind spec mismatch", func() error {
			r := app
			r.Kind = KindPool
			return r.Validate()
		}},
		{"missing spec", func() error {
			r := app
			r.Spec = nil
			return r.Validate()
		}},
		{"small memory", func() error {
			r := app
			spec := r.Spec.(AppSpec)
			spec.MemoryMiB = 8
			r.Spec = spec
			return r.Validate()
		}},
		{"bad port", func() error {
			r := app
			spec := r.Spec.(AppSpec)
			spec.GuestPort = 0
			r.Spec = spec
			return r.Validate()
		}},
		{"zero idle", func() error {
			r := app
			spec := r.Spec.(AppSpec)
			spec.IdleTimeout = Duration{}
			r.Spec = spec
			return r.Validate()
		}},
		{"negative timeout", func() error {
			r := app
			spec := r.Spec.(AppSpec)
			spec.StartupTimeout = Duration{Duration: -time.Second}
			r.Spec = spec
			return r.Validate()
		}},
		{"min above max", func() error {
			r := app
			spec := r.Spec.(AppSpec)
			spec.MinReplicas = 3
			spec.MaxReplicas = 1
			r.Spec = spec
			return r.Validate()
		}},
		{"bad phase", func() error {
			r := app
			r.Status.Phase = "asleep"
			return r.Validate()
		}},
		{"status missing time", func() error {
			r := app
			r.Status.UpdatedAt = time.Time{}
			return r.Validate()
		}},
		{"dormant with replicas", func() error {
			r := app
			r.Status.ReadyReplicas = 1
			return r.Validate()
		}},
		{"ready without replicas", func() error {
			r := pool
			r.Status.ReadyReplicas = 0
			return r.Validate()
		}},
		{"ready above max", func() error {
			r := app
			r.Status.Phase = PhaseReady
			r.Status.ReadyReplicas = 9
			return r.Validate()
		}},
		{"bad warm mode", func() error {
			r := pool
			spec := r.Spec.(PoolSpec)
			spec.WarmMode = "auto"
			r.Spec = spec
			return r.Validate()
		}},
		{"hosts min above max", func() error {
			r := pool
			spec := r.Spec.(PoolSpec)
			spec.MinHosts = 3
			spec.MaxHosts = 1
			r.Spec = spec
			return r.Validate()
		}},
		{"zero quiet period", func() error {
			r := pool
			spec := r.Spec.(PoolSpec)
			spec.QuietPeriod = Duration{}
			r.Spec = spec
			return r.Validate()
		}},
		{"short digest", func() error {
			op := validOperation()
			op.InputDigest = "abcd"
			return op.Validate()
		}},
		{"uppercase digest", func() error {
			op := validOperation()
			op.InputDigest = strings.ToUpper(digest)
			return op.Validate()
		}},
		{"bad action", func() error {
			op := validOperation()
			op.Action = "Provision"
			return op.Validate()
		}},
		{"bad outcome", func() error {
			op := validOperation()
			op.Outcome = "done"
			return op.Validate()
		}},
		{"claim deadline", func() error {
			c := validClaim()
			c.Deadline = c.CreatedAt
			return c.Validate()
		}},
		{"claim count", func() error {
			c := validClaim()
			c.Count = 0
			return c.Validate()
		}},
		{"meta epoch", func() error {
			m := validMeta()
			m.OwnerEpoch = 0
			return m.Validate()
		}},
		{"host memory", func() error {
			h := validHost()
			h.MemoryMiB = 16
			return h.Validate()
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.fn()
			ok := strings.HasPrefix(tc.name, "app") || tc.name == "pool" || tc.name == "operation" ||
				tc.name == "placement" || tc.name == "host" || tc.name == "claim" || tc.name == "meta" ||
				tc.name == "zero status"
			if ok && err != nil {
				t.Fatal(err)
			}
			if !ok && err == nil {
				t.Fatal("accepted")
			}
		})
	}
}

func TestOperationOpen(t *testing.T) {
	cases := []struct {
		outcome Outcome
		open    bool
		closed  bool
	}{
		{OutcomePrepared, true, false},
		{OutcomeDispatched, true, false},
		{OutcomeUnknown, true, false},
		{OutcomeConfirmed, false, true},
		{OutcomeFailed, false, true},
		{"done", false, false},
	}
	for _, tc := range cases {
		op := Operation{Outcome: tc.outcome}
		if op.Open() != tc.open || op.Terminal() != tc.closed {
			t.Errorf("%s open=%v terminal=%v", tc.outcome, op.Open(), op.Terminal())
		}
	}
}

func TestRoundTrip(t *testing.T) {
	values := []any{validApp(), validPool(), validOperation(), validPlacement(), validHost(), validClaim(), validMeta()}
	for _, in := range values {
		b, err := Encode(in)
		if err != nil {
			t.Fatal(err)
		}
		dst := reflect.New(reflect.TypeOf(in))
		if err := Decode(b, dst.Interface()); err != nil {
			t.Fatalf("%T: %v\n%s", in, err, b)
		}
		again, err := Encode(dst.Elem().Interface())
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(b, again) {
			t.Fatalf("%T re-encode\n%s\n%s", in, b, again)
		}
	}
}

func TestRejectsUnknownFields(t *testing.T) {
	cases := []struct {
		name string
		body []byte
		dst  any
	}{
		{"resource", withExtra(t, validApp()), &Resource{}},
		{"app spec", withExtra(t, validApp().Spec), &AppSpec{}},
		{"pool spec", withExtra(t, validPool().Spec), &PoolSpec{}},
		{"operation", withExtra(t, validOperation()), &Operation{}},
		{"placement", withExtra(t, validPlacement()), &Placement{}},
		{"host", withExtra(t, validHost()), &Host{}},
		{"claim", withExtra(t, validClaim()), &ActivationClaim{}},
		{"meta", withExtra(t, validMeta()), &ClusterMeta{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := Decode(tc.body, tc.dst); err == nil {
				t.Fatal("accepted unknown field")
			}
		})
	}
}

func TestNestedSpecRejectsUnknownFields(t *testing.T) {
	b, err := Encode(validApp())
	if err != nil {
		t.Fatal(err)
	}
	b = bytes.Replace(b, []byte(`"image":`), []byte(`"extra":true,"image":`), 1)
	var got Resource
	if err := Decode(b, &got); err == nil {
		t.Fatal("accepted unknown spec field")
	}
}

func TestDurationRejectsNonString(t *testing.T) {
	b, err := Encode(validApp())
	if err != nil {
		t.Fatal(err)
	}
	b = bytes.Replace(b, []byte(`"idle_timeout":"1m0s"`), []byte(`"idle_timeout":60`), 1)
	var got Resource
	if err := Decode(b, &got); err == nil {
		t.Fatal("accepted numeric timeout")
	}
}

func TestPoolSpecIsNotAnApp(t *testing.T) {
	b, err := Encode(validPool())
	if err != nil {
		t.Fatal(err)
	}
	b = bytes.Replace(b, []byte(`"kind":"Pool"`), []byte(`"kind":"App"`), 1)
	var got Resource
	if err := Decode(b, &got); err == nil {
		t.Fatal("decoded a pool spec as an app")
	}
}

func withExtra(t *testing.T, v any) []byte {
	t.Helper()
	b, err := Encode(v)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) == 0 || b[0] != '{' {
		t.Fatalf("not an object: %s", b)
	}
	return append([]byte(`{"unexpected":true,`), b[1:]...)
}
