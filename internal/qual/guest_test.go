package qual_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sufuree/sufuree/internal/activator"
	"github.com/sufuree/sufuree/internal/edge"
	"github.com/sufuree/sufuree/internal/guest"
	"github.com/sufuree/sufuree/internal/journal"
	"github.com/sufuree/sufuree/internal/workload"
)

// TestFixedHostGuestWakeProxyIdle is RFC sequence §2 on a fixed host with no
// AWS spend: edge admit → wake HTTP guest → proxy → idle stop → rewake with a
// new generation. guest.HTTP stands in for the pinned QEMU/KVM image so CI
// can run without ops/KVM; the Launcher contract is the same.
func TestFixedHostGuestWakeProxyIdle(t *testing.T) {
	j, err := journal.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	spec := workload.Spec{
		ID: "web", Image: "http-qual", MemoryMiB: 64, GuestPort: 8080, StateMiB: 8,
		BootEnvFile: "env", GenerationEnvKey: "GEN",
		IdleTimeout:    workload.Duration{Duration: 80 * time.Millisecond},
		StartupTimeout: workload.Duration{Duration: time.Second},
	}
	act := activator.New(spec, j, &guest.HTTP{}, nil, activator.Options{
		Tick: 20 * time.Millisecond, StopGrace: 200 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go act.Run(ctx)
	t.Cleanup(func() { cancel(); <-act.Done() })

	edgeSrv := httptest.NewServer(&edge.Server{
		Activator: &edge.HostWake{Service: "web", Act: act, Timeout: 3 * time.Second},
	})
	t.Cleanup(edgeSrv.Close)

	if st, _, _, _ := act.Snapshot(); st != activator.Asleep {
		t.Fatalf("initial state = %s", st)
	}

	res, err := http.Get(edgeSrv.URL + "/svc/web/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 200 || string(body) != "ok\n" || res.Header.Get("X-Generation") != "1" {
		t.Fatalf("wake1 status=%d body=%q gen=%q", res.StatusCode, body, res.Header.Get("X-Generation"))
	}

	deadline := time.Now().Add(3 * time.Second)
	for {
		st, gen, _, _ := act.Snapshot()
		if st == activator.Asleep && gen == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("idle stop: state=%s gen=%d", st, gen)
		}
		time.Sleep(10 * time.Millisecond)
	}

	res, err = http.Get(edgeSrv.URL + "/svc/web/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ = io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode != 200 || res.Header.Get("X-Generation") != "2" {
		t.Fatalf("rewake status=%d body=%q gen=%q", res.StatusCode, body, res.Header.Get("X-Generation"))
	}
	if r, err := j.Get("web"); err != nil || r.Generation != 2 {
		t.Fatalf("journal generation = %+v err=%v", r, err)
	}

	hz, err := http.Get(edgeSrv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	hz.Body.Close()
	if hz.StatusCode != 200 {
		t.Fatalf("healthz = %d", hz.StatusCode)
	}
	if st, _, _, _ := act.Snapshot(); st == activator.Waking {
		t.Fatal("healthz woke the guest")
	}

	receipt := map[string]any{
		"scenario":     "fixed-host-guest-wake-proxy-idle",
		"aws_spend":    false,
		"launcher":     "guest.HTTP",
		"qemu_pin":     "deferred-to-live-host",
		"wake_gen":     1,
		"rewake_gen":   2,
		"qualified_at": time.Now().UTC().Format(time.RFC3339),
	}
	if dir := os.Getenv("SUFUREE_QUAL_RECEIPT_DIR"); dir != "" {
		writeGuestReceipt(t, dir, receipt)
	}
}

func writeGuestReceipt(t *testing.T, dir string, receipt map[string]any) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "fixed-host-guest-wake-proxy-idle.json")
	buf, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(buf, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}
