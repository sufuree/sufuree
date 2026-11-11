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
	"github.com/sufuree/sufuree/internal/qual/qemupin"
	"github.com/sufuree/sufuree/internal/workload"
)

// TestFixedHostQEMUWakeProxyIdle is the live QEMU/KVM twin of
// TestFixedHostGuestWakeProxyIdle. It is skipped unless SUFUREE_QUAL_QEMU=1
// and a digest-checkable guest image is provided (see
// docs/qualification/rfc-0001-qemu-pin.md). No AWS spend.
func TestFixedHostQEMUWakeProxyIdle(t *testing.T) {
	if !qemupin.Enabled() {
		t.Skip("set SUFUREE_QUAL_QEMU=1 with SUFUREE_QUAL_GUEST_IMAGE to run live QEMU qual")
	}
	resv, err := qemupin.Resolve()
	if err != nil {
		t.Fatal(err)
	}

	dataDir := t.TempDir()
	j, err := journal.Open(filepath.Join(dataDir, "journal"))
	if err != nil {
		t.Fatal(err)
	}
	spec := workload.Spec{
		ID: "web", Image: resv.ImagePath, MemoryMiB: 256, GuestPort: resv.Pin.Image.GuestPort,
		StateMiB: 64, BootEnvFile: "env", GenerationEnvKey: "GEN",
		IdleTimeout:    workload.Duration{Duration: 2 * time.Second},
		StartupTimeout: workload.Duration{Duration: 60 * time.Second},
	}
	launcher := &guest.QEMU{
		DataDir: dataDir,
		QEMUBin: resv.Pin.VMM.Bin,
		OpsBin:  resv.Pin.Ops.Bin,
	}
	act := activator.New(spec, j, launcher, nil, activator.Options{
		Tick: 200 * time.Millisecond, StopGrace: 5 * time.Second,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go act.Run(ctx)
	t.Cleanup(func() { cancel(); <-act.Done() })

	edgeSrv := httptest.NewServer(&edge.Server{
		Activator: &edge.HostWake{Service: "web", Act: act, Timeout: 90 * time.Second},
	})
	t.Cleanup(edgeSrv.Close)
	client := &http.Client{Timeout: 90 * time.Second}

	t0 := time.Now()
	res, err := client.Get(edgeSrv.URL + "/svc/web/")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(res.Body)
	res.Body.Close()
	wakeMS := time.Since(t0).Milliseconds()
	if res.StatusCode != 200 {
		t.Fatalf("wake1 status=%d body=%q", res.StatusCode, body)
	}

	deadline := time.Now().Add(30 * time.Second)
	for {
		st, gen, _, _ := act.Snapshot()
		if st == activator.Asleep && gen >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("idle stop: state=%s gen=%d", st, gen)
		}
		time.Sleep(50 * time.Millisecond)
	}

	hz, err := client.Get(edgeSrv.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	hz.Body.Close()
	if hz.StatusCode != 200 {
		t.Fatalf("healthz = %d", hz.StatusCode)
	}
	if st, gen, _, _ := act.Snapshot(); st != activator.Asleep {
		t.Fatalf("healthz disturbed guest: state=%s gen=%d", st, gen)
	}

	t1 := time.Now()
	res, err = client.Get(edgeSrv.URL + "/svc/web/")
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	rewakeMS := time.Since(t1).Milliseconds()
	if res.StatusCode != 200 {
		t.Fatalf("rewake status=%d", res.StatusCode)
	}
	rec, err := j.Get("web")
	if err != nil || rec.Generation < 2 {
		t.Fatalf("journal generation = %+v err=%v", rec, err)
	}

	receipt := map[string]any{
		"scenario":       "fixed-host-qemu-wake-proxy-idle",
		"aws_spend":      false,
		"launcher":       "guest.QEMU",
		"image_path":     resv.ImagePath,
		"image_sha256":   resv.ImageSHA256,
		"qemu_version":   resv.QEMUVersion,
		"ops_version":    resv.OpsVersion,
		"kvm":            resv.KVM,
		"arch":           resv.Pin.Arch,
		"runtime":        resv.Pin.Image.Runtime,
		"wake_ms":        wakeMS,
		"rewake_ms":      rewakeMS,
		"wake_gen":       1,
		"rewake_gen":     rec.Generation,
		"qualified_at":   time.Now().UTC().Format(time.RFC3339),
		"pin_sha256_set": resv.Pin.Image.SHA256 != "",
	}
	if dir := os.Getenv("SUFUREE_QUAL_RECEIPT_DIR"); dir != "" {
		writeQEMUReceipt(t, dir, receipt)
	}
}

func writeQEMUReceipt(t *testing.T, dir string, receipt map[string]any) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "fixed-host-qemu-wake-proxy-idle.json")
	buf, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(buf, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}
