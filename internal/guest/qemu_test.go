package guest

import (
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// startFake runs a process whose command line names arg, as QEMU's names
// the state volume.
func startFake(t *testing.T, arg string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command("sh", "-c", "sleep 30; :", "sh", arg)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	return cmd
}

func TestReapStopsOnlyOurLeftoverGuests(t *testing.T) {
	q := &QEMU{DataDir: t.TempDir()}
	vol := filepath.Join(q.DataDir, "volumes")
	if err := os.MkdirAll(vol, 0o700); err != nil {
		t.Fatal(err)
	}
	boot := filepath.Join(vol, "w1.boot.raw")
	if err := os.WriteFile(boot, []byte("SECRET=1"), 0o600); err != nil {
		t.Fatal(err)
	}
	other := startFake(t, "file="+filepath.Join(vol, "w2.state.raw")+",format=raw")
	ours := startFake(t, "file="+filepath.Join(vol, "w1.state.raw")+",format=raw")
	exited := make(chan struct{})
	go func() { _, _ = ours.Process.Wait(); close(exited) }()

	n, err := q.Reap("w1")
	if err != nil || n != 1 {
		t.Fatalf("reaped %d, err %v; want 1", n, err)
	}
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("leftover guest still running")
	}
	if err := other.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("another workload's guest was killed: %v", err)
	}
	if _, err := os.Stat(boot); !os.IsNotExist(err) {
		t.Fatal("boot volume left behind")
	}
	if n, err := q.Reap("w1"); err != nil || n != 0 {
		t.Fatalf("second reap: %d %v", n, err)
	}
}
