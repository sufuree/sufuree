package guest

import (
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
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

func TestReapStopsOnlyOurLeftoverGuest(t *testing.T) {
	q := &QEMU{DataDir: t.TempDir()}
	vol := filepath.Join(q.DataDir, "volumes")
	if err := os.MkdirAll(vol, 0o700); err != nil {
		t.Fatal(err)
	}
	boot := filepath.Join(vol, "w1.boot.raw")
	write := func(p, s string) {
		if err := os.WriteFile(p, []byte(s), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	// A pid that is not ours (its command line names no state volume).
	other := startFake(t, "unrelated")
	write(q.pidfile("w1"), strconv.Itoa(other.Process.Pid))
	if reaped, err := q.Reap("w1"); err != nil || reaped {
		t.Fatalf("reaped a foreign process: %v %v", reaped, err)
	}
	if other.ProcessState != nil {
		t.Fatal("foreign process was signalled")
	}

	// Our leftover guest, with its secrets-bearing boot volume.
	ours := startFake(t, "file="+filepath.Join(vol, "w1.state.raw")+",format=raw")
	exited := make(chan struct{})
	go func() { _, _ = ours.Process.Wait(); close(exited) }()
	write(q.pidfile("w1"), strconv.Itoa(ours.Process.Pid))
	write(boot, "SECRET=1")
	reaped, err := q.Reap("w1")
	if err != nil || !reaped {
		t.Fatalf("reaped=%v err=%v", reaped, err)
	}
	select {
	case <-exited:
	case <-time.After(5 * time.Second):
		t.Fatal("leftover guest still running")
	}
	for _, p := range []string{q.pidfile("w1"), boot} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatalf("%s left behind", p)
		}
	}
	if reaped, err := q.Reap("w1"); err != nil || reaped {
		t.Fatalf("second reap: %v %v", reaped, err)
	}
}
