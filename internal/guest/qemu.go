// Package guest boots and stops unikernel guests under QEMU/KVM.
//
// Each workload has a persistent state volume (label "state") on host disk
// and gets a fresh boot volume (label "boot") per boot carrying its per-boot
// environment file, because a Nanos image's argv and environment are fixed
// when the image is built. The shared image is opened with snapshot=on so
// guests never write to it. Volumes are built with the Ops CLI (TFS format).
//
// Networking is QEMU user mode with one host-loopback port forwarded to the
// workload's guest port; the gateway is the only intended client of that
// port. Production isolation (tap/bridge or vsock with per-guest policy) is
// a later qualification.
package guest

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/sufuree/sufuree/internal/workload"
)

// Guest is one running boot.
type Guest interface {
	// Addr is the host address that reaches the workload's guest port.
	Addr() string
	// Exited closes when the guest process ends for any reason.
	Exited() <-chan struct{}
	// Stop ends the guest (graceful signal, then kill after grace).
	Stop(grace time.Duration) error
}

// Launcher boots guests.
type Launcher interface {
	Boot(ctx context.Context, w workload.Spec, generation uint64, env map[string]string, files map[string][]byte) (Guest, error)
}

// QEMU is the KVM launcher.
type QEMU struct {
	DataDir string
	QEMUBin string // default qemu-system-x86_64
	OpsBin  string // default ops
	// OpsHome is Ops's state directory (default $HOME/.ops); volumes it
	// creates are moved out of <OpsHome>/volumes immediately.
	OpsHome string

	volMu sync.Mutex // ops volume names are global; serialize creation
}

func (q *QEMU) bin(v, def string) string {
	if v != "" {
		return v
	}
	return def
}

func (q *QEMU) opsHome() string {
	if q.OpsHome != "" {
		return q.OpsHome
	}
	h, _ := os.UserHomeDir()
	return filepath.Join(h, ".ops")
}

// createVolume builds a TFS volume labelled label from srcDir and moves it
// to dst. Ops registers volumes by name in a shared directory (the user's
// Ops store, which other tools also use), so a leftover volume with the
// same label is moved out of the way first. It is moved into the agent's
// private quarantine directory, not deleted: it may be an interrupted boot
// volume holding secrets, or another tool's volume. Nothing empties the
// quarantine (0700); an operator reviews and removes its contents.
func (q *QEMU) createVolume(ctx context.Context, label, srcDir string, sizeMiB int, dst string) error {
	q.volMu.Lock()
	defer q.volMu.Unlock()
	vdir := filepath.Join(q.opsHome(), "volumes")
	old, _ := filepath.Glob(filepath.Join(vdir, label+":*.raw"))
	if len(old) > 0 {
		quarantine := filepath.Join(q.DataDir, "volumes.quarantine")
		if err := os.MkdirAll(quarantine, 0o700); err != nil {
			return err
		}
		for _, f := range old {
			if err := os.Rename(f, filepath.Join(quarantine, filepath.Base(f))); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("leftover ops volume %s blocks volume creation: %w", filepath.Base(f), err)
			}
		}
	}
	cmd := exec.CommandContext(ctx, q.bin(q.OpsBin, "ops"), "volume", "create", label, "-d", srcDir, "-s", fmt.Sprintf("%dm", sizeMiB))
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("ops volume create %s: %w: %s", label, err, strings.TrimSpace(string(out)))
	}
	made, _ := filepath.Glob(filepath.Join(vdir, label+":*.raw"))
	if len(made) != 1 {
		return fmt.Errorf("ops volume create %s: expected one volume, found %d", label, len(made))
	}
	if err := os.Chmod(made[0], 0o600); err != nil {
		return err
	}
	return os.Rename(made[0], dst)
}

func (q *QEMU) dirs(id string) (vol, logs string, err error) {
	vol = filepath.Join(q.DataDir, "volumes")
	logs = filepath.Join(q.DataDir, "logs")
	for _, d := range []string{vol, logs} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			return "", "", err
		}
	}
	return vol, logs, nil
}

// Boot prepares volumes and starts QEMU. It returns once the process has
// started; readiness is the caller's check.
func (q *QEMU) Boot(ctx context.Context, w workload.Spec, generation uint64, env map[string]string, files map[string][]byte) (Guest, error) {
	vol, logs, err := q.dirs(w.ID)
	if err != nil {
		return nil, err
	}
	state := filepath.Join(vol, w.ID+".state.raw")
	if _, err := os.Stat(state); errors.Is(err, os.ErrNotExist) {
		empty, err := os.MkdirTemp(q.DataDir, "state-src-")
		if err != nil {
			return nil, err
		}
		err = q.createVolume(ctx, "state", empty, w.StateMiB, state)
		_ = os.RemoveAll(empty)
		if err != nil {
			return nil, err
		}
	}
	src, err := os.MkdirTemp(q.DataDir, "boot-src-")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(src) }()
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		fmt.Fprintf(&b, "%s=%s\n", k, env[k])
	}
	if err := os.WriteFile(filepath.Join(src, w.BootEnvFile), []byte(b.String()), 0o600); err != nil {
		return nil, err
	}
	for name, data := range files {
		if err := os.WriteFile(filepath.Join(src, name), data, 0o600); err != nil {
			return nil, err
		}
	}
	boot := filepath.Join(vol, w.ID+".boot.raw")
	if err := q.createVolume(ctx, "boot", src, 8, boot); err != nil {
		return nil, err
	}
	port, err := freePort()
	if err != nil {
		_ = os.Remove(boot)
		return nil, err
	}
	serial := filepath.Join(logs, fmt.Sprintf("%s.%d.log", w.ID, generation))
	args := []string{
		"-machine", "q35",
		"-device", "pcie-root-port,port=0x10,chassis=1,id=pci.1,bus=pcie.0,multifunction=on,addr=0x3",
		"-device", "pcie-root-port,port=0x11,chassis=2,id=pci.2,bus=pcie.0,addr=0x3.0x6",
		"-device", "pcie-root-port,port=0x12,chassis=3,id=pci.4,bus=pcie.0,addr=0x3.0x4",
		"-device", "virtio-scsi-pci,bus=pci.2,addr=0x0,id=scsi0",
		"-device", "scsi-hd,bus=scsi0.0,drive=hd0",
		"-device", "scsi-hd,bus=scsi0.0,drive=hd1",
		"-device", "scsi-hd,bus=scsi0.0,drive=hd2",
		"-vga", "none", "-device", "isa-debug-exit",
		"-m", fmt.Sprintf("%dM", w.MemoryMiB),
		"-device", "virtio-rng-pci", "-device", "virtio-balloon",
		"-machine", "accel=kvm", "-cpu", "host",
		"-drive", "file=" + w.Image + ",format=raw,if=none,id=hd0,snapshot=on",
		"-drive", "file=" + state + ",format=raw,if=none,id=hd1",
		"-drive", "file=" + boot + ",format=raw,if=none,id=hd2",
		"-device", "virtio-net,bus=pci.4,addr=0x0,netdev=n0",
		"-netdev", fmt.Sprintf("user,id=n0,hostfwd=tcp:127.0.0.1:%d-:%d", port, w.GuestPort),
		"-display", "none", "-serial", "file:" + serial,
	}
	cmd := exec.Command(q.bin(q.QEMUBin, "qemu-system-x86_64"), args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		_ = os.Remove(boot)
		return nil, fmt.Errorf("qemu: %w", err)
	}
	// The pidfile lets a restarted agent stop a guest it lost track of.
	pidfile := q.pidfile(w.ID)
	if err := os.WriteFile(pidfile, []byte(strconv.Itoa(cmd.Process.Pid)), 0o600); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		_ = os.Remove(boot)
		return nil, err
	}
	g := &qemuGuest{cmd: cmd, addr: fmt.Sprintf("127.0.0.1:%d", port), exited: make(chan struct{})}
	go func() {
		_ = cmd.Wait()
		// The boot volume holds this boot's env and secret files.
		_ = os.Remove(boot)
		_ = os.Remove(pidfile)
		close(g.exited)
	}()
	return g, nil
}

func (q *QEMU) pidfile(id string) string { return filepath.Join(q.DataDir, "volumes", id+".pid") }

// Reap stops a guest left running by an earlier agent (crash or kill) and
// deletes its boot volume. It kills the recorded process only if its
// command line still names this workload's state volume, so a reused pid
// is left alone.
func (q *QEMU) Reap(id string) (bool, error) {
	pidfile := q.pidfile(id)
	boot := filepath.Join(q.DataDir, "volumes", id+".boot.raw")
	defer func() { _ = os.Remove(boot) }()
	raw, err := os.ReadFile(pidfile)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil || pid <= 1 {
		return false, os.Remove(pidfile)
	}
	state, err := filepath.Abs(filepath.Join(q.DataDir, "volumes", id+".state.raw"))
	if err != nil {
		return false, err
	}
	cmdline, alive, err := processCommandLine(pid)
	switch {
	case err != nil:
		return false, fmt.Errorf("checking pid %d: %w", pid, err)
	case !alive:
		return false, os.Remove(pidfile)
	case !strings.Contains(cmdline, "file="+state+","):
		return false, os.Remove(pidfile) // the pid now belongs to another process
	}
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return false, err
	}
	deadline := time.Now().Add(10 * time.Second)
	for syscall.Kill(pid, 0) == nil {
		if time.Now().After(deadline) {
			return true, fmt.Errorf("guest pid %d did not exit", pid)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return true, os.Remove(pidfile)
}

type qemuGuest struct {
	cmd    *exec.Cmd
	addr   string
	exited chan struct{}
}

func (g *qemuGuest) Addr() string            { return g.addr }
func (g *qemuGuest) Exited() <-chan struct{} { return g.exited }

// Stop sends SIGTERM (QEMU powers the guest off) and SIGKILL after grace.
// The workload contract for stop is crash consistency: every acknowledged
// write is already durable off-host, so stop needs no guest cooperation.
func (g *qemuGuest) Stop(grace time.Duration) error {
	select {
	case <-g.exited:
		return nil
	default:
	}
	_ = g.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-g.exited:
		return nil
	case <-time.After(grace):
	}
	_ = g.cmd.Process.Kill()
	<-g.exited
	return nil
}

// processCommandLine returns pid's command line. On Linux it reads /proc
// (no dependency on a ps that may lack -ww, such as BusyBox's); elsewhere it
// runs ps, whose exit status 1 means no such process.
func processCommandLine(pid int) (string, bool, error) {
	if runtime.GOOS == "linux" {
		b, err := os.ReadFile(fmt.Sprintf("/proc/%d/cmdline", pid))
		if errors.Is(err, os.ErrNotExist) {
			return "", false, nil
		}
		if err != nil {
			return "", false, err
		}
		return strings.ReplaceAll(string(b), "\x00", " "), true, nil
	}
	out, err := exec.Command("ps", "-ww", "-o", "command=", "-p", strconv.Itoa(pid)).Output()
	var exit *exec.ExitError
	if errors.As(err, &exit) && exit.ExitCode() == 1 {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return string(out), true, nil
}

func freePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port, nil
}
