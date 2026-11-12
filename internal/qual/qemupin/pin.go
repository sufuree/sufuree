// Package qemupin loads and checks the fixed-host QEMU/guest image pin used
// for RFC-0001 F05 / live fixed-host qualification.
package qemupin

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const (
	// Schema identifies the pin document format.
	Schema = "sufuree.qemu-guest-pin/v1"

	// EnvEnable opts into live QEMU qualification (unset = skip).
	EnvEnable = "SUFUREE_QUAL_QEMU"
	// EnvImage is the absolute path to the pinned guest image (.raw).
	EnvImage = "SUFUREE_QUAL_GUEST_IMAGE"
	// EnvPin is an optional path to a pin JSON; default is the repo pin.
	EnvPin = "SUFUREE_QUAL_QEMU_PIN"
)

// Pin is the recorded VMM / Ops / image contract for one qualification host.
type Pin struct {
	Schema string   `json:"schema"`
	Arch   string   `json:"arch"`
	VMM    ToolPin  `json:"vmm"`
	Ops    ToolPin  `json:"ops"`
	Image  ImagePin `json:"image"`
	Accel  string   `json:"accel"`
	Notes  string   `json:"notes,omitempty"`
}

// ToolPin names a required host binary and an optional version prefix check.
type ToolPin struct {
	Bin           string `json:"bin"`
	VersionPrefix string `json:"version_prefix,omitempty"`
	VersionFlag   string `json:"version_flag,omitempty"`
}

// ImagePin describes the digest-pinned guest artifact.
type ImagePin struct {
	SHA256    string `json:"sha256"`
	GuestPort int    `json:"guest_port"`
	Format    string `json:"format"`
	Runtime   string `json:"runtime"`
	Workload  string `json:"workload"`
}

// Resolved is a pin plus host-observed tool versions and the image path used.
type Resolved struct {
	Pin         Pin    `json:"pin"`
	ImagePath   string `json:"image_path"`
	ImageSHA256 string `json:"image_sha256"`
	QEMUVersion string `json:"qemu_version"`
	OpsVersion  string `json:"ops_version"`
	KVM         bool   `json:"kvm"`
}

// Load reads a pin JSON file.
func Load(path string) (Pin, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Pin{}, err
	}
	var p Pin
	if err := json.Unmarshal(b, &p); err != nil {
		return Pin{}, err
	}
	if err := p.Valid(); err != nil {
		return Pin{}, err
	}
	return p, nil
}

// Valid checks required pin fields.
func (p Pin) Valid() error {
	switch {
	case p.Schema != Schema:
		return fmt.Errorf("qemupin: schema %q, want %q", p.Schema, Schema)
	case p.Arch != "x86_64":
		return fmt.Errorf("qemupin: arch %q, guest.QEMU pin requires x86_64", p.Arch)
	case p.VMM.Bin == "":
		return errors.New("qemupin: vmm.bin is required")
	case p.Ops.Bin == "":
		return errors.New("qemupin: ops.bin is required")
	case p.Image.GuestPort < 1 || p.Image.GuestPort > 65535:
		return errors.New("qemupin: image.guest_port out of range")
	case p.Image.Format != "raw":
		return fmt.Errorf("qemupin: image.format %q, guest.QEMU only runs raw", p.Image.Format)
	case p.Accel != "kvm":
		return fmt.Errorf("qemupin: accel %q, guest.QEMU only runs with kvm", p.Accel)
	}
	return nil
}

// Enabled reports whether live QEMU qualification was explicitly opted in.
func Enabled() bool {
	v := strings.TrimSpace(os.Getenv(EnvEnable))
	return v == "1" || strings.EqualFold(v, "true") || strings.EqualFold(v, "yes")
}

// DefaultPinPath returns docs/qualification/pins/qemu-guest.json by walking
// up from the working directory (package dirs under the module root).
func DefaultPinPath() string {
	if p := strings.TrimSpace(os.Getenv(EnvPin)); p != "" {
		return p
	}
	wd, err := os.Getwd()
	if err != nil {
		return ""
	}
	dir := wd
	for {
		candidate := filepath.Join(dir, "docs", "qualification", "pins", "qemu-guest.json")
		if st, err := os.Stat(candidate); err == nil && !st.IsDir() {
			return candidate
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

// Resolve loads the pin, locates tools/image, and verifies the image digest
// when the pin records one.
func Resolve() (Resolved, error) {
	path := DefaultPinPath()
	if path == "" {
		return Resolved{}, errors.New("qemupin: pin file not found; set " + EnvPin)
	}
	pin, err := Load(path)
	if err != nil {
		return Resolved{}, err
	}
	img := strings.TrimSpace(os.Getenv(EnvImage))
	if img == "" {
		return Resolved{}, errors.New("qemupin: set " + EnvImage + " to the pinned guest image path")
	}
	img, err = filepath.Abs(img)
	if err != nil {
		return Resolved{}, err
	}
	if st, err := os.Stat(img); err != nil || st.IsDir() {
		return Resolved{}, fmt.Errorf("qemupin: image %s: %w", img, err)
	}
	sum, err := fileSHA256(img)
	if err != nil {
		return Resolved{}, err
	}
	if pin.Image.SHA256 != "" && !strings.EqualFold(pin.Image.SHA256, sum) {
		return Resolved{}, fmt.Errorf("qemupin: image sha256 %s, pin wants %s", sum, pin.Image.SHA256)
	}
	qemuVer, err := toolVersion(pin.VMM)
	if err != nil {
		return Resolved{}, fmt.Errorf("qemupin: vmm: %w", err)
	}
	opsVer, err := toolVersion(pin.Ops)
	if err != nil {
		return Resolved{}, fmt.Errorf("qemupin: ops: %w", err)
	}
	kvm := false
	if pin.Accel == "kvm" {
		if _, err := os.Stat("/dev/kvm"); err != nil {
			return Resolved{}, errors.New("qemupin: accel=kvm but /dev/kvm is unavailable")
		}
		kvm = true
	}
	return Resolved{
		Pin: pin, ImagePath: img, ImageSHA256: sum,
		QEMUVersion: qemuVer, OpsVersion: opsVer, KVM: kvm,
	}, nil
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func toolVersion(t ToolPin) (string, error) {
	bin, err := exec.LookPath(t.Bin)
	if err != nil {
		return "", fmt.Errorf("%s not on PATH: %w", t.Bin, err)
	}
	flag := t.VersionFlag
	if flag == "" {
		flag = "--version"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, bin, flag).CombinedOutput()
	line := strings.TrimSpace(string(out))
	if i := strings.IndexByte(line, '\n'); i >= 0 {
		line = line[:i]
	}
	if err != nil {
		if line == "" {
			return "", fmt.Errorf("%s %s: %w", t.Bin, flag, err)
		}
		return "", fmt.Errorf("%s %s: %w: %s", t.Bin, flag, err, line)
	}
	if t.VersionPrefix != "" {
		matched := false
		for _, tok := range strings.Fields(line) {
			if strings.HasPrefix(tok, t.VersionPrefix) {
				matched = true
				break
			}
		}
		if !matched {
			return "", fmt.Errorf("%s version %q has no token prefixed with %q", t.Bin, line, t.VersionPrefix)
		}
	}
	return line, nil
}
