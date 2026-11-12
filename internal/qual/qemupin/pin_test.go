package qemupin

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadValidPin(t *testing.T) {
	path := DefaultPinPath()
	if path == "" {
		t.Skip("repo pin not found from cwd")
	}
	p, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if p.VMM.Bin != "qemu-system-x86_64" || p.Image.GuestPort != 8080 {
		t.Fatalf("pin = %+v", p)
	}
}

func TestValidRejectsBadSchema(t *testing.T) {
	p := Pin{Schema: "nope", Arch: "x86_64", VMM: ToolPin{Bin: "qemu"}, Ops: ToolPin{Bin: "ops"},
		Image: ImagePin{GuestPort: 8080, Format: "raw"}, Accel: "kvm"}
	if err := p.Valid(); err == nil {
		t.Fatal("want schema error")
	}
}

func TestValidRejectsUnsupportedAccelAndFormat(t *testing.T) {
	base := Pin{Schema: Schema, Arch: "x86_64", VMM: ToolPin{Bin: "qemu"}, Ops: ToolPin{Bin: "ops"},
		Image: ImagePin{GuestPort: 8080, Format: "raw"}, Accel: "kvm"}
	badAccel := base
	badAccel.Accel = "tcg"
	if err := badAccel.Valid(); err == nil {
		t.Fatal("want accel error")
	}
	badFmt := base
	badFmt.Image.Format = "qcow2"
	if err := badFmt.Valid(); err == nil {
		t.Fatal("want format error")
	}
	badArch := base
	badArch.Arch = "arm64"
	if err := badArch.Valid(); err == nil {
		t.Fatal("want arch error")
	}
}

func TestResolveRequiresOptInImage(t *testing.T) {
	t.Setenv(EnvEnable, "1")
	t.Setenv(EnvImage, "")
	dir := t.TempDir()
	pinPath := filepath.Join(dir, "pin.json")
	src := DefaultPinPath()
	if src == "" {
		t.Skip("repo pin not found")
	}
	b, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pinPath, b, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvPin, pinPath)
	if _, err := Resolve(); err == nil {
		t.Fatal("want missing image error")
	}
}

func TestEnabled(t *testing.T) {
	t.Setenv(EnvEnable, "")
	if Enabled() {
		t.Fatal("empty should be false")
	}
	t.Setenv(EnvEnable, "1")
	if !Enabled() {
		t.Fatal("1 should be true")
	}
}
