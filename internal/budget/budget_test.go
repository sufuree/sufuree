package budget

import (
	"errors"
	"testing"
	"time"
)

func TestReserveConfirmRelease(t *testing.T) {
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	l := &Ledger{
		Grant: Grant{DeploymentUID: "d1", Revision: "r1", MaxHosts: 2, MaxMemoryMiB: 1024, Expires: now.Add(time.Hour)},
		Now:   func() time.Time { return now },
	}
	if err := l.ReserveCreate("op-a", 256); err != nil {
		t.Fatal(err)
	}
	if err := l.ReserveCreate("op-a", 256); err != nil {
		t.Fatal(err)
	}
	if err := l.Confirm("op-a"); err != nil {
		t.Fatal(err)
	}
	h, m := l.Usage()
	if h != 1 || m != 256 {
		t.Fatalf("usage = %d %d", h, m)
	}
	if err := l.ReserveCreate("op-b", 256); err != nil {
		t.Fatal(err)
	}
	if err := l.ReserveCreate("op-c", 256); !errors.Is(err, ErrDenied) {
		t.Fatalf("err = %v", err)
	}
	if err := l.Release("op-b"); err != nil {
		t.Fatal(err)
	}
	if err := l.ReserveCreate("op-c", 256); err != nil {
		t.Fatal(err)
	}
}

func TestExpiredGrant(t *testing.T) {
	now := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	l := &Ledger{
		Grant: Grant{DeploymentUID: "d1", MaxHosts: 1, MaxMemoryMiB: 512, Expires: now},
		Now:   func() time.Time { return now },
	}
	if err := l.ReserveCreate("op", 128); !errors.Is(err, ErrExpired) {
		t.Fatalf("err = %v", err)
	}
}

func TestReleasedCannotConfirm(t *testing.T) {
	l := &Ledger{Grant: Grant{DeploymentUID: "d1", MaxHosts: 1, MaxMemoryMiB: 512}}
	if err := l.ReserveCreate("op", 128); err != nil {
		t.Fatal(err)
	}
	if err := l.Release("op"); err != nil {
		t.Fatal(err)
	}
	if err := l.Confirm("op"); !errors.Is(err, ErrDenied) {
		t.Fatalf("err = %v", err)
	}
}
