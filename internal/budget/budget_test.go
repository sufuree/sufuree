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

func TestLedgerPersistAndSeed(t *testing.T) {
	durable := map[string]Reservation{}
	l := &Ledger{
		Grant: Grant{DeploymentUID: "dep-1", MaxHosts: 2, MaxMemoryMiB: 1024},
		Persist: func(op string, r Reservation) error {
			durable[op] = r
			return nil
		},
	}
	if err := l.ReserveCreate("op-1", 256); err != nil {
		t.Fatal(err)
	}
	if err := l.Confirm("op-1"); err != nil {
		t.Fatal(err)
	}
	if durable["op-1"].State != "confirmed" || durable["op-1"].MemoryMiB != 256 {
		t.Fatalf("durable = %+v", durable["op-1"])
	}

	// A fresh ledger re-seeds spent capacity from the durable records.
	reborn := &Ledger{Grant: l.Grant}
	reborn.Seed(durable)
	if hosts, mem := reborn.Usage(); hosts != 1 || mem != 256 {
		t.Fatalf("re-seeded usage = %d hosts, %d MiB", hosts, mem)
	}
	// The same operation stays idempotent after re-seed (no double count).
	if err := reborn.ReserveCreate("op-1", 256); err != nil {
		t.Fatal(err)
	}
	if hosts, _ := reborn.Usage(); hosts != 1 {
		t.Fatalf("double counted: %d hosts", hosts)
	}
	// A released operation cannot be reused after re-seed.
	if err := reborn.Release("op-1"); err != nil {
		t.Fatal(err)
	}
	if err := reborn.ReserveCreate("op-1", 256); err == nil {
		t.Fatal("released operation was reused")
	}
}

func TestLedgerPersistFailureRollsBack(t *testing.T) {
	fail := true
	l := &Ledger{
		Grant: Grant{DeploymentUID: "dep-1", MaxHosts: 2, MaxMemoryMiB: 1024},
		Persist: func(op string, r Reservation) error {
			if fail {
				return errors.New("disk full")
			}
			return nil
		},
	}
	if err := l.ReserveCreate("op-1", 256); err == nil {
		t.Fatal("a failed durable write admitted a reservation")
	}
	if hosts, mem := l.Usage(); hosts != 0 || mem != 0 {
		t.Fatalf("failed persist left usage = %d, %d", hosts, mem)
	}
	fail = false
	if err := l.ReserveCreate("op-1", 256); err != nil {
		t.Fatal(err)
	}
	if hosts, _ := l.Usage(); hosts != 1 {
		t.Fatalf("usage after retry = %d", hosts)
	}
}
