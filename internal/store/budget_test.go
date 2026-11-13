package store

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/sufuree/sufuree/internal/resource"
)

func TestBudgetReservationArchive(t *testing.T) {
	srcPath := filepath.Join(t.TempDir(), "control.db")
	src, err := Open(srcPath)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	mustUpdate(t, src, func(tx *Tx) error {
		return tx.PutBudgetReservation(resource.BudgetReservation{
			OperationID: "op-1", DeploymentUID: "dep-1", MemoryMiB: 256, State: "confirmed",
		})
	})
	var buf bytes.Buffer
	if err := src.Export(&buf); err != nil {
		t.Fatal(err)
	}

	dstPath := filepath.Join(t.TempDir(), "control.db")
	dst, err := Open(dstPath)
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	if err := dst.Import(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatal(err)
	}
	var got []resource.BudgetReservation
	if err := dst.View(func(tx *Tx) error {
		var err error
		got, err = tx.ListBudgetReservations()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].OperationID != "op-1" || got[0].DeploymentUID != "dep-1" ||
		got[0].State != "confirmed" || got[0].MemoryMiB != 256 {
		t.Fatalf("reservations = %+v", got)
	}
}

func TestBudgetReservationValidation(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	err = st.Update(func(tx *Tx) error {
		return tx.PutBudgetReservation(resource.BudgetReservation{
			OperationID: "op-1", DeploymentUID: "dep-1", MemoryMiB: 256, State: "bogus",
		})
	})
	if err == nil {
		t.Fatal("an invalid reservation state was accepted")
	}
}
