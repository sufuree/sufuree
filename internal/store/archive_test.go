package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/sufuree/sufuree/internal/resource"
)

func TestExportImportReopen(t *testing.T) {
	srcPath := filepath.Join(t.TempDir(), "control.db")
	src, err := Open(srcPath)
	if err != nil {
		t.Fatal(err)
	}
	mustUpdate(t, src, func(tx *Tx) error {
		if _, err := tx.ApplyResource(exampleApp()); err != nil {
			return err
		}
		if _, err := tx.ApplyResource(examplePool()); err != nil {
			return err
		}
		if err := tx.PutMeta(resource.ClusterMeta{
			DeploymentUID: "dep-1", RecoveryIncarnation: 1, OwnerEpoch: 4,
		}); err != nil {
			return err
		}
		if err := tx.PutHost(exampleHost("pool-1")); err != nil {
			return err
		}
		if err := tx.PutPlacement(examplePlacement("app-1", "host-1")); err != nil {
			return err
		}
		if err := tx.PutOperation(exampleOp("op-1", "app-1", t0)); err != nil {
			return err
		}
		return tx.PutClaim(exampleClaim("app-1"))
	})
	before := readAll(t, src)
	var buf bytes.Buffer
	if err := src.Export(&buf); err != nil {
		t.Fatal(err)
	}
	if src.db.NoSync {
		t.Fatal("NoSync is enabled")
	}
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}

	dstPath := filepath.Join(t.TempDir(), "control.db")
	dst, err := Open(dstPath)
	if err != nil {
		t.Fatal(err)
	}
	mustUpdate(t, dst, func(tx *Tx) error {
		other := exampleApp()
		other.UID = "app-9"
		other.Name = "other"
		_, err := tx.ApplyResource(other)
		return err
	})
	if err := dst.Import(bytes.NewReader([]byte(`{"version":99,"buckets":[]}`))); err == nil {
		t.Fatal("rejected version was accepted")
	}
	if err := dst.View(func(tx *Tx) error {
		_, err := tx.GetResource("app-9")
		return err
	}); err != nil {
		t.Fatal(err)
	}

	dup := append([]byte(nil), buf.Bytes()...)
	var arch archive
	if err := json.Unmarshal(dup, &arch); err != nil {
		t.Fatal(err)
	}
	arch.Buckets[0].Entries = append(arch.Buckets[0].Entries, arch.Buckets[0].Entries[0])
	bad, err := json.Marshal(arch)
	if err != nil {
		t.Fatal(err)
	}
	if err := dst.Import(bytes.NewReader(bad)); err == nil {
		t.Fatal("duplicate key was accepted")
	}
	if err := dst.View(func(tx *Tx) error {
		_, err := tx.GetResource("app-9")
		return err
	}); err != nil {
		t.Fatal(err)
	}

	if err := dst.Import(bytes.NewReader(buf.Bytes())); err != nil {
		t.Fatal(err)
	}
	if dst.db.NoSync {
		t.Fatal("NoSync is enabled after import")
	}
	if err := dst.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dstPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	got := readAll(t, reopened)
	if !reflect.DeepEqual(before, got) {
		t.Fatalf("reopened = %#v\nbefore = %#v", got, before)
	}
	err = reopened.View(func(tx *Tx) error {
		_, err := tx.GetResource("app-9")
		return err
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("replaced resource err = %v", err)
	}
}

func TestImportRejectsEmpty(t *testing.T) {
	s := newStore(t)
	if err := s.Import(bytes.NewReader(nil)); err == nil {
		t.Fatal("empty import accepted")
	}
	if err := s.Export(nil); err == nil {
		t.Fatal("nil export accepted")
	}
}

type stored struct {
	Resources  []resource.Resource
	Operations []resource.Operation
	Placements []resource.Placement
	Hosts      []resource.Host
	Claims     []resource.ActivationClaim
	Meta       resource.ClusterMeta
}

func readAll(t *testing.T, s *Store) stored {
	t.Helper()
	var out stored
	if err := s.View(func(tx *Tx) error {
		var err error
		if out.Resources, err = tx.ListResources(); err != nil {
			return err
		}
		if out.Operations, err = tx.ListOperations(); err != nil {
			return err
		}
		if out.Placements, err = tx.ListPlacements(); err != nil {
			return err
		}
		if out.Hosts, err = tx.ListHosts(); err != nil {
			return err
		}
		if out.Claims, err = tx.ListClaims(); err != nil {
			return err
		}
		out.Meta, err = tx.GetMeta()
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return out
}
