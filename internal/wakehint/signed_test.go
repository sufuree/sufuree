package wakehint

import (
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testKey() []byte {
	k := make([]byte, KeySize)
	for i := range k {
		k[i] = byte(i)
	}
	return k
}

const kat = `{"version":2,"generation":7,"busy":false,"updated_at":"2026-11-09T00:00:00Z","seq":1}`

// katMAC was computed independently (Python hmac); the Zati publisher
// carries the same vector.
const katMAC = "0b33e49ce6982b61432f736d34c5e24eb9702ea7274ce3a0d32b7685e145c041"

func signed(hint, mac string) []byte {
	return []byte(`{"version":2,"hint":` + hint + `,"mac":"` + mac + `"}`)
}

func TestMACKnownAnswer(t *testing.T) {
	if got := hex.EncodeToString(MAC(testKey(), "inst/abc", []byte(kat))); got != katMAC {
		t.Fatalf("MAC %s, want %s", got, katMAC)
	}
}

func TestParse(t *testing.T) {
	key := testKey()
	h, err := Parse(signed(kat, katMAC), "inst/abc", key)
	if err != nil || h.Generation != 7 || h.Version != 2 {
		t.Fatalf("signed hint: %+v, %v", h, err)
	}
	v1 := `{"version":1,"generation":7,"busy":true,"updated_at":"2026-11-09T00:00:00Z"}`
	if h, err := Parse([]byte(v1), "inst/abc", nil); err != nil || !h.Busy {
		t.Fatalf("unsigned reader, version 1: %+v, %v", h, err)
	}
	if h, err := Parse(signed(kat, katMAC), "inst/abc", nil); err != nil || h.Generation != 7 {
		t.Fatalf("unsigned reader, version 2: %+v, %v", h, err)
	}
	busy := strings.Replace(kat, `"busy":false`, `"busy":true`, 1)
	other := make([]byte, KeySize)
	for name, c := range map[string]struct {
		data   []byte
		prefix string
		key    []byte
	}{
		"version 1 with a key": {[]byte(v1), "inst/abc", key},
		"altered hint":         {signed(busy, katMAC), "inst/abc", key},
		"altered mac":          {signed(kat, strings.Replace(katMAC, "d", "e", 1)), "inst/abc", key},
		"no mac":               {signed(kat, ""), "inst/abc", key},
		"other installation":   {signed(kat, katMAC), "inst/xyz", key},
		"other key":            {signed(kat, katMAC), "inst/abc", other},
	} {
		if _, err := Parse(c.data, c.prefix, c.key); !errors.Is(err, ErrUnauthenticated) {
			t.Errorf("%s: %v, want ErrUnauthenticated", name, err)
		}
	}
	inner1 := strings.Replace(kat, `"version":2`, `"version":1`, 1)
	if _, err := Parse(signed(inner1, hex.EncodeToString(MAC(key, "p", []byte(inner1)))), "p", key); err == nil {
		t.Error("a signed envelope around a version 1 hint was accepted")
	}
	if _, err := Parse([]byte(`{"version":3}`), "p", nil); err == nil {
		t.Error("version 3 accepted")
	}
}

type rawStore struct{ data []byte }

func (r *rawStore) ReadRaw(context.Context, string) ([]byte, bool, error) {
	return r.data, r.data != nil, nil
}

func sign(key []byte, prefix, hint string) []byte {
	return signed(hint, hex.EncodeToString(MAC(key, prefix, []byte(hint))))
}

func TestVerifyingRefusesReplays(t *testing.T) {
	ctx, key, p := context.Background(), testKey(), "inst/abc"
	src := &rawStore{}
	v := &Verifying{Source: src, Key: key}
	if _, ok, err := v.Read(ctx, p); ok || err != nil {
		t.Fatalf("no hint: ok=%v err=%v", ok, err)
	}
	// at(gen, seq, ts): the clock (ts) never decides order.
	at := func(gen, seq, ts string) string {
		return `{"version":2,"generation":` + gen + `,"busy":true,"updated_at":"2026-11-09T00:00:` + ts + `Z","seq":` + seq + `}`
	}
	steps := []struct {
		hint string
		ok   bool
	}{
		{at("5", "3", "10"), true},
		{at("5", "3", "10"), true},  // re-read unchanged
		{at("5", "4", "20"), true},  // newer
		{at("5", "5", "01"), true},  // newer, after the guest clock stepped back
		{at("5", "2", "59"), false}, // older within the generation
		{at("4", "9", "59"), false}, // older generation
		{at("6", "1", "00"), true},  // next boot
	}
	for i, s := range steps {
		src.data = sign(key, p, s.hint)
		_, ok, err := v.Read(ctx, p)
		if s.ok && (err != nil || !ok) {
			t.Fatalf("step %d: ok=%v err=%v, want accepted", i, ok, err)
		}
		if !s.ok && !errors.Is(err, ErrStale) {
			t.Fatalf("step %d: ok=%v err=%v, want ErrStale", i, ok, err)
		}
	}
	src.data = []byte(at("7", "1", "00")) // unsigned
	if _, _, err := v.Read(ctx, p); err == nil {
		t.Fatal("an unsigned hint was accepted")
	}
	if _, _, err := (&Verifying{Source: src}).Read(ctx, p); err == nil {
		t.Fatal("a verifier without a key read a hint")
	}
}

func TestLoadOrCreateKey(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "hintkeys")
	k1, err := LoadOrCreateKey(dir, "w1")
	if err != nil || len(k1) != KeySize {
		t.Fatalf("create: %d bytes, %v", len(k1), err)
	}
	st, err := os.Stat(filepath.Join(dir, "w1.key"))
	if err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("key file mode %v, %v", st.Mode(), err)
	}
	if d, _ := os.Stat(dir); d.Mode().Perm() != 0o700 {
		t.Fatalf("key dir mode %v", d.Mode())
	}
	k2, err := LoadOrCreateKey(dir, "w1")
	if err != nil || string(k1) != string(k2) {
		t.Fatal("the key changed on reload")
	}
	if k3, _ := LoadOrCreateKey(dir, "w2"); string(k3) == string(k1) {
		t.Fatal("two workloads share a key")
	}
	p := filepath.Join(dir, "w1.key")
	if err := os.Chmod(p, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOrCreateKey(dir, "w1"); err == nil {
		t.Fatal("a group/world-readable key was accepted")
	}
	_ = os.Chmod(p, 0o600)
	if err := os.WriteFile(filepath.Join(dir, "w3.key"), []byte("short"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOrCreateKey(dir, "w3"); err == nil {
		t.Fatal("a short key was accepted")
	}
	if err := os.Symlink(p, filepath.Join(dir, "w4.key")); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadOrCreateKey(dir, "w4"); err == nil {
		t.Fatal("a symlinked key was accepted")
	}
}
