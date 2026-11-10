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

func TestVerifyingKeepsTheLastAuthenticHint(t *testing.T) {
	ctx, key, p := context.Background(), testKey(), "inst/abc"
	src := &rawStore{}
	var refused []error
	v := &Verifying{Source: src, Key: key, OnRefused: func(err error) { refused = append(refused, err) }}
	if _, ok, err := v.Read(ctx, p); ok || err != nil {
		t.Fatalf("no hint yet: ok=%v err=%v", ok, err)
	}
	src.data = []byte(`{"version":1,"generation":5,"busy":true,"updated_at":"2026-11-09T00:00:00Z"}`)
	if _, _, err := v.Read(ctx, p); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("unsigned before any accepted: %v, want ErrUnauthenticated", err)
	}
	// at(gen, seq, ts): the clock (ts) never decides order.
	at := func(gen, seq, ts string) string {
		return `{"version":2,"generation":` + gen + `,"busy":false,"updated_at":"2026-11-09T00:00:` + ts + `Z","seq":` + seq + `}`
	}
	type step struct {
		data          []byte
		gen, seq      uint64 // the hint Read must return
		fallback      bool   // returned with ErrFallback
		refusedLogged bool
	}
	stale := sign(key, p, at("5", "2", "59"))
	steps := []step{
		{sign(key, p, at("5", "3", "10")), 5, 3, false, false},
		{sign(key, p, at("5", "3", "10")), 5, 3, false, false}, // re-read unchanged
		{sign(key, p, at("5", "4", "20")), 5, 4, false, false}, // newer
		{sign(key, p, at("5", "5", "01")), 5, 5, false, false}, // newer, guest clock stepped back
		{stale, 5, 5, true, false},                             // older: one alone can be a concurrent read
		{stale, 5, 5, true, false},
		{stale, 5, 5, true, true},                                                           // the third in a row is reported
		{sign(key, p, at("4", "9", "59")), 5, 5, true, true},                                // older generation: the fourth stale in a row
		{nil, 5, 5, true, true},                                                             // deleted
		{[]byte(strings.Replace(at("9", "1", "00"), "false", "true", 1)), 5, 5, true, true}, // unsigned busy
		{signed(at("9", "1", "00"), strings.Repeat("0", 64)), 5, 5, true, true},             // bad MAC
		{sign(key, "inst/other", at("9", "1", "00")), 5, 5, true, true},                     // another prefix's hint
		{sign(key, p, at("6", "1", "00")), 6, 1, false, false},                              // next boot
	}
	for i, s := range steps {
		src.data = s.data
		n := len(refused)
		h, ok, err := v.Read(ctx, p)
		if !ok || h.Generation != s.gen || h.Seq != s.seq {
			t.Fatalf("step %d: %+v ok=%v err=%v, want generation %d seq %d", i, h, ok, err, s.gen, s.seq)
		}
		if s.fallback != errors.Is(err, ErrFallback) || (!s.fallback && err != nil) {
			t.Fatalf("step %d: err %v, want fallback %v", i, err, s.fallback)
		}
		if logged := len(refused) > n; logged != s.refusedLogged {
			t.Fatalf("step %d: refusal reported = %v, want %v", i, logged, s.refusedLogged)
		}
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
	// A crash mid-creation leaves only a temporary file, never a partial key.
	if err := os.WriteFile(filepath.Join(dir, ".w5.key-123"), []byte("par"), 0o600); err != nil {
		t.Fatal(err)
	}
	if k, err := LoadOrCreateKey(dir, "w5"); err != nil || len(k) != KeySize {
		t.Fatalf("after an interrupted creation: %d bytes, %v", len(k), err)
	}
}

// TestVerifyingRecordSurvivesRestart: with Record set, an agent restart
// keeps the newest accepted hint, so an older signed hint replayed after
// the restart is refused and a deleted hint falls back to the recorded one.
func TestVerifyingRecordSurvivesRestart(t *testing.T) {
	ctx, key, p := context.Background(), testKey(), "inst/abc"
	at := func(gen, seq string) []byte {
		return sign(key, p, `{"version":2,"generation":`+gen+`,"busy":false,"updated_at":"2026-11-09T00:00:00Z","seq":`+seq+`}`)
	}
	record := filepath.Join(t.TempDir(), "w1.last")
	var recErrs []error
	fresh := func(src *rawStore) *Verifying {
		return &Verifying{Source: src, Key: key, Record: record, OnRecordError: func(err error) { recErrs = append(recErrs, err) }}
	}
	src := &rawStore{}
	v := fresh(src)
	for _, seq := range []string{"3", "4"} {
		src.data = at("5", seq)
		if _, _, err := v.Read(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	if st, err := os.Stat(record); err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("record: %v %v, want a 0600 file", st, err)
	}

	// Restart: the store now serves the older hint (seq 3).
	src.data = at("5", "3")
	h, ok, err := fresh(src).Read(ctx, p)
	if !ok || h.Seq != 4 || !errors.Is(err, ErrFallback) || !errors.Is(err, ErrStale) {
		t.Fatalf("replay after restart: %+v ok=%v err=%v, want the recorded seq 4 and a stale refusal", h, ok, err)
	}
	// Without the record, that replay would have been accepted.
	if h, _, err := (&Verifying{Source: src, Key: key}).Read(ctx, p); err != nil || h.Seq != 3 {
		t.Fatalf("control without a record: %+v %v", h, err)
	}
	// Restart with the hint deleted: the recorded hint is used.
	src.data = nil
	if h, ok, err := fresh(src).Read(ctx, p); !ok || h.Seq != 4 || !errors.Is(err, ErrFallback) {
		t.Fatalf("deleted hint after restart: %+v ok=%v err=%v", h, ok, err)
	}
	// A newer hint after restart is accepted and recorded.
	src.data = at("6", "1")
	if h, _, err := fresh(src).Read(ctx, p); err != nil || h.Generation != 6 {
		t.Fatalf("newer hint after restart: %+v %v", h, err)
	}
	src.data = at("5", "4")
	if h, _, err := fresh(src).Read(ctx, p); !errors.Is(err, ErrStale) || h.Generation != 6 {
		t.Fatalf("the record did not advance: %+v %v", h, err)
	}
	if len(recErrs) != 0 {
		t.Fatalf("record errors: %v", recErrs)
	}
}

// TestVerifyingRecordRefusesBadRecords: a record that does not verify (a
// different prefix, altered bytes), is not private or is not a regular file
// is reported and ignored, never trusted.
func TestVerifyingRecordRefusesBadRecords(t *testing.T) {
	ctx, key, p := context.Background(), testKey(), "inst/abc"
	hint := `{"version":2,"generation":5,"busy":false,"updated_at":"2026-11-09T00:00:00Z","seq":9}`
	newer := sign(key, p, `{"version":2,"generation":5,"busy":false,"updated_at":"2026-11-09T00:00:00Z","seq":2}`)
	cases := map[string]func(path string){
		"another prefix": func(path string) { _ = os.WriteFile(path, sign(key, "inst/other", hint), 0o600) },
		"altered": func(path string) {
			_ = os.WriteFile(path, []byte(strings.Replace(string(sign(key, p, hint)), `"seq":9`, `"seq":99`, 1)), 0o600)
		},
		"not private": func(path string) { _ = os.WriteFile(path, sign(key, p, hint), 0o644); _ = os.Chmod(path, 0o644) },
		"symlink": func(path string) {
			target := path + ".target"
			_ = os.WriteFile(target, sign(key, p, hint), 0o600)
			_ = os.Symlink(target, path)
		},
	}
	for name, plant := range cases {
		t.Run(name, func(t *testing.T) {
			record := filepath.Join(t.TempDir(), "w1.last")
			plant(record)
			var recErrs []error
			v := &Verifying{Source: &rawStore{data: newer}, Key: key, Record: record, OnRecordError: func(err error) { recErrs = append(recErrs, err) }}
			h, _, err := v.Read(ctx, p)
			if err != nil || h.Seq != 2 {
				t.Fatalf("read with a bad record: %+v %v, want the stored seq 2 accepted", h, err)
			}
			if len(recErrs) == 0 {
				t.Fatal("the bad record was not reported")
			}
		})
	}
}

// TestVerifyingRecordWriteFailureKeepsHints: when the record cannot be
// written, hints are still accepted and the failure is reported.
func TestVerifyingRecordWriteFailureKeepsHints(t *testing.T) {
	ctx, key, p := context.Background(), testKey(), "inst/abc"
	var recErrs []error
	v := &Verifying{
		Source: &rawStore{data: sign(key, p, `{"version":2,"generation":5,"busy":true,"updated_at":"2026-11-09T00:00:00Z","seq":1}`)},
		Key:    key, Record: filepath.Join(t.TempDir(), "missing-dir", "w1.last"),
		OnRecordError: func(err error) { recErrs = append(recErrs, err) },
	}
	if h, ok, err := v.Read(ctx, p); err != nil || !ok || !h.Busy {
		t.Fatalf("read: %+v %v %v", h, ok, err)
	}
	if len(recErrs) != 1 {
		t.Fatalf("record errors = %d, want 1", len(recErrs))
	}
}
