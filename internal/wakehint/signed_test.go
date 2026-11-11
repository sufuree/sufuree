package wakehint

import (
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strconv"
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

func hintAt(gen, seq uint64) string {
	return `{"version":2,"generation":` + strconv.FormatUint(gen, 10) + `,"busy":false,"updated_at":"2026-11-09T00:00:00Z","seq":` + strconv.FormatUint(seq, 10) + `}`
}

func TestKeyringFile(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "hintkeys")
	r, err := OpenKeyring(dir, "w1")
	if err != nil {
		t.Fatal(err)
	}
	if r.For(1) != nil {
		t.Fatal("an empty keyring has a key")
	}
	k5, err := r.ForBoot(5)
	if err != nil || len(k5) != KeySize {
		t.Fatalf("ForBoot: %d bytes, %v", len(k5), err)
	}
	k9, err := r.ForBoot(9)
	if err != nil || string(k9) == string(k5) {
		t.Fatalf("the second boot's key: %v, same=%v", err, string(k9) == string(k5))
	}
	for _, g := range []uint64{0, 5, 9} {
		if _, err := r.ForBoot(g); err == nil {
			t.Fatalf("ForBoot(%d) accepted a generation not above the newest boot", g)
		}
	}
	for g, want := range map[uint64][]byte{0: nil, 4: nil, 5: k5, 6: nil, 8: nil, 9: k9, 10: nil, 100: nil} {
		if got := r.For(g); string(got) != string(want) {
			t.Errorf("For(%d) is the wrong key", g)
		}
	}
	p := filepath.Join(dir, "w1.keys")
	if st, err := os.Stat(p); err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("keyring file mode: %v", err)
	}
	if d, _ := os.Stat(dir); d.Mode().Perm() != 0o700 {
		t.Fatalf("key dir mode %v", d.Mode())
	}
	// A restarted agent has the same keys.
	r2, err := OpenKeyring(dir, "w1")
	if err != nil || string(r2.For(5)) != string(k5) || string(r2.For(9)) != string(k9) {
		t.Fatalf("reopened keyring differs: %v", err)
	}
	// An accepted hint from the newer boot drops the older key, durably.
	r2.Accepted(9)
	if r2.For(5) != nil || string(r2.For(9)) != string(k9) {
		t.Fatal("Accepted(9) did not drop exactly the older key")
	}
	if r3, err := OpenKeyring(dir, "w1"); err != nil || r3.For(5) != nil || string(r3.For(9)) != string(k9) {
		t.Fatalf("the dropped key came back after a restart: %v", err)
	}
	if other, _ := OpenKeyring(dir, "w2"); other.For(9) != nil {
		t.Fatal("two workloads share a keyring")
	}
	if err := os.Chmod(p, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenKeyring(dir, "w1"); err == nil {
		t.Fatal("a group/world-readable keyring was accepted")
	}
	for name, body := range map[string]string{
		"junk": "x", "version": `{"version":2,"keys":[]}`, "short key": `{"version":1,"keys":[{"generation":1,"key":"00"}]}`,
		"a boot at generation 0": `{"version":1,"keys":[{"generation":0,"key":"` + hex.EncodeToString(k5) + `"}]}`,
		"order":                  `{"version":1,"keys":[{"generation":2,"key":"` + hex.EncodeToString(k5) + `"},{"generation":2,"key":"` + hex.EncodeToString(k9) + `"}]}`,
	} {
		if err := os.WriteFile(filepath.Join(dir, "bad.keys"), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenKeyring(dir, "bad"); err == nil {
			t.Errorf("a keyring with %s was accepted", name)
		}
	}
}

// TestKeyringIsBounded: boots that never publish a hint don't grow the
// keyring without limit, and never cost the key of the last accepted hint.
func TestKeyringIsBounded(t *testing.T) {
	ctx, p := context.Background(), "inst/w1"
	dir := t.TempDir()
	r, err := OpenKeyring(dir, "w1")
	if err != nil {
		t.Fatal(err)
	}
	record := filepath.Join(dir, "w1.last")
	k5, _ := r.ForBoot(5)
	src := &rawStore{data: sign(k5, p, hintAt(5, 3))}
	if _, ok, err := (&Verifying{Source: src, Keys: r, Record: record}).Read(ctx, p); err != nil || !ok {
		t.Fatalf("the accepted hint: %v, %v", ok, err)
	}
	for g := uint64(6); g < 6+maxRingKeys+4; g++ { // boots that publish nothing
		if _, err := r.ForBoot(g); err != nil {
			t.Fatal(err)
		}
	}
	// A restarted agent has the bounded keyring, with the accepted hint's
	// key, and still falls back to that hint when the stored one is gone.
	r2, err := OpenKeyring(dir, "w1")
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for g := uint64(5); g < 6+maxRingKeys+4; g++ {
		if r2.For(g) != nil {
			n++
		}
	}
	if n != maxRingKeys || string(r2.For(5)) != string(k5) || r2.For(6) != nil || r2.For(6+maxRingKeys+3) == nil {
		t.Fatalf("the keyring kept %d keys (want %d), or dropped the accepted hint's or the newest", n, maxRingKeys)
	}
	h, ok, err := (&Verifying{Source: &rawStore{data: []byte("gone")}, Keys: r2, Record: record}).Read(ctx, p)
	if !errors.Is(err, ErrFallback) || !ok || h.Generation != 5 || h.Seq != 3 {
		t.Fatalf("after a restart the last authentic hint: %+v, %v, %v", h, ok, err)
	}
}

// TestKeyringAdoptsTheSingleKey: a key from before rotation still verifies
// the sleeping workload's last hint, and is gone from disk once the keyring
// is saved.
func TestKeyringAdoptsTheSingleKey(t *testing.T) {
	dir := t.TempDir()
	old := filepath.Join(dir, "w1.key")
	if err := os.WriteFile(old, testKey(), 0o600); err != nil {
		t.Fatal(err)
	}
	r, err := OpenKeyring(dir, "w1")
	if err != nil {
		t.Fatal(err)
	}
	if string(r.For(7)) != string(testKey()) {
		t.Fatal("the single key does not cover the generations before the first boot")
	}
	k8, err := r.ForBoot(8)
	if err != nil {
		t.Fatal(err)
	}
	if string(r.For(7)) != string(testKey()) || string(r.For(8)) != string(k8) || r.For(9) != nil {
		t.Fatal("the first boot's key replaced, or did not follow, the single key")
	}
	// The single key never covers a boot's generation, even one whose key
	// was dropped while the single key's hint is still the accepted one.
	r.Accepted(7)
	for g := uint64(9); g < 9+maxRingKeys+2; g++ {
		if _, err := r.ForBoot(g); err != nil {
			t.Fatal(err)
		}
	}
	if string(r.For(7)) != string(testKey()) || r.For(8) != nil || r.For(9) != nil {
		t.Fatal("after boots were dropped the single key covers a boot's generation, or was dropped")
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatalf("the single key file is still on disk: %v", err)
	}
	if r2, err := OpenKeyring(dir, "w1"); err != nil || string(r2.For(7)) != string(testKey()) {
		t.Fatalf("the adopted key was lost on reopen: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "w2.key"), []byte("short"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenKeyring(dir, "w2"); err == nil {
		t.Fatal("a short single key was adopted")
	}
	for name, body := range map[string]string{
		"no bound":           `{"version":1,"keys":[{"generation":0,"key":"` + hex.EncodeToString(testKey()) + `","legacy":true},{"generation":8,"key":"` + hex.EncodeToString(k8) + `"}]}`,
		"a bound above boot": `{"version":1,"keys":[{"generation":0,"key":"` + hex.EncodeToString(testKey()) + `","legacy":true,"below":9},{"generation":8,"key":"` + hex.EncodeToString(k8) + `"}]}`,
	} {
		if err := os.WriteFile(filepath.Join(dir, "bad.keys"), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenKeyring(dir, "bad"); err == nil {
			t.Errorf("a keyring whose single key has %s was accepted", name)
		}
	}
	// Beside an existing keyring a leftover single key is not read, only
	// removed at the next save.
	if err := os.WriteFile(old, []byte("short"), 0o644); err != nil {
		t.Fatal(err)
	}
	r3, err := OpenKeyring(dir, "w1")
	if err != nil {
		t.Fatalf("a leftover single key beside a keyring stopped the agent: %v", err)
	}
	if _, err := r3.ForBoot(100); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatalf("the leftover single key is still on disk: %v", err)
	}
}

// TestVerifyingRotatesKeysByBoot: a hint verifies only with the key of the
// boot it names, so a key taken from one boot signs nothing for a later
// one, and the earlier boot's last hint keeps verifying until the later
// boot publishes.
func TestVerifyingRotatesKeysByBoot(t *testing.T) {
	ctx, p := context.Background(), "inst/w1"
	dir := t.TempDir()
	ring, err := OpenKeyring(dir, "w1")
	if err != nil {
		t.Fatal(err)
	}
	src := &rawStore{}
	v := &Verifying{Source: src, Keys: ring, Record: filepath.Join(dir, "w1.last")}
	k5, _ := ring.ForBoot(5)
	src.data = sign(k5, p, hintAt(5, 3))
	if h, ok, err := v.Read(ctx, p); err != nil || !ok || h.Generation != 5 {
		t.Fatalf("the first boot's hint: %+v, %v, %v", h, ok, err)
	}
	// Boot 5's key signs nothing for a generation no boot has yet, so it
	// can't get ahead of the next boot's hints.
	for _, g := range []uint64{6, 1000, 1 << 63} {
		src.data = sign(k5, p, hintAt(g, 1))
		if h, _, err := v.Read(ctx, p); !errors.Is(err, ErrUnauthenticated) || h.Generation != 5 {
			t.Fatalf("a hint for generation %d signed with boot 5's key, before any later boot: %+v, %v", g, h, err)
		}
	}
	src.data = sign(k5, p, hintAt(5, 3))
	// The next boot has its key; the sleeping guest's last hint still verifies.
	k6, _ := ring.ForBoot(6)
	if h, ok, err := v.Read(ctx, p); err != nil || !ok || h.Seq != 3 {
		t.Fatalf("the earlier boot's hint after the next boot's key was issued: %+v, %v, %v", h, ok, err)
	}
	// The earlier boot's key can't sign for the later boot, at any seq.
	src.data = sign(k5, p, hintAt(6, 99))
	if h, _, err := v.Read(ctx, p); !errors.Is(err, ErrUnauthenticated) || h.Generation != 5 {
		t.Fatalf("a hint for boot 6 signed with boot 5's key: %+v, %v", h, err)
	}
	src.data = sign(k5, p, hintAt(7, 1))
	if _, _, err := v.Read(ctx, p); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("a hint for an unissued generation signed with boot 5's key: %v", err)
	}
	// Nor can the later boot's key rewrite the earlier boot's hints.
	src.data = sign(k6, p, hintAt(5, 99))
	if _, _, err := v.Read(ctx, p); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("a hint for boot 5 signed with boot 6's key: %v", err)
	}
	// Once the later boot publishes, the earlier key is dropped.
	src.data = sign(k6, p, hintAt(6, 1))
	if h, ok, err := v.Read(ctx, p); err != nil || !ok || h.Generation != 6 {
		t.Fatalf("the later boot's hint: %+v, %v, %v", h, ok, err)
	}
	if ring.For(5) != nil {
		t.Fatal("the earlier boot's key was kept after the later boot's hint was accepted")
	}
	src.data = sign(k5, p, hintAt(5, 100))
	if h, _, err := v.Read(ctx, p); !errors.Is(err, ErrUnauthenticated) || h.Generation != 6 {
		t.Fatalf("the earlier boot's key after it was dropped: %+v, %v", h, err)
	}
	// A restarted agent reloads the keyring and the record, and still
	// refuses the earlier boot's key.
	ring2, err := OpenKeyring(dir, "w1")
	if err != nil {
		t.Fatal(err)
	}
	v2 := &Verifying{Source: src, Keys: ring2, Record: filepath.Join(dir, "w1.last")}
	if h, ok, err := v2.Read(ctx, p); !errors.Is(err, ErrFallback) || !ok || h.Generation != 6 {
		t.Fatalf("after a restart: %+v, %v, %v", h, ok, err)
	}
	if _, _, err := (&Verifying{Source: src, Keys: NewKeyring()}).Read(ctx, p); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("an empty keyring read a hint: %v", err)
	}
}

// TestKeyringReportsAFailedDrop: when dropping keys can't be saved, the
// caller is told, and the keys are gone from memory all the same.
func TestKeyringReportsAFailedDrop(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root writes to a read-only directory")
	}
	dir := t.TempDir()
	r, err := OpenKeyring(dir, "w1")
	if err != nil {
		t.Fatal(err)
	}
	var errs []error
	r.OnSaveError = func(err error) { errs = append(errs, err) }
	if _, err := r.ForBoot(5); err != nil {
		t.Fatal(err)
	}
	if _, err := r.ForBoot(6); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	r.Accepted(6)
	if len(errs) != 1 || r.For(5) != nil || r.For(6) == nil {
		t.Fatalf("a failed drop: %d errors reported, key 5 kept=%v", len(errs), r.For(5) != nil)
	}
}

// TestKeyringKeepsTheRecordedHintsKey: a hint served only through the
// record (the stored one is refused) keeps its key through the bound, so
// the record still verifies after another restart.
func TestKeyringKeepsTheRecordedHintsKey(t *testing.T) {
	ctx, p := context.Background(), "inst/w1"
	dir := t.TempDir()
	record := filepath.Join(dir, "w1.last")
	// An agent from before rotation left its key and its record.
	if err := os.WriteFile(filepath.Join(dir, "w1.key"), testKey(), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(record, sign(testKey(), p, hintAt(5, 3)), 0o600); err != nil {
		t.Fatal(err)
	}
	gone := &rawStore{data: []byte("tampered")}
	r, err := OpenKeyring(dir, "w1")
	if err != nil {
		t.Fatal(err)
	}
	if h, ok, err := (&Verifying{Source: gone, Keys: r, Record: record}).Read(ctx, p); !errors.Is(err, ErrFallback) || !ok || h.Seq != 3 {
		t.Fatalf("the recorded hint: %+v, %v, %v", h, ok, err)
	}
	for g := uint64(6); g < 6+maxRingKeys+2; g++ {
		if _, err := r.ForBoot(g); err != nil {
			t.Fatal(err)
		}
	}
	r2, err := OpenKeyring(dir, "w1")
	if err != nil {
		t.Fatal(err)
	}
	if h, ok, err := (&Verifying{Source: gone, Keys: r2, Record: record}).Read(ctx, p); !errors.Is(err, ErrFallback) || !ok || h.Seq != 3 {
		t.Fatalf("the recorded hint after the bound and a restart: %+v, %v, %v", h, ok, err)
	}
}
