package wakehint

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// Signed hints (version 2).
//
// The host trusts a hint for scheduling (busy, next_due) and keeps its
// progress as the workload's rollback floor. Unsigned, anyone who can write
// the hint store could forge one: report busy so a workload is never
// stopped, move next_due so scheduled work is missed, or report progress
// that blocks every boot. A workload with hint_key_env_key gets a key the
// host generates and keeps (never in the hint store), delivered in its boot
// env, and signs every hint:
//
//	{"version":2,"hint":<hint>,"mac":"<hex>"}
//
// mac is HMAC-SHA256(key, MACLabel ‖ 0 ‖ prefix ‖ 0 ‖ <hint>) over the exact
// <hint> bytes, and <hint> carries "version":2. Binding the prefix keeps one
// workload's hint from being copied over another's.
//
// Verifying refuses unsigned or altered hints and replays: a hint older
// than one it has accepted, ordered by (generation, seq). seq is the
// guest's per-boot publish counter, so a guest clock that steps backward
// does not make its later hints look stale. With Record set, the newest
// accepted hint is also kept on the host's disk and reloaded (and verified
// again) after an agent restart, so a restart does not reopen a replay of
// an older signed hint. Without Record that state is in memory only.

// MACLabel is the MAC domain separator, shared with workloads.
const MACLabel = "sufuree wake hint v2"

// KeySize is the hint key length in bytes.
const KeySize = 32

// ErrUnauthenticated reports a hint that is unsigned or fails its MAC.
var ErrUnauthenticated = errors.New("wake hint is not authenticated")

// ErrStale reports a validly signed hint older than one already accepted.
var ErrStale = errors.New("wake hint is older than one already accepted (replayed?)")

type envelope struct {
	Version int             `json:"version"`
	Hint    json.RawMessage `json:"hint"`
	MAC     string          `json:"mac"`
}

// MAC authenticates hint bytes for the workload whose hints are at prefix.
func MAC(key []byte, prefix string, hint []byte) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(MACLabel))
	m.Write([]byte{0})
	m.Write([]byte(prefix))
	m.Write([]byte{0})
	m.Write(hint)
	return m.Sum(nil)
}

// Parse decodes a published hint. With a key it accepts only a version 2
// envelope whose MAC verifies for prefix; without one it accepts version 1
// or the hint inside a version 2 envelope, unverified.
func Parse(data []byte, prefix string, key []byte) (Hint, error) {
	var probe struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(data, &probe); err != nil {
		return Hint{}, errors.New("wakehint: not a hint")
	}
	if probe.Version == 1 {
		if key != nil {
			return Hint{}, fmt.Errorf("%w: version 1 hints are unsigned", ErrUnauthenticated)
		}
		var h Hint
		if err := json.Unmarshal(data, &h); err != nil {
			return Hint{}, errors.New("wakehint: not a version 1 hint")
		}
		return h, nil
	}
	if probe.Version != 2 {
		return Hint{}, fmt.Errorf("wakehint: unsupported hint version %d", probe.Version)
	}
	var e envelope
	if err := json.Unmarshal(data, &e); err != nil || len(e.Hint) == 0 {
		return Hint{}, errors.New("wakehint: not a version 2 hint")
	}
	if key != nil {
		mac, err := hex.DecodeString(e.MAC)
		if err != nil || !hmac.Equal(mac, MAC(key, prefix, e.Hint)) {
			return Hint{}, ErrUnauthenticated
		}
	}
	var h Hint
	if err := json.Unmarshal(e.Hint, &h); err != nil || h.Version != 2 || h.Seq == 0 {
		return Hint{}, errors.New("wakehint: the signed hint is not a version 2 hint with a seq")
	}
	return h, nil
}

// RawReader fetches a hint's published bytes.
type RawReader interface {
	ReadRaw(ctx context.Context, prefix string) ([]byte, bool, error)
}

// Verifying reads one workload's signed hints, refusing unsigned, altered
// and replayed ones. Use one per workload.
//
// It keeps the newest authentic hint it has accepted. When the stored hint
// is then missing, unsigned, altered or older, Read returns that hint, ok,
// and an error wrapping ErrFallback (and the refusal). A caller deciding
// what a sleeping workload needs can use the hint: its hint does not
// change, so deleting or replacing wake.json cannot suppress a scheduled
// wake. A caller vetoing an idle stop must treat it as unreadable, so a
// store that keeps serving refused hints cannot hold an old "busy" forever.
// Before any hint is accepted, a missing hint is ok=false and an invalid
// one an error.
type Verifying struct {
	Source RawReader
	Key    []byte
	// OnRefused, when set, is told of refused stored hints (for logging):
	// every missing, unsigned or altered one, and an older one once it has
	// been refused staleReportAfter times in a row (one alone can be a
	// concurrent read's older bytes).
	OnRefused func(error)
	// Record, when set, is a host-local file that keeps the newest accepted
	// hint's signed bytes across agent restarts. It belongs beside the key
	// (a private directory), never in the hint store.
	Record string
	// OnRecordError, when set, is told when Record cannot be read or
	// written. Neither is fatal: the hint is still used, and the cost is
	// that the next restart starts without the record.
	OnRecordError func(error)

	mu       sync.Mutex
	last     Hint
	seen     bool
	stale    int  // consecutive stale refusals
	recorded bool // Record has been loaded
}

// maxRecord bounds the record file read back at start.
const maxRecord = 64 << 10

// staleReportAfter is how many consecutive stale refusals are reported.
const staleReportAfter = 3

// ErrFallback marks a Read that returned the last accepted hint because
// the stored one was refused.
var ErrFallback = errors.New("using the last authentic wake hint")

// Read implements Reader.
func (v *Verifying) Read(ctx context.Context, prefix string) (Hint, bool, error) {
	if len(v.Key) != KeySize {
		return Hint{}, false, errors.New("wakehint: no hint key")
	}
	data, ok, err := v.Source.ReadRaw(ctx, prefix)
	if err != nil {
		return Hint{}, false, err
	}
	var h Hint
	switch {
	case !ok:
		err = fmt.Errorf("%s/wake.json: %w: the hint is missing", prefix, ErrUnauthenticated)
	default:
		if h, err = Parse(data, prefix, v.Key); err != nil {
			err = fmt.Errorf("%s/wake.json: %w", prefix, err)
		}
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	v.loadRecord(prefix)
	if err == nil && v.seen && (h.Generation < v.last.Generation || (h.Generation == v.last.Generation && h.Seq < v.last.Seq)) {
		err = fmt.Errorf("%s/wake.json: %w", prefix, ErrStale)
	}
	if err == nil {
		if !v.seen || h.Generation != v.last.Generation || h.Seq != v.last.Seq {
			v.saveRecord(data)
		}
		v.seen, v.last, v.stale = true, h, 0
		return h, true, nil
	}
	if !v.seen {
		if !ok {
			return Hint{}, false, nil
		}
		return Hint{}, false, err
	}
	report := true
	if errors.Is(err, ErrStale) {
		v.stale++
		report = v.stale >= staleReportAfter
	} else {
		v.stale = 0
	}
	if report && v.OnRefused != nil {
		v.OnRefused(err)
	}
	return v.last, true, fmt.Errorf("%w: %w", ErrFallback, err)
}

// loadRecord reloads the newest accepted hint once, verifying it again with
// the key and prefix. v.mu must be held.
func (v *Verifying) loadRecord(prefix string) {
	if v.Record == "" || v.recorded {
		return
	}
	v.recorded = true
	data, err := readPrivate(v.Record, maxRecord)
	if errors.Is(err, fs.ErrNotExist) {
		return
	}
	if err == nil {
		var h Hint
		if h, err = Parse(data, prefix, v.Key); err == nil {
			v.seen, v.last = true, h
			return
		}
	}
	v.recordError(fmt.Errorf("wake hint record %s: %w", v.Record, err))
}

// saveRecord replaces the record with data, the signed bytes of a newly
// accepted hint. v.mu must be held.
func (v *Verifying) saveRecord(data []byte) {
	if v.Record == "" {
		return
	}
	if err := writePrivate(v.Record, data); err != nil {
		v.recordError(fmt.Errorf("wake hint record %s: %w", v.Record, err))
	}
}

func (v *Verifying) recordError(err error) {
	if v.OnRecordError != nil {
		v.OnRecordError(err)
	}
}

// writePrivate replaces path with data (mode 0600) by writing a synced
// temporary file and renaming it into place, so a crash leaves the old
// record or the new one, never a partial one.
func writePrivate(path string, data []byte) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, "."+filepath.Base(path)+"-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer func() { _ = os.Remove(tmp) }()
	cherr := f.Chmod(0o600)
	_, werr := f.Write(data)
	serr := f.Sync()
	cerr := f.Close()
	if err := errors.Join(cherr, werr, serr, cerr); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// readPrivate reads a regular file readable only by its owner, refusing a
// symlink and anything over limit bytes.
func readPrivate(p string, limit int64) ([]byte, error) {
	f, err := os.OpenFile(p, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() || st.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("%s must be a regular file readable only by its owner", p)
	}
	data, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("%s is larger than %d bytes", p, limit)
	}
	return data, nil
}

// LoadOrCreateKey returns workload id's hint key from dir, creating it
// (random, mode 0600, in a 0700 directory) on first use. The key never
// leaves the host except in that workload's boot env.
func LoadOrCreateKey(dir, id string) ([]byte, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, err
	}
	p := filepath.Join(dir, id+".key")
	if k, err := readKey(p); err == nil || !errors.Is(err, fs.ErrNotExist) {
		return k, err
	}
	k := make([]byte, KeySize)
	if _, err := rand.Read(k); err != nil {
		return nil, err
	}
	// Written in full to a temporary file and linked into place, so a crash
	// never leaves a partial key behind.
	f, err := os.CreateTemp(dir, "."+id+".key-*")
	if err != nil {
		return nil, err
	}
	tmp := f.Name()
	defer func() { _ = os.Remove(tmp) }()
	cherr := f.Chmod(0o600)
	_, werr := f.Write(k)
	serr := f.Sync()
	cerr := f.Close()
	if err := errors.Join(cherr, werr, serr, cerr); err != nil {
		return nil, err
	}
	if err := os.Link(tmp, p); errors.Is(err, fs.ErrExist) {
		return readKey(p) // created concurrently
	} else if err != nil {
		return nil, err
	}
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return k, nil
}

func readKey(p string) ([]byte, error) {
	f, err := os.OpenFile(p, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() || st.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("hint key %s must be a regular file readable only by its owner", p)
	}
	k, err := io.ReadAll(io.LimitReader(f, KeySize+1))
	if err != nil {
		return nil, err
	}
	if len(k) != KeySize {
		return nil, fmt.Errorf("hint key %s is not %d bytes", p, KeySize)
	}
	return k, nil
}
