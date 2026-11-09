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
	"time"
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
// than one it has accepted (lower generation, or the same generation with
// an earlier updated_at). That record is in memory, so the first hint read
// after an agent restart is accepted if its MAC verifies.

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
	if err := json.Unmarshal(e.Hint, &h); err != nil || h.Version != 2 {
		return Hint{}, errors.New("wakehint: the signed hint is not a version 2 hint")
	}
	return h, nil
}

// RawReader fetches a hint's published bytes.
type RawReader interface {
	ReadRaw(ctx context.Context, prefix string) ([]byte, bool, error)
}

// Verifying reads one workload's signed hints, refusing unsigned, altered
// and replayed ones. Use one per workload.
type Verifying struct {
	Source RawReader
	Key    []byte

	mu   sync.Mutex
	gen  uint64
	at   time.Time
	seen bool
}

// Read implements Reader.
func (v *Verifying) Read(ctx context.Context, prefix string) (Hint, bool, error) {
	if len(v.Key) != KeySize {
		return Hint{}, false, errors.New("wakehint: no hint key")
	}
	data, ok, err := v.Source.ReadRaw(ctx, prefix)
	if err != nil || !ok {
		return Hint{}, ok, err
	}
	h, err := Parse(data, prefix, v.Key)
	if err != nil {
		return Hint{}, false, fmt.Errorf("%s/wake.json: %w", prefix, err)
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.seen && (h.Generation < v.gen || (h.Generation == v.gen && h.UpdatedAt.Before(v.at))) {
		return Hint{}, false, fmt.Errorf("%s/wake.json: %w", prefix, ErrStale)
	}
	v.seen, v.gen, v.at = true, h.Generation, h.UpdatedAt
	return h, true, nil
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
	f, err := os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, fs.ErrExist) {
		return readKey(p) // created concurrently
	}
	if err != nil {
		return nil, err
	}
	_, werr := f.Write(k)
	serr := f.Sync()
	cerr := f.Close()
	if err := errors.Join(werr, serr, cerr); err != nil {
		_ = os.Remove(p)
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
