package wakehint

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// Hint keys rotate at every boot.
//
// A Keyring holds one workload's hint keys on the host. Every boot gets a
// new key, bound to that boot's generation: a hint verifies only with the
// key of the boot whose generation it names, so a guest must name its boot
// generation in every signed hint. A key taken from one guest therefore
// signs nothing for any other boot, issued or not, and restarting a
// workload replaces its key.
//
// Older keys are kept only while a hint they signed can still be the
// newest: once a hint from a later boot is accepted, earlier hints are
// stale whatever their MAC, and their keys are dropped.

// maxRingKeys bounds a keyring whose boots never publish a hint (boots that
// keep failing). The key of the last accepted hint is always kept; of the
// others the oldest go first, and a hint they signed is then refused as
// unauthenticated.
const maxRingKeys = 16

// Keyring is one workload's hint keys. Its methods are safe for concurrent
// use.
type Keyring struct {
	path string // "" keeps the keys in memory only
	// OnSaveError, when set, is told when dropping keys could not be saved
	// (for logging). The keys are gone from memory; a restart before a
	// later save brings them back. Set it before the keyring is used.
	OnSaveError func(error)

	mu       sync.Mutex
	keys     []ringKey // ascending by generation; a legacy key is first
	accepted uint64    // generation of the newest accepted hint
	seen     bool      // a hint has been accepted
	legacy   string    // a pre-rotation key file to remove once the ring is saved
}

type ringKey struct {
	Generation uint64 `json:"generation"`
	Key        string `json:"key"` // hex
	// Legacy marks the single key from before rotation. It covers every
	// generation below the first boot under the keyring (generation 0).
	Legacy bool `json:"legacy,omitempty"`
	// Below, on the legacy key, is the first boot's generation, fixed when
	// that boot's key is created: the legacy key covers only generations
	// below it, whatever keys are dropped later. 0 means no boot yet.
	Below uint64 `json:"below,omitempty"`
	raw   []byte
}

type ringFile struct {
	Version int       `json:"version"`
	Keys    []ringKey `json:"keys"`
	// Accepted is the generation of the newest accepted hint, whose key is
	// never evicted.
	Accepted *uint64 `json:"accepted,omitempty"`
}

// maxRingFile bounds the keyring file read at start.
const maxRingFile = 16 << 10

// NewKeyring returns an empty keyring kept in memory only (tests, and
// agents that keep no state).
func NewKeyring() *Keyring { return &Keyring{} }

// OpenKeyring loads workload id's keyring from dir (0700), or starts an
// empty one. The file is dir/<id>.keys, mode 0600; keys never leave the
// host except in that workload's boot env. A single key left by an agent
// from before rotation (dir/<id>.key) is adopted, when the keyring is new,
// for the generations before the first boot under this keyring, so the hint
// a sleeping workload last published still verifies. Until that first boot
// it verifies a hint naming any generation, as it did before rotation.
func OpenKeyring(dir, id string) (*Keyring, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, err
	}
	k := &Keyring{path: filepath.Join(dir, id+".keys")}
	data, err := readPrivate(k.path, maxRingFile)
	switch {
	case err == nil:
		var f ringFile
		if err := json.Unmarshal(data, &f); err != nil || f.Version != 1 {
			return nil, fmt.Errorf("hint keyring %s is not a version 1 keyring", k.path)
		}
		for i := range f.Keys {
			raw, err := hex.DecodeString(f.Keys[i].Key)
			if err != nil || len(raw) != KeySize {
				return nil, fmt.Errorf("hint keyring %s holds a key that is not %d bytes", k.path, KeySize)
			}
			if i > 0 && f.Keys[i].Generation <= f.Keys[i-1].Generation {
				return nil, fmt.Errorf("hint keyring %s is not in generation order", k.path)
			}
			if f.Keys[i].Legacy != (i == 0 && f.Keys[i].Generation == 0) || (!f.Keys[i].Legacy && f.Keys[i].Below != 0) {
				return nil, fmt.Errorf("hint keyring %s has a key at generation %d that is not a boot's", k.path, f.Keys[i].Generation)
			}
			f.Keys[i].raw = raw
		}
		if n := len(f.Keys); n > 1 && f.Keys[0].Legacy && (f.Keys[0].Below == 0 || f.Keys[0].Below > f.Keys[1].Generation) {
			return nil, fmt.Errorf("hint keyring %s has a single key that covers a boot's generation", k.path)
		}
		k.keys = f.Keys
		if f.Accepted != nil {
			k.accepted, k.seen = *f.Accepted, true
		}
	case !errors.Is(err, fs.ErrNotExist):
		return nil, err
	}
	// A leftover single key is adopted when the keyring is new. Beside an
	// existing keyring it is only removed, unread, at the next save.
	legacy := filepath.Join(dir, id+".key")
	if len(k.keys) > 0 || k.seen {
		if _, err := os.Lstat(legacy); err == nil {
			k.legacy = legacy
		}
		return k, nil
	}
	switch raw, err := readKey(legacy); {
	case err == nil:
		k.legacy = legacy
		k.keys = []ringKey{{Key: hex.EncodeToString(raw), Legacy: true, raw: raw}}
	case !errors.Is(err, fs.ErrNotExist):
		return nil, err
	}
	return k, nil
}

// ForBoot creates the key for the boot at generation and returns it. The
// keyring is saved before it returns, so a guest never holds a key the
// host could lose. generation must be above every earlier boot's.
func (k *Keyring) ForBoot(generation uint64) ([]byte, error) {
	if generation == 0 {
		return nil, errors.New("wakehint: a boot's hint key needs its generation")
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if n := len(k.keys); n > 0 && generation <= k.keys[n-1].Generation {
		return nil, fmt.Errorf("wakehint: generation %d is not above the keyring's newest boot (%d)", generation, k.keys[n-1].Generation)
	}
	raw := make([]byte, KeySize)
	if _, err := rand.Read(raw); err != nil {
		return nil, err
	}
	keys := append(append([]ringKey(nil), k.keys...), ringKey{Generation: generation, Key: hex.EncodeToString(raw), raw: raw})
	if keys[0].Legacy && keys[0].Below == 0 {
		keys[0].Below = generation
	}
	// Over the bound, the oldest key goes, except the one that verifies
	// the last accepted hint: the host still falls back to that hint.
	for len(keys) > maxRingKeys {
		drop := 0
		if k.seen && drop == indexFor(keys, k.accepted) {
			drop = 1
		}
		keys = append(keys[:drop:drop], keys[drop+1:]...)
	}
	if err := k.save(keys); err != nil {
		return nil, fmt.Errorf("wakehint: saving the hint keyring: %w", err)
	}
	k.keys = keys
	return append([]byte(nil), raw...), nil
}

// For returns the key that signs hints naming generation: the key of the
// boot at exactly that generation, or nil when the keyring has none. The
// single key from before rotation covers the generations below the first
// boot.
func (k *Keyring) For(generation uint64) []byte {
	k.mu.Lock()
	defer k.mu.Unlock()
	if i := indexFor(k.keys, generation); i >= 0 {
		return k.keys[i].raw
	}
	return nil
}

// Accepted notes that a hint naming generation was accepted. Keys of
// earlier boots are dropped: any hint they signed is now stale. If that
// cannot be saved, OnSaveError is told and the keys are gone from memory at
// once. The next boot's key, or an accepted hint from a later boot, saves
// the keyring again; another hint from the same boot does not.
func (k *Keyring) Accepted(generation uint64) {
	k.mu.Lock()
	defer k.mu.Unlock()
	i := indexFor(k.keys, generation)
	if i < 0 || (k.seen && k.accepted == generation) {
		return
	}
	k.keys = append([]ringKey(nil), k.keys[i:]...)
	k.accepted, k.seen = generation, true
	if err := k.save(k.keys); err != nil && k.OnSaveError != nil {
		k.OnSaveError(fmt.Errorf("hint keyring %s: %w", k.path, err))
	}
}

// indexFor returns the index in keys of the key for generation, or -1: a
// boot's key at exactly that generation, or the legacy key for a
// generation below the first boot (any generation while there is no boot).
func indexFor(keys []ringKey, generation uint64) int {
	for i := len(keys) - 1; i >= 0; i-- {
		switch {
		case keys[i].Legacy:
			if keys[i].Below == 0 || generation < keys[i].Below {
				return i
			}
		case keys[i].Generation == generation:
			return i
		}
	}
	return -1
}

// save replaces the keyring file with keys, then removes an adopted
// single-key file. k.mu must be held.
func (k *Keyring) save(keys []ringKey) error {
	if k.path == "" {
		return nil
	}
	f := ringFile{Version: 1, Keys: keys}
	if k.seen {
		f.Accepted = &k.accepted
	}
	data, err := json.Marshal(f)
	if err != nil {
		return err
	}
	if err := writePrivate(k.path, append(data, '\n')); err != nil {
		return err
	}
	if k.legacy != "" {
		if err := os.Remove(k.legacy); err == nil || errors.Is(err, fs.ErrNotExist) {
			k.legacy = ""
		}
	}
	return nil
}
