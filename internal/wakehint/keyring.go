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
// new key, bound to that boot's generation: a hint naming generation g is
// verified with the key of the newest boot at or below g. A key taken from
// one guest therefore signs nothing for a later boot, and restarting a
// workload replaces its key.
//
// Older keys are kept only while a hint they signed can still be the
// newest: once a hint from a later boot is accepted, earlier hints are
// stale whatever their MAC, and their keys are dropped.

// maxRingKeys bounds a keyring whose boots never publish a hint (boots that
// keep failing). The oldest keys go first; a hint they signed is then
// refused as unauthenticated.
const maxRingKeys = 16

// Keyring is one workload's hint keys. Its methods are safe for concurrent
// use.
type Keyring struct {
	path string // "" keeps the keys in memory only

	mu     sync.Mutex
	keys   []ringKey // ascending by generation
	legacy string    // a pre-rotation key file to remove once the ring is saved
}

type ringKey struct {
	Generation uint64 `json:"generation"`
	Key        string `json:"key"` // hex
	raw        []byte
}

type ringFile struct {
	Version int       `json:"version"`
	Keys    []ringKey `json:"keys"`
}

// maxRingFile bounds the keyring file read at start.
const maxRingFile = 16 << 10

// NewKeyring returns an empty keyring kept in memory only (tests, and
// agents that keep no state).
func NewKeyring() *Keyring { return &Keyring{} }

// OpenKeyring loads workload id's keyring from dir (0700), or starts an
// empty one. The file is dir/<id>.keys, mode 0600; keys never leave the
// host except in that workload's boot env. A single key left by an agent
// from before rotation (dir/<id>.key) is adopted for the generations before
// the first boot under this keyring, so the hint a sleeping workload last
// published still verifies.
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
			f.Keys[i].raw = raw
		}
		k.keys = f.Keys
	case !errors.Is(err, fs.ErrNotExist):
		return nil, err
	}
	// A leftover single key is adopted when the keyring is new, and removed
	// in any case once the keyring is saved.
	legacy := filepath.Join(dir, id+".key")
	switch raw, err := readKey(legacy); {
	case err == nil:
		k.legacy = legacy
		if len(k.keys) == 0 {
			k.keys = []ringKey{{Generation: 0, Key: hex.EncodeToString(raw), raw: raw}}
		}
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
	if len(keys) > maxRingKeys {
		keys = keys[len(keys)-maxRingKeys:]
	}
	if err := k.save(keys); err != nil {
		return nil, fmt.Errorf("wakehint: saving the hint keyring: %w", err)
	}
	k.keys = keys
	return append([]byte(nil), raw...), nil
}

// For returns the key that signs hints naming generation: the key of the
// newest boot at or below it, or nil when there is none.
func (k *Keyring) For(generation uint64) []byte {
	k.mu.Lock()
	defer k.mu.Unlock()
	if i := k.covering(generation); i >= 0 {
		return k.keys[i].raw
	}
	return nil
}

// Accepted notes that a hint naming generation was accepted. Keys of
// earlier boots are dropped: any hint they signed is now stale. A failed
// save is retried by the next change; the keys are gone from memory at
// once.
func (k *Keyring) Accepted(generation uint64) {
	k.mu.Lock()
	defer k.mu.Unlock()
	i := k.covering(generation)
	if i <= 0 {
		return
	}
	k.keys = append([]ringKey(nil), k.keys[i:]...)
	_ = k.save(k.keys)
}

// covering returns the index of the newest key at or below generation, or
// -1. k.mu must be held.
func (k *Keyring) covering(generation uint64) int {
	for i := len(k.keys) - 1; i >= 0; i-- {
		if k.keys[i].Generation <= generation {
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
	data, err := json.Marshal(ringFile{Version: 1, Keys: keys})
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
