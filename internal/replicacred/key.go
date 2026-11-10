package replicacred

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// Purpose is the KMS encryption context purpose a wrapped provisioning key
// is bound to. The key policy allows decryption only with it.
const Purpose = "sufuree-replica-credentials"

func wrapContext() map[string]string { return map[string]string{"purpose": Purpose} }

// Wrapper encrypts the provisioning key at rest (controlauth.KMSWrapper).
type Wrapper interface {
	Wrap(ctx context.Context, plaintext []byte, context map[string]string) (ciphertext []byte, keyID string, err error)
}

// Unwrapper decrypts it (controlauth.KMSWrapper).
type Unwrapper interface {
	Unwrap(ctx context.Context, ciphertext []byte, context map[string]string) ([]byte, error)
}

// keyFile is the provisioning key file: wrapped (KMSKeyID and Ciphertext)
// or, for development only, plaintext (Key).
type keyFile struct {
	Version    int    `json:"version"`
	KMSKeyID   string `json:"kms_key_id,omitempty"`
	Ciphertext []byte `json:"ciphertext,omitempty"`
	Key        string `json:"key,omitempty"`
}

// InitKey creates a new provisioning key at path (0600), wrapped by w, or in
// plaintext when w is nil (development only). It refuses to replace a file.
func InitKey(ctx context.Context, path string, w Wrapper) error {
	key := make([]byte, KeySize)
	if _, err := rand.Read(key); err != nil {
		return err
	}
	defer clear(key)
	f := keyFile{Version: 1}
	if w != nil {
		ct, id, err := w.Wrap(ctx, key, wrapContext())
		if err != nil {
			return err
		}
		if id == "" {
			return errors.New("replicacred: the wrapper returned no key ID")
		}
		f.KMSKeyID, f.Ciphertext = id, ct
	} else {
		f.Key = hex.EncodeToString(key)
	}
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	return writeExclusive(path, append(b, '\n'))
}

// KeyID returns the KMS key a key file's content (blob) was wrapped under,
// or "" for a plaintext key. The caller reads the file and must refuse one
// others can read (controlauth.ReadPrivate).
func KeyID(blob []byte) (string, error) {
	f, err := decodeKeyFile(blob)
	return f.KMSKeyID, err
}

// LoadKey returns the provisioning key in blob (a key file's content). A
// wrapped key is unwrapped with u, pinned to the recorded KMS key; a
// plaintext key needs u to be nil, so a PHI configuration that expects KMS
// can't silently load a plaintext key.
func LoadKey(ctx context.Context, blob []byte, u Unwrapper) ([]byte, error) {
	f, err := decodeKeyFile(blob)
	if err != nil {
		return nil, err
	}
	var key []byte
	switch {
	case f.KMSKeyID != "":
		if u == nil {
			return nil, errors.New("replicacred: the key file is KMS-wrapped but no unwrapper is configured")
		}
		if key, err = u.Unwrap(ctx, f.Ciphertext, wrapContext()); err != nil {
			return nil, err
		}
	default:
		if u != nil {
			return nil, errors.New("replicacred: the key file is plaintext, not KMS-wrapped")
		}
		if key, err = hex.DecodeString(f.Key); err != nil {
			return nil, errors.New("replicacred: the key file's key is not hex")
		}
	}
	if len(key) != KeySize {
		clear(key)
		return nil, fmt.Errorf("replicacred: the provisioning key is not %d bytes", KeySize)
	}
	return key, nil
}

func decodeKeyFile(blob []byte) (keyFile, error) {
	var f keyFile
	wrapped := func() bool { return f.KMSKeyID != "" && len(f.Ciphertext) > 0 && f.Key == "" }
	plain := func() bool { return f.KMSKeyID == "" && len(f.Ciphertext) == 0 && f.Key != "" }
	if err := json.Unmarshal(blob, &f); err != nil || f.Version != 1 || !(wrapped() || plain()) {
		return keyFile{}, errors.New("replicacred: not a provisioning key file")
	}
	return f, nil
}

func writeExclusive(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, werr := f.Write(data)
	serr := f.Sync()
	cerr := f.Close()
	if err := errors.Join(werr, serr, cerr); err != nil {
		_ = os.Remove(path)
		return err
	}
	return nil
}
