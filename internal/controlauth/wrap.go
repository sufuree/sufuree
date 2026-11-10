package controlauth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
)

// KeyWrapper encrypts the CA key at rest (RFC-0001: KMS envelope custody
// for CA secrets). The context is bound into the ciphertext: unwrapping
// needs the same context.
type KeyWrapper interface {
	Wrap(ctx context.Context, plaintext []byte, context map[string]string) ([]byte, error)
	Unwrap(ctx context.Context, ciphertext []byte, context map[string]string) ([]byte, error)
	KeyID() string
}

// caKeyWrappedFile holds the wrapped CA key.
const caKeyWrappedFile = "ca.key.kms"

// wrappedKey is ca.key.kms.
type wrappedKey struct {
	Version    int    `json:"version"`
	KeyID      string `json:"kms_key_id"`
	Ciphertext []byte `json:"ciphertext"`
}

// wrapContext binds a wrapped CA key to its certificate: a key wrapped for
// one CA cannot be unwrapped beside another's certificate.
func wrapContext(certDER []byte) map[string]string {
	sum := sha256.Sum256(certDER)
	return map[string]string{"purpose": "sufuree-control-ca", "ca-cert-sha256": hex.EncodeToString(sum[:])}
}

// KMSClient is the part of the KMS API the wrapper uses.
type KMSClient interface {
	Encrypt(ctx context.Context, in *kms.EncryptInput, opts ...func(*kms.Options)) (*kms.EncryptOutput, error)
	Decrypt(ctx context.Context, in *kms.DecryptInput, opts ...func(*kms.Options)) (*kms.DecryptOutput, error)
}

// KMSWrapper wraps with one AWS KMS symmetric key.
type KMSWrapper struct {
	Client KMSClient
	Key    string // key ID, ARN or alias
}

// KeyID implements KeyWrapper.
func (w *KMSWrapper) KeyID() string { return w.Key }

// Wrap implements KeyWrapper.
func (w *KMSWrapper) Wrap(ctx context.Context, plaintext []byte, ec map[string]string) ([]byte, error) {
	out, err := w.Client.Encrypt(ctx, &kms.EncryptInput{KeyId: aws.String(w.Key), Plaintext: plaintext, EncryptionContext: ec})
	if err != nil {
		return nil, fmt.Errorf("controlauth: kms encrypt: %w", err)
	}
	return out.CiphertextBlob, nil
}

// Unwrap implements KeyWrapper. The key ID is pinned, so a ciphertext
// under another key is refused rather than decrypted with it.
func (w *KMSWrapper) Unwrap(ctx context.Context, ciphertext []byte, ec map[string]string) ([]byte, error) {
	out, err := w.Client.Decrypt(ctx, &kms.DecryptInput{KeyId: aws.String(w.Key), CiphertextBlob: ciphertext, EncryptionContext: ec})
	if err != nil {
		return nil, fmt.Errorf("controlauth: kms decrypt: %w", err)
	}
	return out.Plaintext, nil
}

func encodeWrapped(keyID string, ct []byte) ([]byte, error) {
	return json.Marshal(wrappedKey{Version: 1, KeyID: keyID, Ciphertext: ct})
}

func decodeWrapped(b []byte) (wrappedKey, error) {
	var w wrappedKey
	if err := json.Unmarshal(b, &w); err != nil || w.Version != 1 || len(w.Ciphertext) == 0 || w.KeyID == "" {
		return wrappedKey{}, errors.New("controlauth: ca.key.kms is not a wrapped CA key")
	}
	return w, nil
}

// WrappedKeyID returns the KMS key a wrapped CA key in dir was wrapped
// under, as recorded at init.
func WrappedKeyID(dir string) (string, error) {
	blob, err := readPrivate(filepath.Join(dir, caKeyWrappedFile))
	if err != nil {
		return "", err
	}
	w, err := decodeWrapped(blob)
	return w.KeyID, err
}
