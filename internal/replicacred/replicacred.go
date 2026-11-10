// Package replicacred gives each installation its own object-store
// credentials, limited to that installation's replica prefix.
//
// A guest gets its replica credentials at boot. With one credential shared
// by every guest, one compromised guest could read every installation's
// replica. Here the controller derives each installation's credentials
// from one provisioning key:
//
//	access key = HMAC(key, "ak", version, installation)
//	secret key = HMAC(key, "sk", version, installation)
//
// So no secret is stored in the controller's Raft state. Before handing the
// credentials to a host, it registers them with the object store as an
// identity that may only read, write, list and tag
// <bucket>/inst/<installation>/*, and checks the store accepts them
// (Registrar). Hosts get them through POST
// /v1/hosts/{id}/credentials/{installation}, which serves only
// installations the host holds.
//
// Rotation: raising the key version derives new credentials for every
// installation. Each installation's identity is replaced, and the old key
// revoked, at that installation's next boot, when its previous guest has
// stopped.
package replicacred

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base32"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// KeySize is the provisioning key length in bytes.
const KeySize = 32

// AccessKeyPrefix starts every derived access key, so they're recognizable
// in object-store logs.
const AccessKeyPrefix = "SFR"

var installationPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// Credentials are one installation's object-store credentials.
type Credentials struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
}

// Deriver derives installations' credentials from a provisioning key.
type Deriver struct {
	key     []byte
	version uint32
}

// NewDeriver copies key. version starts at 1 and is raised to rotate.
func NewDeriver(key []byte, version uint32) (*Deriver, error) {
	if len(key) != KeySize {
		return nil, fmt.Errorf("replicacred: the provisioning key must be %d bytes", KeySize)
	}
	if version == 0 {
		return nil, errors.New("replicacred: the key version starts at 1")
	}
	return &Deriver{key: append([]byte(nil), key...), version: version}, nil
}

// Version is the key version credentials are derived under.
func (d *Deriver) Version() uint32 { return d.version }

// ValidInstallation reports whether id can name an identity and a prefix.
func ValidInstallation(id string) error {
	if !installationPattern.MatchString(id) || strings.Contains(id, "..") {
		return fmt.Errorf("replicacred: installation %q is not valid", id)
	}
	return nil
}

func (d *Deriver) mac(label, installation string) []byte {
	m := hmac.New(sha256.New, d.key)
	var v [4]byte
	binary.BigEndian.PutUint32(v[:], d.version)
	m.Write([]byte("sufuree-replica-credentials\x00" + label + "\x00"))
	m.Write(v[:])
	m.Write([]byte(installation))
	return m.Sum(nil)
}

// For returns installation's credentials under the current version.
func (d *Deriver) For(installation string) (Credentials, error) {
	if err := ValidInstallation(installation); err != nil {
		return Credentials{}, err
	}
	ak := base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(d.mac("ak", installation))
	return Credentials{
		AccessKeyID:     AccessKeyPrefix + ak[:17],
		SecretAccessKey: base64.RawURLEncoding.EncodeToString(d.mac("sk", installation)),
	}, nil
}

// Prefix is installation's replica prefix inside the bucket. It matches the
// replica session tag value (bootscope.TagValue).
func Prefix(installation string) string { return "inst/" + installation }

// IdentityName names installation's object-store identity.
func IdentityName(installation string) string { return "sufuree-" + installation }

// identity is a SeaweedFS identity file (/etc/iam/identities/<name>.json).
type identity struct {
	Name        string           `json:"name"`
	Credentials []identityCredit `json:"credentials"`
	Actions     []string         `json:"actions"`
}

type identityCredit struct {
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	Status    string `json:"status"`
}

// Identity returns installation's identity document: its one credential
// may read, write, list and tag only <bucket>/inst/<installation>/*.
func Identity(bucket, installation string, c Credentials) ([]byte, error) {
	if err := ValidInstallation(installation); err != nil {
		return nil, err
	}
	if bucket == "" || strings.ContainsAny(bucket, "/*:") {
		return nil, fmt.Errorf("replicacred: bucket %q is not valid", bucket)
	}
	scope := bucket + "/" + Prefix(installation) + "/*"
	return json.Marshal(identity{
		Name:        IdentityName(installation),
		Credentials: []identityCredit{{AccessKey: c.AccessKeyID, SecretKey: c.SecretAccessKey, Status: "Active"}},
		Actions:     []string{"Read:" + scope, "Write:" + scope, "List:" + scope, "Tagging:" + scope},
	})
}
