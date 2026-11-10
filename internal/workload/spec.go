// Package workload defines the versioned workload contract a host agent
// admits (RFC-0001 §4). Unknown or unsupported fields fail admission.
package workload

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// HostConfig is one host's admitted workloads and shared settings.
type HostConfig struct {
	Version int `json:"version"`
	// DataDir holds the durable journal and guest volumes.
	DataDir string `json:"data_dir"`
	// SocketDir holds one gateway socket per workload (<id>.sock, 0600).
	SocketDir string `json:"socket_dir"`
	// WakeHints locates each workload's published hint
	// (<prefix>/wake.json). Optional: without it there is no scheduled
	// wake and idle stop is never vetoed by guest work.
	WakeHints *ObjectStore `json:"wake_hints,omitempty"`
	Workloads []Spec       `json:"workloads"`
}

// ObjectStore is an S3-compatible endpoint. Credentials come from the
// environment (SUFUREE_HINT_ACCESS_KEY, SUFUREE_HINT_SECRET_KEY).
type ObjectStore struct {
	Endpoint string `json:"endpoint"`
	Region   string `json:"region,omitempty"`
	Bucket   string `json:"bucket"`
	// CAFile is a PEM bundle trusted in addition to the system roots, for
	// a store with a private CA.
	CAFile string `json:"ca_file,omitempty"`
}

// AWSRole issues each boot short-lived AWS credentials for one role,
// scoped to this workload. The agent assumes the role with its own
// credentials (the default AWS chain, such as the instance profile), tags
// the session with SessionTags, and names it <id>-g<generation>, so the
// role's policy can limit the guest to its own resources
// (aws:PrincipalTag) and every use is attributable to a workload and a
// generation. The credentials reach the guest in the boot env under the
// keys in Env. They are for start-up: they are not refreshed, so a guest
// awake longer than Duration must not depend on them after that.
type AWSRole struct {
	RoleARN     string            `json:"role_arn"`
	Region      string            `json:"region"`
	SessionTags map[string]string `json:"session_tags"`
	// Duration is the credentials' lifetime: 15m to 1h (default 15m).
	Duration Duration   `json:"duration,omitempty"`
	Env      AWSRoleEnv `json:"env"`
}

// AWSRoleEnv names the boot env keys that receive the credentials.
type AWSRoleEnv struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	SessionToken    string `json:"session_token"`
}

// Keys returns the three env keys.
func (e AWSRoleEnv) Keys() []string {
	return []string{e.AccessKeyID, e.SecretAccessKey, e.SessionToken}
}

// Spec is one stateful unikernel workload.
type Spec struct {
	ID string `json:"id"`
	// Image is the bootable guest image (shared, opened read-only with a
	// per-boot overlay).
	Image     string `json:"image"`
	MemoryMiB int    `json:"memory_mib"`
	// GuestPort is the TCP port the workload serves inside the guest.
	GuestPort int `json:"guest_port"`
	// StateMiB sizes the persistent state volume (label "state").
	StateMiB int `json:"state_mib"`
	// BootEnv is written per boot to the boot volume (label "boot") as
	// <BootEnvFile>, with SUFUREE_GENERATION_KEY set to the boot's
	// generation. Values starting with "env:" are read from the agent's
	// environment so secrets stay out of the config file.
	BootEnvFile      string            `json:"boot_env_file"`
	BootEnv          map[string]string `json:"boot_env"`
	GenerationEnvKey string            `json:"generation_env_key"`
	// BootFiles are extra files on the boot volume (name -> content), for
	// material a workload reads from a file, such as a key. Values use the
	// same "env:NAME" indirection as BootEnv.
	BootFiles map[string]string `json:"boot_files,omitempty"`
	// BootCredentials, when set, issues per-boot credentials (see AWSRole).
	BootCredentials *AWSRole `json:"boot_credentials,omitempty"`
	// HintPrefix is the object-store prefix the guest publishes
	// <prefix>/wake.json under.
	HintPrefix string `json:"hint_prefix,omitempty"`
	// ProgressEnvKey, when set, receives the highest progress the hints
	// have reported, as "<epoch>:<seq>", on every boot after the first
	// report (see wakehint.Progress). Requires hint_prefix and
	// hint_key_env_key: the guest refuses to boot below that progress, so
	// it is taken only from signed hints, or anyone who can write the hint
	// store could block every boot with a forged one.
	ProgressEnvKey string `json:"progress_env_key,omitempty"`
	// HintKeyEnvKey, when set, receives this workload's hint key (hex),
	// generated and kept by the host, on every boot; the host then accepts
	// only hints signed with it (see wakehint.Verifying). Requires
	// hint_prefix.
	HintKeyEnvKey string `json:"hint_key_env_key,omitempty"`

	IdleTimeout    Duration `json:"idle_timeout"`
	StartupTimeout Duration `json:"startup_timeout"`
	// ScheduledLead wakes a sleeping workload this long before its
	// published next-due instant.
	ScheduledLead Duration `json:"scheduled_lead"`
	// MinAwake keeps a workload up at least this long after a wake, so a
	// scheduled wake has time to admit its work and publish "busy".
	MinAwake Duration `json:"min_awake"`
}

// Duration is a JSON string such as "90s".
type Duration struct{ time.Duration }

func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.Duration = v
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(d.String()) }

var idPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)

// Load reads and validates a host config. Unknown fields are refused.
func Load(path string) (HostConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return HostConfig{}, err
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var c HostConfig
	if err := dec.Decode(&c); err != nil {
		return HostConfig{}, fmt.Errorf("host config: %w", err)
	}
	return c, c.Validate()
}

// Validate checks the config shape; it does not touch the filesystem.
func (c HostConfig) Validate() error {
	if c.Version != 1 {
		return fmt.Errorf("host config: version %d is not supported (want 1)", c.Version)
	}
	if c.DataDir == "" || c.SocketDir == "" {
		return errors.New("host config: data_dir and socket_dir are required")
	}
	if !filepath.IsAbs(c.DataDir) || !filepath.IsAbs(c.SocketDir) {
		return errors.New("host config: data_dir and socket_dir must be absolute paths")
	}
	if strings.Contains(c.DataDir, ",") {
		return errors.New("host config: data_dir must not contain a comma (QEMU option syntax)")
	}
	if len(c.Workloads) == 0 {
		return errors.New("host config: no workloads")
	}
	seen := map[string]bool{}
	for _, w := range c.Workloads {
		if err := w.validate(); err != nil {
			return err
		}
		if seen[w.ID] {
			return fmt.Errorf("workload %q: duplicate id", w.ID)
		}
		seen[w.ID] = true
		if w.HintPrefix != "" && c.WakeHints == nil {
			return fmt.Errorf("workload %q: hint_prefix needs wake_hints", w.ID)
		}
	}
	return nil
}

func (w Spec) validate() error {
	bad := func(msg string) error { return fmt.Errorf("workload %q: %s", w.ID, msg) }
	switch {
	case !idPattern.MatchString(w.ID):
		return bad("id must be lowercase letters, digits and dashes")
	case w.Image == "":
		return bad("image is required")
	case w.MemoryMiB < 32:
		return bad("memory_mib must be at least 32")
	case w.GuestPort < 1 || w.GuestPort > 65535:
		return bad("guest_port is out of range")
	case w.StateMiB < 16:
		return bad("state_mib must be at least 16")
	case !plainName(w.BootEnvFile):
		return bad("boot_env_file must be a plain file name")
	case w.GenerationEnvKey == "" || strings.ContainsAny(w.GenerationEnvKey, "=\n\r"):
		return bad("generation_env_key is required and must be a plain key")
	case strings.Contains(w.Image, ","):
		return bad("image path must not contain a comma (QEMU option syntax)")
	case w.IdleTimeout.Duration <= 0, w.StartupTimeout.Duration <= 0:
		return bad("idle_timeout and startup_timeout must be positive")
	case w.ProgressEnvKey != "" && w.HintPrefix == "":
		return bad("progress_env_key needs hint_prefix")
	case w.ProgressEnvKey != "" && w.HintKeyEnvKey == "":
		return bad("progress_env_key needs hint_key_env_key (progress is taken only from signed hints)")
	case strings.ContainsAny(w.ProgressEnvKey, "=\n\r"), w.ProgressEnvKey != "" && w.ProgressEnvKey == w.GenerationEnvKey:
		return bad("progress_env_key must be a plain key other than generation_env_key")
	}
	if _, clash := w.BootEnv[w.ProgressEnvKey]; clash && w.ProgressEnvKey != "" {
		return bad("progress_env_key must not also be in boot_env")
	}
	if k := w.HintKeyEnvKey; k != "" {
		_, inEnv := w.BootEnv[k]
		switch {
		case w.HintPrefix == "":
			return bad("hint_key_env_key needs hint_prefix")
		case strings.ContainsAny(k, "=\n\r"), inEnv, k == w.GenerationEnvKey, k == w.ProgressEnvKey:
			return bad("hint_key_env_key must be a plain key not used by boot_env, generation_env_key or progress_env_key")
		}
	}
	for name := range w.BootFiles {
		if !plainName(name) || name == w.BootEnvFile {
			return bad("boot_files names must be plain file names other than boot_env_file")
		}
	}
	for k, v := range w.BootEnv {
		if k == "" || strings.ContainsAny(k, "=\n") || strings.Contains(v, "\n") {
			return bad("boot_env keys and values must be single-line KEY=VALUE")
		}
	}
	if c := w.BootCredentials; c != nil {
		// <id>-g<generation> must fit STS's 64 characters untruncated, so
		// every session name stays unique.
		if len(w.ID) > 64-len("-g")-20 {
			return bad("ids longer than 42 characters cannot use boot_credentials")
		}
		if err := c.validate(w); err != nil {
			return bad(err.Error())
		}
	}
	return nil
}

// Session tag limits (AWS STS).
var (
	tagKeyPattern   = regexp.MustCompile(`^[\p{L}\p{Z}\p{N}_.:/=+\-@]{1,128}$`)
	tagValuePattern = regexp.MustCompile(`^[\p{L}\p{Z}\p{N}_.:/=+\-@]{0,256}$`)
)

func (c *AWSRole) validate(w Spec) error {
	switch {
	case !strings.HasPrefix(c.RoleARN, "arn:") || !strings.Contains(c.RoleARN, ":role/"):
		return errors.New("boot_credentials.role_arn must be an IAM role ARN")
	case c.Region == "":
		return errors.New("boot_credentials.region is required")
	case len(c.SessionTags) == 0:
		return errors.New("boot_credentials.session_tags must scope the session (at least one tag)")
	case len(c.SessionTags) > 50:
		return errors.New("boot_credentials.session_tags allows at most 50 tags")
	case c.Duration.Duration != 0 && (c.Duration.Duration < 15*time.Minute || c.Duration.Duration > time.Hour):
		return errors.New("boot_credentials.duration must be between 15m and 1h")
	}
	for k, v := range c.SessionTags {
		if !tagKeyPattern.MatchString(k) || !tagValuePattern.MatchString(v) {
			return fmt.Errorf("boot_credentials.session_tags %q is not a valid STS tag", k)
		}
	}
	seen := map[string]bool{}
	for _, k := range c.Env.Keys() {
		_, inEnv := w.BootEnv[k]
		if k == "" || strings.ContainsAny(k, "=\n") || seen[k] || inEnv || k == w.GenerationEnvKey || k == w.ProgressEnvKey || k == w.HintKeyEnvKey {
			return errors.New("boot_credentials.env needs three distinct keys not used by boot_env, generation_env_key, progress_env_key or hint_key_env_key")
		}
		seen[k] = true
	}
	return nil
}

func plainName(s string) bool {
	return s != "" && !strings.ContainsAny(s, "/\\") && !strings.HasPrefix(s, ".")
}

func (w Spec) resolve(field, v string, getenv func(string) (string, bool)) (string, error) {
	name, ok := strings.CutPrefix(v, "env:")
	if !ok {
		return v, nil
	}
	val, ok := getenv(name)
	if !ok {
		return "", fmt.Errorf("workload %q: %s needs %s in the agent environment", w.ID, field, name)
	}
	return val, nil
}

// ResolveBootFiles returns the boot files with "env:NAME" values resolved.
func (w Spec) ResolveBootFiles(getenv func(string) (string, bool)) (map[string][]byte, error) {
	out := make(map[string][]byte, len(w.BootFiles))
	for name, v := range w.BootFiles {
		val, err := w.resolve("boot_files "+name, v, getenv)
		if err != nil {
			return nil, err
		}
		out[name] = []byte(val)
	}
	return out, nil
}

// ResolveBootEnv returns the boot env with "env:NAME" values read from the
// agent's environment and the generation key set.
func (w Spec) ResolveBootEnv(generation uint64, getenv func(string) (string, bool)) (map[string]string, error) {
	out := make(map[string]string, len(w.BootEnv)+1)
	for k, v := range w.BootEnv {
		val, err := w.resolve("boot_env "+k, v, getenv)
		if err != nil {
			return nil, err
		}
		if strings.ContainsAny(val, "\n\r") {
			return nil, fmt.Errorf("workload %q: boot_env %s resolves to a multi-line value", w.ID, k)
		}
		out[k] = val
	}
	out[w.GenerationEnvKey] = fmt.Sprint(generation)
	return out, nil
}
