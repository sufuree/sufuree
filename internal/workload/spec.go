// Package workload defines the versioned workload contract a host agent
// admits (RFC-0001 §4). Unknown or unsupported fields fail admission.
package workload

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
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
	// HintPrefix is the object-store prefix the guest publishes
	// <prefix>/wake.json under.
	HintPrefix string `json:"hint_prefix,omitempty"`

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
	case w.GenerationEnvKey == "":
		return bad("generation_env_key is required")
	case w.IdleTimeout.Duration <= 0, w.StartupTimeout.Duration <= 0:
		return bad("idle_timeout and startup_timeout must be positive")
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
		out[k] = val
	}
	out[w.GenerationEnvKey] = fmt.Sprint(generation)
	return out, nil
}
