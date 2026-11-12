// Package ec2live loads the opt-in live EC2 0↔1 qualification pin.
package ec2live

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	Schema = "sufuree.ec2-live-pin/v1"

	EnvEnable = "SUFUREE_QUAL_EC2"
	EnvPin    = "SUFUREE_QUAL_EC2_PIN"
)

// Pin is the approved launch + grant envelope for one live receipt.
type Pin struct {
	Schema            string `json:"schema"`
	Region            string `json:"region"`
	AMI               string `json:"ami"`
	AMIName           string `json:"ami_name,omitempty"`
	InstanceType      string `json:"instance_type"`
	SubnetID          string `json:"subnet_id"`
	SecurityGroupID   string `json:"security_group_id"`
	MemoryMiB         int    `json:"memory_mib"`
	GrantMaxHosts     int    `json:"grant_max_hosts"`
	GrantMaxMemoryMiB int    `json:"grant_max_memory_mib"`
	Notes             string `json:"notes,omitempty"`
}

// Enabled reports whether live EC2 qualification was opted in.
func Enabled() bool {
	v := strings.TrimSpace(os.Getenv(EnvEnable))
	return v == "1" || strings.EqualFold(v, "true") || strings.EqualFold(v, "yes")
}

// Load reads a pin JSON file.
func Load(path string) (Pin, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Pin{}, err
	}
	var p Pin
	if err := json.Unmarshal(b, &p); err != nil {
		return Pin{}, err
	}
	if err := p.Valid(); err != nil {
		return Pin{}, err
	}
	return p, nil
}

// Valid checks required fields and the hard spend envelope for this harness.
func (p Pin) Valid() error {
	switch {
	case p.Schema != Schema:
		return fmt.Errorf("ec2live: schema %q, want %q", p.Schema, Schema)
	case p.Region == "":
		return errors.New("ec2live: region is required")
	case !strings.HasPrefix(p.AMI, "ami-"):
		return errors.New("ec2live: ami is required")
	case p.InstanceType == "":
		return errors.New("ec2live: instance_type is required")
	case !strings.HasPrefix(p.SubnetID, "subnet-"):
		return errors.New("ec2live: subnet_id is required")
	case !strings.HasPrefix(p.SecurityGroupID, "sg-"):
		return errors.New("ec2live: security_group_id is required")
	case p.MemoryMiB < 32:
		return errors.New("ec2live: memory_mib must be >= 32")
	case p.GrantMaxHosts != 1:
		return fmt.Errorf("ec2live: grant_max_hosts must be 1 for this harness, got %d", p.GrantMaxHosts)
	case p.GrantMaxMemoryMiB < p.MemoryMiB:
		return errors.New("ec2live: grant_max_memory_mib must cover memory_mib")
	}
	return nil
}

// DefaultPinPath walks up from cwd to find the us-west-1 live pin.
func DefaultPinPath() string {
	if p := strings.TrimSpace(os.Getenv(EnvPin)); p != "" {
		return p
	}
	wd, err := os.Getwd()
	if err != nil {
		return ""
	}
	dir := wd
	for {
		candidate := filepath.Join(dir, "docs", "qualification", "pins", "ec2-live-us-west-1.json")
		if st, err := os.Stat(candidate); err == nil && !st.IsDir() {
			return candidate
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

// Resolve loads the default or env-selected pin.
func Resolve() (Pin, error) {
	path := DefaultPinPath()
	if path == "" {
		return Pin{}, errors.New("ec2live: pin file not found; set " + EnvPin)
	}
	return Load(path)
}
