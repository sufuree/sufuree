// Package bootscope pins per-boot AWS session tags to installations a host
// is allowed to run. The host agent can assume a guest role, but it must not
// freely choose any inst/* tag: the controller either issues the session or
// publishes the only allowed tag for each placement.
package bootscope

import (
	"errors"
	"fmt"
	"strings"

	"github.com/sufuree/sufuree/internal/resource"
)

// ReplicaTagKey is the STS session tag that scopes a guest to one
// installation's resources (aws:PrincipalTag/replica).
const ReplicaTagKey = "replica"

// InstallationPrefix is the required value prefix for replica tags.
const InstallationPrefix = "inst/"

// ErrNotHeld means the host has no active placement for that installation.
var ErrNotHeld = errors.New("bootscope: host does not hold this installation")

// ErrTagMismatch means a requested session tag tried to select another
// installation (or an invalid inst/* value).
var ErrTagMismatch = errors.New("bootscope: session tag does not match the installation")

// TagValue returns the controller-pinned replica tag for installationID.
func TagValue(installationID string) string {
	return InstallationPrefix + installationID
}

// ParseInstallation returns the installation ID encoded in a replica tag
// value, or false if the value is not an installation tag.
func ParseInstallation(tagValue string) (string, bool) {
	if !strings.HasPrefix(tagValue, InstallationPrefix) {
		return "", false
	}
	id := strings.TrimPrefix(tagValue, InstallationPrefix)
	if id == "" || strings.Contains(id, "/") {
		return "", false
	}
	return id, true
}

// Active reports whether a placement still authorizes boot credentials for
// the assigned host. Draining placements keep credentials until stop; dormant
// and recovery-required do not authorize new boots.
func Active(p resource.Placement) bool {
	switch p.Phase {
	case resource.PhaseStarting, resource.PhaseReady, resource.PhaseDraining, resource.PhaseBudgetLimited:
		return true
	default:
		return false
	}
}

// HostHolds reports whether hostUID has an active placement whose replica
// (installation) identity is installationID.
func HostHolds(placements []resource.Placement, hostUID, installationID string) bool {
	for _, p := range placements {
		if p.HostUID != hostUID || p.Replica != installationID {
			continue
		}
		if Active(p) {
			return true
		}
	}
	return false
}

// PinSessionTags returns session tags for one boot. The replica tag is forced
// to this installation. Any other requested tag whose value looks like inst/*
// must name the same installation; otherwise the call fails closed.
// requested may be nil.
func PinSessionTags(installationID string, requested map[string]string) (map[string]string, error) {
	if installationID == "" {
		return nil, errors.New("bootscope: installation id is required")
	}
	want := TagValue(installationID)
	out := make(map[string]string, len(requested)+1)
	for k, v := range requested {
		if id, ok := ParseInstallation(v); ok && id != installationID {
			return nil, fmt.Errorf("%w: %s=%s", ErrTagMismatch, k, v)
		}
		if k == ReplicaTagKey && v != "" && v != want {
			return nil, fmt.Errorf("%w: replica=%s", ErrTagMismatch, v)
		}
		out[k] = v
	}
	out[ReplicaTagKey] = want
	return out, nil
}

// Authorize returns pinned session tags when the host holds an active
// placement for the installation. Otherwise it returns ErrNotHeld.
func Authorize(placements []resource.Placement, hostUID, installationID string, requested map[string]string) (map[string]string, error) {
	if !HostHolds(placements, hostUID, installationID) {
		return nil, ErrNotHeld
	}
	return PinSessionTags(installationID, requested)
}

// Assignment is what the controller publishes for one host placement so the
// agent (or a later credential broker) can apply the pinned replica tag.
type Assignment struct {
	Placement   resource.Placement `json:"placement"`
	SessionTags map[string]string  `json:"session_tags"`
}

// AssignmentsForHost builds assignment records with controller-pinned session
// tags for every active placement on hostUID.
func AssignmentsForHost(placements []resource.Placement, hostUID string) []Assignment {
	out := make([]Assignment, 0, len(placements))
	for _, p := range placements {
		if p.HostUID != hostUID || !Active(p) {
			continue
		}
		tags, err := PinSessionTags(p.Replica, nil)
		if err != nil {
			continue
		}
		out = append(out, Assignment{Placement: p, SessionTags: tags})
	}
	return out
}
