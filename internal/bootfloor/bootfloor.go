// Package bootfloor keeps each installation's boot generation H and rollback
// floor on the controller, so both survive the loss of any host (Zati RFC
// 007 §7.4, option A).
//
// A host asks the controller for every boot (Allocate): the controller first
// merges the host's own record into its own, then commits H = previous + 1
// as a fresh Raft entry, and returns H with the floor and its incarnation.
// No cached authorization is ever reused for H. Hosts report progress in
// batches (Report). The floor only rises within an incarnation; only an
// audited lowering (Lower: the installation owner requests, a different
// Zati operator approves) starts a new incarnation with a lower floor.
//
// Merging a host record follows one rule everywhere:
//   - H always takes the maximum: raising it is always safe;
//   - an older incarnation's floor is dropped, so a host that missed a
//     rollback cannot raise the floor above its target;
//   - the same incarnation keeps the maximum floor;
//   - a newer incarnation's floor is never taken: the host's record is
//     only a claim, and adopting it would lower the floor without the
//     audited two-person action. The controller is behind (its journal was
//     restored from a backup taken before a lowering), so the boot is
//     refused (ErrBehindHost) until an operator re-applies the lowering
//     through Lower, which records the audit again.
//
// A controller with no record seeds it from the first host entry in
// incarnation 1, so a standalone host enrols without losing its H or floor.
// An entry claiming a later incarnation seeds only H and counts as ahead.
//
// Commands are applied deterministically by the controller FSM (Apply).
// Whether the host holds the installation is checked by the leader under
// the journal's write lock, before it proposes.
package bootfloor

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/sufuree/sufuree/internal/resource"
	"github.com/sufuree/sufuree/internal/store"
)

// Op is the controller FSM command that carries a Command.
const Op = "boot_floor"

// Command kinds.
const (
	KindAllocate = "allocate"
	KindReport   = "report"
	KindLower    = "lower"
	// Two-person lowering on the authenticated API: one principal
	// requests, a different one approves exactly that request.
	KindRequestLower = "request_lower"
	KindApproveLower = "approve_lower"
)

// MaxGeneration bounds any generation a host may claim, so a bad host
// record cannot exhaust an installation's generations.
const MaxGeneration = 1 << 48

// MaxReportEntries bounds one report command.
const MaxReportEntries = 5000

var (
	// ErrConflict means the record changed between the leader's read and
	// the commit; the host retries.
	ErrConflict = errors.New("bootfloor: record changed; retry")
	// ErrStaleEpoch means the command was planned under an older owner
	// epoch than the store now holds.
	ErrStaleEpoch = errors.New("bootfloor: owner epoch changed")
	// ErrBehindHost means a host holds a newer floor incarnation than the
	// controller: the controller missed a lowering (it was restored from an
	// older backup). Boots are refused until the lowering is re-applied.
	ErrBehindHost = errors.New("bootfloor: the controller is behind this host's floor incarnation; re-apply the lowering")
	// ErrNotFound means there is no floor record for the installation.
	ErrNotFound = errors.New("bootfloor: no record for this installation")
)

// Entry is one installation's record as a host holds it: the floor
// incarnation it knows, the highest H it has, and its floor.
type Entry struct {
	Installation string `json:"installation"`
	Incarnation  uint64 `json:"incarnation"`
	// IncarnationFrom is the first generation of Incarnation, as granted.
	IncarnationFrom uint64 `json:"incarnation_from,omitempty"`
	Generation      uint64 `json:"generation"`
	Epoch           uint64 `json:"epoch"`
	Seq             uint64 `json:"seq"`
}

// Lowering is an intentional rollback. Epoch and Seq must be the restored
// lease's exact (data generation, TXID).
type Lowering struct {
	ExpectIncarnation uint64 `json:"expect_incarnation"`
	// TargetIncarnation is the new incarnation; zero means the next one.
	// Re-applying a lowering a restored controller missed sets it to the
	// incarnation the hosts hold.
	TargetIncarnation uint64 `json:"target_incarnation,omitempty"`
	Epoch             uint64 `json:"epoch"`
	Seq               uint64 `json:"seq"`
	RequestedBy       string `json:"requested_by"`
	ApprovedBy        string `json:"approved_by"`
	Reason            string `json:"reason"`
	Reference         string `json:"reference"`
	// At is set by the controller when it accepts the request.
	At time.Time `json:"at"`
	// PendingDigest names the request an approval approves.
	PendingDigest string `json:"pending_digest,omitempty"`
}

// Command is one boot_floor FSM command.
type Command struct {
	Kind       string `json:"kind"`
	OwnerEpoch uint64 `json:"owner_epoch"`
	// Allocate: the installation, the record's H the leader read, and the
	// host's own record.
	Installation string `json:"installation,omitempty"`
	Expect       uint64 `json:"expect,omitempty"`
	Local        *Entry `json:"local,omitempty"`
	// Report: entries the leader has checked the host holds.
	Entries []Entry `json:"entries,omitempty"`
	// Lower: the audited rollback.
	Lower *Lowering `json:"lower,omitempty"`
}

// Outcome of merging one host entry.
type Outcome int

const (
	Unchanged Outcome = iota // nothing new
	Raised                   // H or the floor rose
	Ahead                    // the host holds a newer incarnation; only H was taken
	Created                  // no record existed
)

func (o Outcome) String() string {
	return [...]string{"unchanged", "raised", "ahead", "created"}[o]
}

func progressBelow(e1, s1, e2, s2 uint64) bool {
	return e1 < e2 || (e1 == e2 && s1 < s2)
}

// Merge applies one host entry to rec (nil when the controller has none)
// under the package rule. The entry's installation must be rec's.
func Merge(rec *resource.BootFloor, e Entry) (resource.BootFloor, Outcome) {
	inc := max(e.Incarnation, 1)
	// A host record never holds progress above an H it issued; clamp a
	// malformed one instead of letting it break the record's invariant.
	if e.Generation > MaxGeneration {
		e.Generation, e.Epoch, e.Seq = 0, 0, 0
	}
	epoch, seq := e.Epoch, e.Seq
	if epoch > e.Generation {
		epoch, seq = 0, 0
	}
	if rec == nil {
		if inc > 1 {
			// Only an audited lowering creates a later incarnation. A host
			// claiming one (the controller lost the record, say restored
			// from an older backup) seeds H alone, in incarnation 1; its
			// floor is never taken, or a grant would carry the claimed
			// incarnation and lower honest hosts' floors.
			return resource.BootFloor{Installation: e.Installation, Generation: e.Generation, Incarnation: 1}, Ahead
		}
		return resource.BootFloor{
			Installation: e.Installation, Generation: e.Generation, Incarnation: inc,
			IncarnationFrom: e.IncarnationFrom, ProgressEpoch: epoch, ProgressSeq: seq,
		}, Created
	}
	out := *rec
	o := Unchanged
	if e.Generation > out.Generation {
		out.Generation, o = e.Generation, Raised
	}
	switch {
	case inc < rec.Incarnation:
		return out, o
	case inc > rec.Incarnation:
		return out, Ahead
	}
	if progressBelow(out.ProgressEpoch, out.ProgressSeq, epoch, seq) {
		out.ProgressEpoch, out.ProgressSeq, o = epoch, seq, Raised
	}
	return out, o
}

// PlanAllocate returns the record after an allocation, and how the host's
// entry merged. H is the result's Generation. rec is nil when the
// controller has no record; expect must then be 0.
func PlanAllocate(rec *resource.BootFloor, c Command) (resource.BootFloor, Outcome, error) {
	if c.Installation == "" {
		return resource.BootFloor{}, Unchanged, errors.New("bootfloor: allocate needs an installation")
	}
	var have uint64
	if rec != nil {
		have = rec.Generation
	}
	if have != c.Expect {
		return resource.BootFloor{}, Unchanged, ErrConflict
	}
	var out resource.BootFloor
	o := Unchanged
	switch {
	case c.Local != nil:
		local := *c.Local
		local.Installation = c.Installation
		out, o = Merge(rec, local)
		if o == Ahead {
			return resource.BootFloor{}, o, ErrBehindHost
		}
	case rec != nil:
		out = *rec
	default:
		out, o = resource.BootFloor{Installation: c.Installation, Incarnation: 1}, Created
	}
	if out.Generation == ^uint64(0) {
		return resource.BootFloor{}, Unchanged, errors.New("bootfloor: generation exhausted")
	}
	out.Generation++
	if out.StartPending() {
		out.IncarnationFrom = out.Generation
	}
	return out, o, out.Validate()
}

// PlanLower returns the record after an audited lowering.
func PlanLower(rec resource.BootFloor, l Lowering) (resource.BootFloor, error) {
	if l.ExpectIncarnation != rec.Incarnation {
		return resource.BootFloor{}, ErrConflict
	}
	target := l.TargetIncarnation
	if target == 0 {
		target = rec.Incarnation + 1
	}
	if target <= rec.Incarnation {
		return resource.BootFloor{}, errors.New("bootfloor: the target incarnation must be above the current one")
	}
	if l.Epoch > rec.Generation {
		return resource.BootFloor{}, errors.New("bootfloor: the restored mark names a generation never issued")
	}
	audit := resource.FloorLowering{
		RequestedBy: l.RequestedBy, ApprovedBy: l.ApprovedBy, Reason: l.Reason, Reference: l.Reference, At: l.At,
		FromIncarnation: rec.Incarnation, FromEpoch: rec.ProgressEpoch, FromSeq: rec.ProgressSeq,
	}
	if err := audit.Validate(); err != nil {
		return resource.BootFloor{}, err
	}
	out := rec
	out.Incarnation = target
	out.IncarnationFrom = 0 // StartPending: set by the next allocation
	out.ProgressEpoch, out.ProgressSeq = l.Epoch, l.Seq
	out.Lowered = &audit
	out.Pending = nil
	return out, out.Validate()
}

// PlanRequestLower records a requested lowering on rec, replacing any
// earlier request. It is checked as far as it can be before approval.
func PlanRequestLower(rec resource.BootFloor, l Lowering) (resource.BootFloor, error) {
	if l.ExpectIncarnation != rec.Incarnation {
		return resource.BootFloor{}, ErrConflict
	}
	target := l.TargetIncarnation
	if target == 0 {
		target = rec.Incarnation + 1
	}
	if l.Epoch > rec.Generation {
		return resource.BootFloor{}, errors.New("bootfloor: the restored mark names a generation never issued")
	}
	out := rec
	out.Pending = &resource.PendingLowering{
		RequestedBy: l.RequestedBy, ExpectIncarnation: l.ExpectIncarnation, TargetIncarnation: target,
		Epoch: l.Epoch, Seq: l.Seq, Reason: l.Reason, Reference: l.Reference, At: l.At,
	}
	return out, out.Validate()
}

// ErrNoMatchingRequest means an approval does not match the pending
// request (or there is none, or it has expired).
var ErrNoMatchingRequest = errors.New("bootfloor: no pending lowering matches this approval")

// PendingTTL is how long a lowering request may wait for approval.
const PendingTTL = 24 * time.Hour

// PendingDigest identifies one pending request exactly: who asked, for
// whom, why, and what. An approval names it, so a later request that
// replaces the pending one (even with the same mark) is not approved by an
// approval meant for the first.
func PendingDigest(p resource.PendingLowering) string {
	b, _ := json.Marshal(p)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// PlanApproveLower applies the pending lowering that l repeats, approved by
// l.ApprovedBy, who must not be its requester.
func PlanApproveLower(rec resource.BootFloor, l Lowering) (resource.BootFloor, error) {
	p := rec.Pending
	if p == nil || l.PendingDigest == "" || PendingDigest(*p) != l.PendingDigest {
		return resource.BootFloor{}, ErrNoMatchingRequest
	}
	if l.At.Sub(p.At) > PendingTTL {
		return resource.BootFloor{}, fmt.Errorf("%w: the request expired", ErrNoMatchingRequest)
	}
	return PlanLower(rec, Lowering{
		ExpectIncarnation: p.ExpectIncarnation, TargetIncarnation: p.TargetIncarnation,
		Epoch: p.Epoch, Seq: p.Seq, RequestedBy: p.RequestedBy, ApprovedBy: l.ApprovedBy,
		Reason: p.Reason, Reference: p.Reference, At: l.At,
	})
}

// Encode marshals a command for the FSM.
func Encode(c Command) ([]byte, error) { return json.Marshal(c) }

// Decode strictly unmarshals an FSM command payload.
func Decode(payload []byte) (Command, error) {
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.DisallowUnknownFields()
	var c Command
	if err := dec.Decode(&c); err != nil {
		return Command{}, fmt.Errorf("bootfloor: command: %w", err)
	}
	if _, err := dec.Token(); err != io.EOF {
		return Command{}, errors.New("bootfloor: command: trailing data")
	}
	return c, nil
}

func getFloor(tx *store.Tx, id string) (*resource.BootFloor, error) {
	f, err := tx.GetBootFloor(id)
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &f, nil
}

// Apply runs one command in tx. It is the controller FSM's only writer of
// floor records, so it must stay deterministic.
func Apply(tx *store.Tx, payload []byte) error {
	c, err := Decode(payload)
	if err != nil {
		return err
	}
	meta, err := tx.GetMeta()
	if err != nil {
		return err
	}
	if c.OwnerEpoch != meta.OwnerEpoch {
		return ErrStaleEpoch
	}
	switch c.Kind {
	case KindAllocate:
		rec, err := getFloor(tx, c.Installation)
		if err != nil {
			return err
		}
		out, _, err := PlanAllocate(rec, c)
		if err != nil {
			return err
		}
		return tx.PutBootFloor(out)
	case KindReport:
		if len(c.Entries) > MaxReportEntries {
			return fmt.Errorf("bootfloor: report has %d entries; at most %d", len(c.Entries), MaxReportEntries)
		}
		for _, e := range c.Entries {
			rec, err := getFloor(tx, e.Installation)
			if err != nil {
				return err
			}
			out, o := Merge(rec, e)
			if o == Unchanged || (o == Ahead && rec != nil && out.Generation == rec.Generation) {
				continue
			}
			if err := tx.PutBootFloor(out); err != nil {
				return err
			}
		}
		return nil
	case KindLower, KindRequestLower, KindApproveLower:
		if c.Lower == nil {
			return errors.New("bootfloor: lower needs a lowering")
		}
		rec, err := getFloor(tx, c.Installation)
		if err != nil {
			return err
		}
		if rec == nil {
			return ErrNotFound
		}
		plan := map[string]func(resource.BootFloor, Lowering) (resource.BootFloor, error){
			KindLower: PlanLower, KindRequestLower: PlanRequestLower, KindApproveLower: PlanApproveLower,
		}[c.Kind]
		out, err := plan(*rec, *c.Lower)
		if err != nil {
			return err
		}
		return tx.PutBootFloor(out)
	default:
		return fmt.Errorf("bootfloor: unknown command kind %q", c.Kind)
	}
}
