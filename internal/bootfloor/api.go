package bootfloor

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"time"

	"github.com/sufuree/sufuree/internal/bootscope"
	"github.com/sufuree/sufuree/internal/raftnode"
	"github.com/sufuree/sufuree/internal/resource"
	"github.com/sufuree/sufuree/internal/store"
)

// Committer is the controller journal (controlraft.Journal): reads, and
// commits of boot_floor commands planned under its write lock.
type Committer interface {
	View(func(*store.Tx) error) error
	Commit(plan func(*store.Tx) ([]byte, error)) error
}

// Grant is the controller's answer to a boot request: the H to boot with,
// and the floor with its incarnation.
type Grant struct {
	Installation    string `json:"installation"`
	Generation      uint64 `json:"generation"`
	Incarnation     uint64 `json:"incarnation"`
	IncarnationFrom uint64 `json:"incarnation_from,omitempty"`
	ProgressEpoch   uint64 `json:"progress_epoch"`
	ProgressSeq     uint64 `json:"progress_seq"`
}

// BootRequest is a host's boot request body.
type BootRequest struct {
	Local *Entry `json:"local,omitempty"`
}

// ReportRequest is a host's batched progress report.
type ReportRequest struct {
	Entries []Entry `json:"entries"`
}

// ReportResult lists the entries the controller did not take: NotHeld
// because the host does not hold those installations, Ahead because the
// host holds a newer floor incarnation (ErrBehindHost; only H was taken).
type ReportResult struct {
	Accepted int      `json:"accepted"`
	NotHeld  []string `json:"not_held,omitempty"`
	Ahead    []string `json:"ahead,omitempty"`
}

// LowerRequest is the audited rollback request body.
type LowerRequest = Lowering

// Routes:
//
//	POST /v1/hosts/{id}/boot/{installation}       allocate H for one boot
//	POST /v1/hosts/{id}/progress                  batched progress report
//	GET  /v1/installations/{installation}/floor    read the record
//	POST /v1/installations/{installation}/floor/lower  audited lowering
//
// The host ID in these paths is not authenticated yet; see the package
// README note in docs. Mount only with the controller's boot-floor flag.
func Register(mux *http.ServeMux, j Committer, now func() time.Time) {
	RegisterWithIdentity(mux, j, now, nil)
}

// Identify returns the authenticated principal of a request.
type Identify func(*http.Request) (string, bool)

// RegisterWithIdentity is Register for an authenticated control API. With
// identify set, the single-call lowering (whose names are only asserted) is
// not served; instead:
//
//	POST /v1/installations/{installation}/floor/lower-request  (product client, for an owner)
//	POST /v1/installations/{installation}/floor/lower-approve  (a different principal)
//
// The request records its authenticated principal and the owner it acts
// for; the approval records its own principal and must repeat the
// request's incarnations, mark and reference.
func RegisterWithIdentity(mux *http.ServeMux, j Committer, now func() time.Time, identify Identify) {
	if now == nil {
		now = time.Now
	}
	mux.HandleFunc("POST /v1/hosts/{id}/boot/{installation}", func(w http.ResponseWriter, r *http.Request) {
		postBoot(j, w, r)
	})
	mux.HandleFunc("POST /v1/hosts/{id}/progress", func(w http.ResponseWriter, r *http.Request) {
		postReport(j, w, r)
	})
	mux.HandleFunc("GET /v1/installations/{installation}/floor", func(w http.ResponseWriter, r *http.Request) {
		var f resource.BootFloor
		err := j.View(func(tx *store.Tx) error {
			var err error
			f, err = tx.GetBootFloor(r.PathValue("installation"))
			return err
		})
		if err != nil {
			writeErr(w, err)
			return
		}
		writeJSON(w, http.StatusOK, viewOf(f))
	})
	if identify == nil {
		mux.HandleFunc("POST /v1/installations/{installation}/floor/lower", func(w http.ResponseWriter, r *http.Request) {
			postLower(j, now, w, r)
		})
		return
	}
	mux.HandleFunc("POST /v1/installations/{installation}/floor/lower-request", func(w http.ResponseWriter, r *http.Request) {
		who, ok := identify(r)
		if !ok {
			http.Error(w, "unauthenticated", http.StatusUnauthorized)
			return
		}
		var req LowerRequestBody
		if err := readBody(r, 1<<14, &req); err != nil {
			http.Error(w, "lower request: "+err.Error(), http.StatusBadRequest)
			return
		}
		if !ownerPattern.MatchString(req.Owner) {
			http.Error(w, "lower request: owner is required (the installation owner the product client authenticated)", http.StatusBadRequest)
			return
		}
		l := Lowering{ExpectIncarnation: req.ExpectIncarnation, TargetIncarnation: req.TargetIncarnation,
			Epoch: req.Epoch, Seq: req.Seq, Reason: req.Reason, Reference: req.Reference,
			RequestedBy: who + " for owner " + req.Owner, At: now().UTC()}
		commitLowering(j, w, r.PathValue("installation"), KindRequestLower, l, PlanRequestLower)
	})
	mux.HandleFunc("POST /v1/installations/{installation}/floor/lower-approve", func(w http.ResponseWriter, r *http.Request) {
		who, ok := identify(r)
		if !ok {
			http.Error(w, "unauthenticated", http.StatusUnauthorized)
			return
		}
		var req ApproveBody
		if err := readBody(r, 1<<14, &req); err != nil {
			http.Error(w, "lower approval: "+err.Error(), http.StatusBadRequest)
			return
		}
		l := Lowering{PendingDigest: req.PendingDigest, ApprovedBy: who, At: now().UTC()}
		commitLowering(j, w, r.PathValue("installation"), KindApproveLower, l, PlanApproveLower)
	})
}

var ownerPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9.@_:-]{0,255}$`)

// LowerRequestBody is a product client's lowering request.
type LowerRequestBody struct {
	Owner             string `json:"owner"`
	ExpectIncarnation uint64 `json:"expect_incarnation"`
	TargetIncarnation uint64 `json:"target_incarnation,omitempty"`
	Epoch             uint64 `json:"epoch"`
	Seq               uint64 `json:"seq"`
	Reason            string `json:"reason"`
	Reference         string `json:"reference"`
}

// ApproveBody names the exact pending request being approved: its digest,
// as GET .../floor returns it in pending_digest.
type ApproveBody struct {
	PendingDigest string `json:"pending_digest"`
}

// FloorView is the floor record with the pending request's digest.
type FloorView struct {
	Floor         resource.BootFloor `json:"floor"`
	PendingDigest string             `json:"pending_digest,omitempty"`
}

func viewOf(f resource.BootFloor) FloorView {
	v := FloorView{Floor: f}
	if f.Pending != nil {
		v.PendingDigest = PendingDigest(*f.Pending)
	}
	return v
}

func commitLowering(j Committer, w http.ResponseWriter, inst, kind string, l Lowering,
	plan func(resource.BootFloor, Lowering) (resource.BootFloor, error)) {
	var out resource.BootFloor
	err := j.Commit(func(tx *store.Tx) ([]byte, error) {
		meta, err := tx.GetMeta()
		if err != nil {
			return nil, err
		}
		rec, err := getFloor(tx, inst)
		if err != nil {
			return nil, err
		}
		if rec == nil {
			return nil, ErrNotFound
		}
		if out, err = plan(*rec, l); err != nil {
			return nil, err
		}
		return Encode(Command{Kind: kind, OwnerEpoch: meta.OwnerEpoch, Installation: inst, Lower: &l})
	})
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, viewOf(out))
}

func readBody(r *http.Request, limit int64, v any) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		return err
	}
	if int64(len(body)) > limit {
		return errors.New("request body is too large")
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	return dec.Decode(v)
}

// heldBy returns the installations hostID holds through an active placement.
func heldBy(tx *store.Tx, hostID string) (map[string]bool, error) {
	ps, err := tx.ListPlacements()
	if err != nil {
		return nil, err
	}
	held := map[string]bool{}
	for _, p := range ps {
		if p.HostUID == hostID && bootscope.Active(p) {
			held[p.Replica] = true
		}
	}
	return held, nil
}

func postBoot(j Committer, w http.ResponseWriter, r *http.Request) {
	hostID, inst := r.PathValue("id"), r.PathValue("installation")
	var req BootRequest
	if err := readBody(r, 1<<12, &req); err != nil {
		http.Error(w, "boot request: "+err.Error(), http.StatusBadRequest)
		return
	}
	var grant Grant
	err := j.Commit(func(tx *store.Tx) ([]byte, error) {
		held, err := heldBy(tx, hostID)
		if err != nil {
			return nil, err
		}
		if !held[inst] {
			return nil, bootscope.ErrNotHeld
		}
		meta, err := tx.GetMeta()
		if err != nil {
			return nil, err
		}
		rec, err := getFloor(tx, inst)
		if err != nil {
			return nil, err
		}
		c := Command{Kind: KindAllocate, OwnerEpoch: meta.OwnerEpoch, Installation: inst, Local: req.Local}
		if rec != nil {
			c.Expect = rec.Generation
		}
		out, _, err := PlanAllocate(rec, c)
		if err != nil {
			return nil, err
		}
		grant = Grant{
			Installation: inst, Generation: out.Generation, Incarnation: out.Incarnation,
			IncarnationFrom: out.IncarnationFrom, ProgressEpoch: out.ProgressEpoch, ProgressSeq: out.ProgressSeq,
		}
		return Encode(c)
	})
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, grant)
}

func postReport(j Committer, w http.ResponseWriter, r *http.Request) {
	hostID := r.PathValue("id")
	var req ReportRequest
	if err := readBody(r, 1<<20, &req); err != nil {
		http.Error(w, "report: "+err.Error(), http.StatusBadRequest)
		return
	}
	if len(req.Entries) > MaxReportEntries {
		http.Error(w, "report: too many entries", http.StatusRequestEntityTooLarge)
		return
	}
	var res ReportResult
	err := j.Commit(func(tx *store.Tx) ([]byte, error) {
		res = ReportResult{}
		held, err := heldBy(tx, hostID)
		if err != nil {
			return nil, err
		}
		meta, err := tx.GetMeta()
		if err != nil {
			return nil, err
		}
		c := Command{Kind: KindReport, OwnerEpoch: meta.OwnerEpoch}
		seen := map[string]bool{}
		for _, e := range req.Entries {
			if seen[e.Installation] {
				continue
			}
			seen[e.Installation] = true
			if !held[e.Installation] {
				res.NotHeld = append(res.NotHeld, e.Installation)
				continue
			}
			rec, err := getFloor(tx, e.Installation)
			if err != nil {
				return nil, err
			}
			if _, o := Merge(rec, e); o == Ahead {
				res.Ahead = append(res.Ahead, e.Installation)
			}
			c.Entries = append(c.Entries, e)
		}
		res.Accepted = len(c.Entries) - len(res.Ahead)
		return Encode(c)
	})
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func postLower(j Committer, now func() time.Time, w http.ResponseWriter, r *http.Request) {
	inst := r.PathValue("installation")
	var req LowerRequest
	if err := readBody(r, 1<<14, &req); err != nil {
		http.Error(w, "lower: "+err.Error(), http.StatusBadRequest)
		return
	}
	req.At = now().UTC()
	var out resource.BootFloor
	err := j.Commit(func(tx *store.Tx) ([]byte, error) {
		meta, err := tx.GetMeta()
		if err != nil {
			return nil, err
		}
		rec, err := getFloor(tx, inst)
		if err != nil {
			return nil, err
		}
		if rec == nil {
			return nil, ErrNotFound
		}
		if out, err = PlanLower(*rec, req); err != nil {
			return nil, err
		}
		l := req
		return Encode(Command{Kind: KindLower, OwnerEpoch: meta.OwnerEpoch, Installation: inst, Lower: &l})
	})
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, viewOf(out))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	buf, err := json.Marshal(v)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_, _ = w.Write(append(buf, '\n'))
}

func writeErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, bootscope.ErrNotHeld):
		http.Error(w, err.Error(), http.StatusForbidden)
	case errors.Is(err, store.ErrNotFound), errors.Is(err, ErrNotFound):
		http.Error(w, err.Error(), http.StatusNotFound)
	case errors.Is(err, ErrConflict), errors.Is(err, ErrStaleEpoch), errors.Is(err, ErrBehindHost), errors.Is(err, ErrNoMatchingRequest):
		http.Error(w, err.Error(), http.StatusConflict)
	case errors.Is(err, raftnode.ErrNotLeader):
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
	default:
		http.Error(w, err.Error(), http.StatusBadRequest)
	}
}
