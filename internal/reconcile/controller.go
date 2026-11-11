// Package reconcile runs one control loop for RFC-0001: read desired
// resources and unfinished operations, decide the next transition, persist
// that intent, and only then call the executor. A failed intent write
// dispatches nothing. A timed-out effect is recorded unknown and is not
// launched again.
package reconcile

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sufuree/sufuree/internal/demand"
	"github.com/sufuree/sufuree/internal/executor"
	"github.com/sufuree/sufuree/internal/placement"
	"github.com/sufuree/sufuree/internal/resource"
	"github.com/sufuree/sufuree/internal/store"
)

const (
	actionCreate    = "create-host"
	actionTerminate = "terminate-host"
)

// DB is the control store surface the loop needs. *store.Store implements it.
type DB interface {
	Update(fn func(*store.Tx) error) error
	View(fn func(*store.Tx) error) error
}

// Options tunes the loop. Zero Tick is one second. Zero Backoff retries a
// failed create on the next tick. Now overrides the clock for tests.
type Options struct {
	Tick    time.Duration
	Backoff time.Duration
	Now     func() time.Time
	Logger  *slog.Logger
	// Revoker removes revoked installations' object-store identities
	// (replicacred.Service). Without it, revocations stay pending.
	Revoker Revoker
}

// Revoker removes an installation's object-store identity. Removing one
// that is already gone succeeds.
type Revoker interface {
	Revoke(ctx context.Context, installation string) error
}

// revokeBatch bounds the revocations one revokeOnce works through.
const revokeBatch = 16

// revokeTimeout bounds one revocation.
const revokeTimeout = 30 * time.Second

// Controller reconciles one store against one executor.
type Controller struct {
	db      DB
	exec    executor.Executor
	demand  *demand.Manager
	tick    time.Duration
	backoff time.Duration
	now     func() time.Time
	log     *slog.Logger
	last    time.Time
	revoker Revoker
	// revokeCursor is the last installation revokeOnce tried. Only the
	// revocation loop touches it.
	revokeCursor string

	mu     sync.Mutex
	buffer *placement.BufferRecommendation
}

// New binds the loop to db. A nil executor is a memory executor with effects
// disabled, so cloud calls fail closed.
func New(db DB, exec executor.Executor, dem *demand.Manager, opt Options) *Controller {
	if exec == nil {
		exec = &executor.Memory{}
	}
	if opt.Tick <= 0 {
		opt.Tick = time.Second
	}
	if opt.Logger == nil {
		opt.Logger = slog.Default()
	}
	return &Controller{
		db: db, exec: exec, demand: dem,
		tick: opt.Tick, backoff: opt.Backoff, now: opt.Now, log: opt.Logger,
		revoker: opt.Revoker,
	}
}

// Run reconciles until ctx is cancelled. A failed pass is logged and retried
// on the next tick; it is not reported as a successful scale action.
func (c *Controller) Run(ctx context.Context) error {
	revoking := make(chan struct{})
	go func() {
		defer close(revoking)
		c.revokeLoop(ctx)
	}()
	defer func() { <-revoking }()
	t := time.NewTicker(c.tick)
	defer t.Stop()
	for {
		if err := c.ReconcileOnce(ctx); err != nil && ctx.Err() == nil {
			c.log.Error("reconcile", "err", err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// ReconcileOnce performs one pass. Operation intent is committed before any
// executor call in that pass.
func (c *Controller) ReconcileOnce(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	w, err := c.load()
	if err != nil {
		return err
	}
	if err := c.resume(ctx, w); err != nil {
		return err
	}
	planned, err := placement.PlanResult(w.input)
	if err != nil {
		return err
	}
	c.setBuffer(planned.Buffer)
	for _, d := range planned.Decisions {
		if err := c.apply(ctx, w, d); err != nil {
			return err
		}
	}
	if err := c.writeStatus(w.now); err != nil {
		return err
	}
	if err := c.finishDeletes(); err != nil {
		return err
	}
	return c.AcknowledgeReadyPlacements(ctx)
}

// finishDeletes removes each deleting app that has nothing left: no
// placement, claim or operation. Removing it records its installation's
// revocation (store.DeleteResource). A deleting app's claims past their
// deadline go first: nothing will place it, and their waiters have given
// up. Pools are not removed here.
func (c *Controller) finishDeletes() error {
	now := c.planNow()
	return c.db.Update(func(tx *store.Tx) error {
		all, err := tx.ListResources()
		if err != nil {
			return err
		}
		claims, err := tx.ListClaims()
		if err != nil {
			return err
		}
		deleting := map[string]bool{}
		for _, r := range all {
			if r.Kind == resource.KindApp && r.Lifecycle == resource.LifecycleDeleting {
				deleting[r.UID] = true
			}
		}
		for _, cl := range claims {
			if deleting[cl.ResourceUID] && !cl.Deadline.After(now) {
				if err := tx.DeleteClaim(cl.UID); err != nil {
					return err
				}
			}
		}
		for _, r := range all {
			if r.Kind != resource.KindApp || r.Lifecycle != resource.LifecycleDeleting {
				continue
			}
			if err := tx.DeleteResource(r.UID); err != nil && !errors.Is(err, store.ErrInUse) {
				return err
			}
		}
		return nil
	})
}

// revokeOnce removes the identities of up to revokeBatch queued
// revocations and records each one done. It runs in its own loop, not the
// reconcile pass, so a slow or hung store can't hold up placement. Each
// call starts after the last installation the previous one tried,
// wrapping around, so revocations that keep failing don't starve the
// rest. A failure is logged and retried on a later call.
func (c *Controller) revokeOnce(ctx context.Context) {
	if c.revoker == nil {
		return
	}
	var pending []resource.Revocation
	if err := c.db.View(func(tx *store.Tx) error {
		got, _, err := tx.PendingRevocations(c.revokeCursor, revokeBatch)
		if err != nil {
			return err
		}
		if len(got) < revokeBatch && c.revokeCursor != "" {
			// Wrap around to the start of the queue.
			head, _, err := tx.PendingRevocations("", revokeBatch-len(got))
			if err != nil {
				return err
			}
			seen := map[string]bool{}
			for _, r := range got {
				seen[r.Installation] = true
			}
			for _, r := range head {
				if !seen[r.Installation] {
					got = append(got, r)
				}
			}
		}
		pending = got
		return nil
	}); err != nil {
		c.log.Error("list revocations", "err", err)
		return
	}
	for _, r := range pending {
		if ctx.Err() != nil {
			return
		}
		c.revokeCursor = r.Installation
		rctx, cancel := context.WithTimeout(ctx, revokeTimeout)
		err := c.revoker.Revoke(rctx, r.Installation)
		cancel()
		if err != nil {
			c.log.Error("revoke installation identity", "installation", r.Installation, "err", err)
			continue
		}
		if err := c.db.Update(func(tx *store.Tx) error {
			return tx.MarkRevoked(r.Installation, c.planNow())
		}); err != nil {
			c.log.Error("record revocation", "installation", r.Installation, "err", err)
		}
	}
}

// revokeLoop runs revokeOnce every tick until ctx ends.
func (c *Controller) revokeLoop(ctx context.Context) {
	t := time.NewTicker(c.tick)
	defer t.Stop()
	for {
		c.revokeOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// ApplyResource creates or updates a resource if ResourceVersion matches the
// stored token. Version 0 creates. An equal spec and lifecycle does not write
// and does not bump the generation. A stale version is rejected.
func (c *Controller) ApplyResource(_ context.Context, in resource.Resource) (resource.Resource, error) {
	in = resource.Normalize(in)
	if in.Lifecycle == "" {
		in.Lifecycle = resource.LifecycleEnabled
	}
	var out resource.Resource
	err := c.db.Update(func(tx *store.Tx) error {
		cur, found, err := findByName(tx, in.Kind, in.Name)
		if err != nil {
			return err
		}
		if !found {
			if in.ResourceVersion != 0 {
				return fmt.Errorf("reconcile: create %s/%s: %w", in.Kind, in.Name, store.ErrStaleResourceVersion)
			}
			if in.UID == "" {
				in.UID = newResourceUID(in.Kind, in.Name)
			}
			if in.Generation == 0 {
				in.Generation = 1
			}
			in.Status = resource.Status{}
			got, err := tx.ApplyResource(in)
			out = got
			return err
		}
		if in.ResourceVersion != cur.ResourceVersion {
			return fmt.Errorf("reconcile: update %s: %w", cur.UID, store.ErrStaleResourceVersion)
		}
		in.UID = cur.UID
		in.Status = cur.Status
		same := reflect.DeepEqual(resource.Normalize(cur).Spec, resource.Normalize(in).Spec) && in.Lifecycle == cur.Lifecycle
		if same {
			out = cur
			return nil
		}
		if reflect.DeepEqual(resource.Normalize(cur).Spec, resource.Normalize(in).Spec) {
			in.Generation = cur.Generation
		} else {
			in.Generation = cur.Generation + 1
		}
		got, err := tx.ApplyResource(in)
		out = got
		return err
	})
	return out, err
}

// GetResource returns the named resource.
func (c *Controller) GetResource(_ context.Context, kind resource.Kind, name string) (resource.Resource, error) {
	var out resource.Resource
	var found bool
	err := c.db.View(func(tx *store.Tx) error {
		got, ok, err := findByName(tx, kind, name)
		out, found = got, ok
		return err
	})
	if err != nil {
		return resource.Resource{}, err
	}
	if !found {
		return resource.Resource{}, fmt.Errorf("reconcile: %s/%s: %w", kind, name, store.ErrNotFound)
	}
	return out, nil
}

// DeleteResource moves an app to lifecycle deleting, in one transaction, and
// returns it. ifMatch, when non-zero, must be its current resource version.
// An app already deleting is returned unchanged. The loop finishes the
// delete (finishDeletes).
func (c *Controller) DeleteResource(_ context.Context, kind resource.Kind, name string, ifMatch uint64) (resource.Resource, error) {
	if kind != resource.KindApp {
		return resource.Resource{}, fmt.Errorf("reconcile: only an App can be deleted, not %s", kind)
	}
	var out resource.Resource
	err := c.db.Update(func(tx *store.Tx) error {
		cur, found, err := findByName(tx, kind, name)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("reconcile: %s/%s: %w", kind, name, store.ErrNotFound)
		}
		if ifMatch != 0 && ifMatch != cur.ResourceVersion {
			return fmt.Errorf("reconcile: delete %s: %w", cur.UID, store.ErrStaleResourceVersion)
		}
		if cur.Lifecycle == resource.LifecycleDeleting {
			out = cur
			return nil
		}
		next := cur
		next.Lifecycle = resource.LifecycleDeleting
		got, err := tx.ApplyResource(next)
		out = got
		return err
	})
	return out, err
}

// ListResources returns every resource.
func (c *Controller) ListResources(context.Context) ([]resource.Resource, error) {
	var out []resource.Resource
	err := c.db.View(func(tx *store.Tx) error {
		got, err := tx.ListResources()
		out = got
		return err
	})
	return out, err
}

// ListHosts returns every host record.
func (c *Controller) ListHosts(context.Context) ([]resource.Host, error) {
	var out []resource.Host
	err := c.db.View(func(tx *store.Tx) error {
		got, err := tx.ListHosts()
		out = got
		return err
	})
	return out, err
}

// ListOperations returns every operation, including closed ones.
func (c *Controller) ListOperations(context.Context) ([]resource.Operation, error) {
	var out []resource.Operation
	err := c.db.View(func(tx *store.Tx) error {
		got, err := tx.ListOperations()
		out = got
		return err
	})
	return out, err
}

type world struct {
	now        time.Time
	meta       resource.ClusterMeta
	pool       *resource.Resource
	apps       []resource.Resource
	hosts      []resource.Host
	placements []resource.Placement
	ops        []resource.Operation
	claims     []resource.ActivationClaim
	input      placement.Input
}

func (c *Controller) load() (world, error) {
	var w world
	w.now = c.planNow()
	err := c.db.View(func(tx *store.Tx) error {
		meta, err := tx.GetMeta()
		if err != nil {
			return err
		}
		w.meta = meta
		resources, err := tx.ListResources()
		if err != nil {
			return err
		}
		var pools []resource.Resource
		for _, r := range resources {
			switch r.Kind {
			case resource.KindPool:
				pools = append(pools, r)
			case resource.KindApp:
				w.apps = append(w.apps, r)
			default:
				return fmt.Errorf("reconcile: kind %q is not supported", r.Kind)
			}
		}
		if len(pools) > 1 {
			return errors.New("reconcile: more than one pool; one capacity ledger is required")
		}
		if len(pools) == 1 {
			w.pool = &pools[0]
		}
		w.hosts, err = tx.ListHosts()
		if err != nil {
			return err
		}
		w.placements, err = tx.ListPlacements()
		if err != nil {
			return err
		}
		w.ops, err = tx.ListOperations()
		if err != nil {
			return err
		}
		w.claims, err = tx.ListClaims()
		return err
	})
	if err != nil {
		return world{}, err
	}
	w.input = c.inputFrom(w)
	return w, nil
}

func (c *Controller) setBuffer(b *placement.BufferRecommendation) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if b == nil {
		c.buffer = nil
		return
	}
	cp := *b
	c.buffer = &cp
}

// BufferRecommendation returns the last warm-capacity observation from reconcile.
func (c *Controller) BufferRecommendation() *placement.BufferRecommendation {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.buffer == nil {
		return nil
	}
	cp := *c.buffer
	return &cp
}

// ListPlacementsByHost returns placements assigned to hostID.
func (c *Controller) ListPlacementsByHost(_ context.Context, hostID string) ([]resource.Placement, error) {
	if hostID == "" {
		return nil, errors.New("reconcile: host id is required")
	}
	var out []resource.Placement
	err := c.db.View(func(tx *store.Tx) error {
		all, err := tx.ListPlacements()
		if err != nil {
			return err
		}
		for _, p := range all {
			if p.HostUID == hostID {
				out = append(out, p)
			}
		}
		return nil
	})
	return out, err
}

func (c *Controller) inputFrom(w world) placement.Input {
	in := placement.Input{Now: w.now, DisableScaleIn: w.pool == nil, WarmMode: placement.WarmOff}
	if w.pool != nil {
		spec := w.pool.Spec.(resource.PoolSpec)
		in.MinHosts = spec.MinHosts
		in.MaxHosts = spec.MaxHosts
		in.Quiet = spec.QuietPeriod.Duration
		in.WarmMode = placement.WarmMode(spec.WarmMode)
		in.ReserveMiB = spec.ReserveMiB
	}
	used := map[string]int{}
	byHost := map[string][]resource.Placement{}
	for _, p := range w.placements {
		used[p.HostUID] += p.MemoryMiB
		byHost[p.HostUID] = append(byHost[p.HostUID], p)
	}
	uncertain := map[string]bool{}
	var blind bool
	for _, op := range w.ops {
		if !op.Open() {
			continue
		}
		item := placement.Operation{ID: op.OperationID, Action: op.Action, Outcome: string(op.Outcome)}
		if isTerminate(op.Action) {
			host, _, ok := parseTerminate(op.OperationID)
			if !ok {
				blind = true
				continue
			}
			item.HostID = host
			if op.Outcome == resource.OutcomeUnknown {
				uncertain[host] = true
			}
		}
		in.Ops = append(in.Ops, item)
	}
	for _, h := range w.hosts {
		if blind {
			in.Ops = append(in.Ops, placement.Operation{
				ID: "unknown-" + h.UID, Action: actionTerminate, Outcome: string(resource.OutcomeUnknown), HostID: h.UID,
			})
		}
		ph := placement.Host{
			ID: h.UID, MemoryMiB: h.MemoryMiB, UsedMiB: used[h.UID],
			Draining: h.Draining, Ready: h.Ready,
		}
		if len(byHost[h.UID]) == 0 {
			ph.EmptySince = h.ObservedAt
		}
		in.Hosts = append(in.Hosts, ph)
	}
	demandBy := map[string]int{}
	for _, claim := range w.claims {
		if claim.Deadline.After(w.now) && claim.Count > 0 {
			demandBy[claim.ResourceUID] = 1
		}
	}
	for _, app := range w.apps {
		spec, ok := app.Spec.(resource.AppSpec)
		if !ok {
			continue
		}
		wl := placement.Workload{
			ResourceUID: app.UID, Name: app.Name, MemoryMiB: spec.MemoryMiB,
			MinReplicas: spec.MinReplicas, MaxReplicas: spec.MaxReplicas,
			Idle: spec.IdleTimeout.Duration, Lifecycle: string(app.Lifecycle),
			PendingDemand: demandBy[app.UID],
		}
		for _, p := range w.placements {
			if p.ResourceUID != app.UID {
				continue
			}
			wl.Replicas = append(wl.Replicas, placement.Replica{
				PlacementID: p.UID, HostID: p.HostUID, Since: p.UpdatedAt,
				Protected: uncertain[p.HostUID],
			})
		}
		in.Workloads = append(in.Workloads, wl)
	}
	return in
}

func (c *Controller) apply(ctx context.Context, w world, d placement.Decision) error {
	switch d.Op {
	case placement.OpReleaseReplica:
		return c.db.Update(func(tx *store.Tx) error {
			return tx.DeletePlacement(d.PlacementID)
		})
	case placement.OpPlaceReplica:
		return c.place(ctx, w, d)
	case placement.OpRequestCapacity:
		if d.MemoryMiB < 32 {
			return errors.New("reconcile: capacity request has no guest memory; not dispatching")
		}
		if w.pool == nil {
			return errors.New("reconcile: capacity request without a pool; not dispatching")
		}
		if c.cooling(w.ops, w.now) {
			return nil
		}
		return c.startCreate(ctx, *w.pool, d.MemoryMiB)
	case placement.OpDrainHost:
		return c.drain(w.now, d.HostID)
	case placement.OpTerminateHost:
		if w.pool == nil {
			return errors.New("reconcile: terminate without a pool; not dispatching")
		}
		return c.startTerminate(ctx, *w.pool, d.HostID)
	default:
		return fmt.Errorf("reconcile: decision %q is not supported", d.Op)
	}
}

func (c *Controller) place(ctx context.Context, w world, d placement.Decision) error {
	gen := uint64(1)
	for _, p := range w.placements {
		if p.ResourceUID == d.ResourceUID && p.Generation >= gen {
			gen = p.Generation + 1
		}
	}
	id, err := entropy()
	if err != nil {
		return err
	}
	replica, err := replicaID(w, d.ResourceUID)
	if err != nil {
		return err
	}
	stamp := c.stamp(w.now)
	p := resource.Placement{
		UID: "p" + id, ResourceUID: d.ResourceUID, HostUID: d.HostID,
		Replica: replica, Generation: gen, OwnerEpoch: w.meta.OwnerEpoch,
		MemoryMiB: d.MemoryMiB, Phase: resource.PhaseReady, UpdatedAt: stamp,
	}
	if err := c.db.Update(func(tx *store.Tx) error { return tx.PutPlacement(p) }); err != nil {
		return err
	}
	if c.demand == nil {
		return nil
	}
	for _, claim := range w.claims {
		if claim.ResourceUID != d.ResourceUID || !claim.Deadline.After(w.now) {
			continue
		}
		if err := c.demand.AcknowledgeHandoff(ctx, claim.UID, gen); err != nil {
			return err
		}
	}
	return nil
}

func (c *Controller) drain(now time.Time, hostID string) error {
	return c.db.Update(func(tx *store.Tx) error {
		h, err := tx.GetHost(hostID)
		if err != nil {
			return err
		}
		h.Draining = true
		if now.After(h.ObservedAt) {
			h.ObservedAt = now
		}
		return tx.PutHost(h)
	})
}

func (c *Controller) cooling(ops []resource.Operation, now time.Time) bool {
	if c.backoff <= 0 {
		return false
	}
	for _, op := range ops {
		if op.Action == actionCreate && op.Outcome == resource.OutcomeFailed && now.Sub(op.UpdatedAt) < c.backoff {
			return true
		}
	}
	return false
}

func (c *Controller) writeStatus(now time.Time) error {
	var resources []resource.Resource
	var placements []resource.Placement
	var hosts []resource.Host
	var claims []resource.ActivationClaim
	var ops []resource.Operation
	err := c.db.View(func(tx *store.Tx) error {
		var err error
		resources, err = tx.ListResources()
		if err != nil {
			return err
		}
		placements, err = tx.ListPlacements()
		if err != nil {
			return err
		}
		hosts, err = tx.ListHosts()
		if err != nil {
			return err
		}
		claims, err = tx.ListClaims()
		if err != nil {
			return err
		}
		ops, err = tx.ListOperations()
		return err
	})
	if err != nil {
		return err
	}
	waiting := false
	for _, op := range ops {
		if op.Action == actionCreate && op.Open() {
			waiting = true
		}
	}
	for _, res := range resources {
		status, ok := observe(res, placements, hosts, claims, waiting, now)
		if !ok || sameStatus(status, res.Status) {
			continue
		}
		res.Status = status
		if err := c.db.Update(func(tx *store.Tx) error {
			cur, err := tx.GetResource(res.UID)
			if err != nil {
				return err
			}
			cur.Status = status
			_, err = tx.ApplyResource(cur)
			return err
		}); err != nil {
			return err
		}
	}
	return nil
}

func sameStatus(a, b resource.Status) bool {
	return a.Phase == b.Phase && a.ReadyReplicas == b.ReadyReplicas && a.Message == b.Message && a.ObservedGeneration == b.ObservedGeneration
}

func observe(res resource.Resource, placements []resource.Placement, hosts []resource.Host, claims []resource.ActivationClaim, waiting bool, now time.Time) (resource.Status, bool) {
	var status resource.Status
	status.ObservedGeneration = res.Generation
	status.UpdatedAt = now
	switch res.Kind {
	case resource.KindApp:
		spec, ok := res.Spec.(resource.AppSpec)
		if !ok {
			return resource.Status{}, false
		}
		ready, assigned := 0, 0
		for _, p := range placements {
			if p.ResourceUID != res.UID {
				continue
			}
			assigned++
			if p.Phase == resource.PhaseReady {
				ready++
			}
		}
		pending := false
		for _, claim := range claims {
			if claim.ResourceUID == res.UID && claim.Deadline.After(now) {
				pending = true
			}
		}
		switch {
		case res.Lifecycle != resource.LifecycleEnabled && assigned > 0:
			status.Phase = resource.PhaseDraining
			status.ReadyReplicas = ready
			status.Message = "draining"
		case ready > 0:
			status.Phase = resource.PhaseReady
			status.ReadyReplicas = ready
			status.Message = "ready"
		case assigned > 0 || pending || waiting:
			status.Phase = resource.PhaseStarting
			status.Message = "starting"
			if waiting && ready == 0 {
				status.Message = "waiting for capacity"
			}
		default:
			status.Phase = resource.PhaseDormant
			status.Message = "dormant"
		}
		if status.ReadyReplicas > spec.MaxReplicas {
			return resource.Status{}, false
		}
	case resource.KindPool:
		spec, ok := res.Spec.(resource.PoolSpec)
		if !ok {
			return resource.Status{}, false
		}
		ready, draining := 0, 0
		for _, h := range hosts {
			if h.PoolUID != res.UID {
				continue
			}
			if h.Draining {
				draining++
			}
			if h.Ready && !h.Draining {
				ready++
			}
		}
		switch {
		case ready > 0:
			status.Phase = resource.PhaseReady
			status.ReadyReplicas = ready
			status.Message = "hosts ready"
		case draining > 0:
			status.Phase = resource.PhaseDraining
			status.Message = "draining"
		default:
			status.Phase = resource.PhaseDormant
			status.Message = "no backend hosts"
		}
		if status.ReadyReplicas > spec.MaxHosts {
			return resource.Status{}, false
		}
	default:
		return resource.Status{}, false
	}
	if err := status.Validate(); err != nil {
		return resource.Status{}, false
	}
	return status, true
}

func findByName(tx *store.Tx, kind resource.Kind, name string) (resource.Resource, bool, error) {
	all, err := tx.ListResources()
	if err != nil {
		return resource.Resource{}, false, err
	}
	for _, r := range all {
		if r.Kind == kind && r.Name == name {
			return r, true, nil
		}
	}
	return resource.Resource{}, false, nil
}

func newResourceUID(kind resource.Kind, name string) string {
	switch kind {
	case resource.KindPool:
		return "pool-" + name
	default:
		return "app-" + name
	}
}

func (c *Controller) planNow() time.Time {
	if c.now != nil {
		return c.now().UTC()
	}
	return time.Now().UTC()
}

func (c *Controller) stamp(after time.Time) time.Time {
	t := c.planNow()
	if !after.IsZero() && !t.After(after) {
		t = after.Add(time.Nanosecond)
	}
	if !c.last.IsZero() && !t.After(c.last) {
		t = c.last.Add(time.Nanosecond)
	}
	c.last = t
	return t
}

// replicaID names a new placement's replica. An app with at most one
// replica is one installation, so its replica is stable: the replica of an
// existing placement (an installation placed before stable IDs keeps its
// own), otherwise its resource UID. It is the same on every host it moves
// to, which keeps its storage prefix, hint key, boot generation and
// rollback floor (package bootfloor) with it. Only an app that may run
// several replicas at once gets a fresh replica ID.
func replicaID(w world, resourceUID string) (string, error) {
	for _, app := range w.apps {
		if app.UID != resourceUID {
			continue
		}
		if spec, ok := app.Spec.(resource.AppSpec); ok && spec.MaxReplicas <= 1 {
			for _, p := range w.placements {
				if p.ResourceUID == resourceUID {
					return p.Replica, nil
				}
			}
			return resourceUID, nil
		}
	}
	rep, err := entropy()
	if err != nil {
		return "", err
	}
	return "r" + rep, nil
}

func entropy() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("reconcile: entropy: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

func digest(parts ...string) string {
	sum := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return hex.EncodeToString(sum[:])
}

func isCreate(action string) bool { return action == actionCreate }

func isTerminate(action string) bool { return action == actionTerminate }

func parseCreate(id string) (memory int, extra string, ok bool) {
	rest, found := strings.CutPrefix(id, "c")
	if !found || strings.HasPrefix(id, "c-") {
		return 0, "", false
	}
	num, extra, found := strings.Cut(rest, "-")
	if !found || len(extra) != 16 {
		return 0, "", false
	}
	memory, err := strconv.Atoi(num)
	if err != nil || memory < 32 {
		return 0, "", false
	}
	return memory, extra, true
}

func parseTerminate(id string) (host, extra string, ok bool) {
	rest, found := strings.CutPrefix(id, "t-")
	if !found {
		return "", "", false
	}
	i := strings.LastIndex(rest, "-")
	if i <= 0 || len(rest[i+1:]) != 16 {
		return "", "", false
	}
	return rest[:i], rest[i+1:], true
}
