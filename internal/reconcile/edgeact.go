package reconcile

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/sufuree/sufuree/internal/demand"
	"github.com/sufuree/sufuree/internal/resource"
	"github.com/sufuree/sufuree/internal/store"
)

// HostLease holds a host-agent activity lease for the life of one edge
// forward. Acquire must wake/keep the guest until release is called.
type HostLease interface {
	Acquire(ctx context.Context) (addr string, release func(), err error)
}

// EdgeActivator admits a named App through the demand manager and returns a
// host-local upstream only after handoff. Without a registered upstream or
// host lease it still completes handoff when a ready placement exists, but
// returns an error so the edge answers 503 rather than inventing an address.
type EdgeActivator struct {
	Controller *Controller
	Demand     *demand.Manager
	// HostLeases maps App name → host activity lease. When set for a service,
	// Acquire takes the lease after demand handoff and returns that address
	// and release so the guest cannot idle-stop mid-forward. Prefer this for
	// direct guest addresses.
	HostLeases map[string]HostLease
	// Upstreams maps resource UID or App name → dial address of a
	// lease-holding host gateway (not a raw guest). Empty release is safe
	// only because the gateway holds its own activity lease while proxying.
	Upstreams map[string]string
	Wait      time.Duration
}

// Acquire implements edge.Activator.
func (a *EdgeActivator) Acquire(r *http.Request, service string) (string, func(), error) {
	if a == nil || a.Controller == nil || a.Demand == nil {
		return "", nil, errors.New("reconcile: edge activator is not configured")
	}
	res, err := a.Controller.GetResource(r.Context(), resource.KindApp, service)
	if err != nil {
		return "", nil, err
	}
	if res.Lifecycle == resource.LifecycleDeleting {
		// Nothing will place it; a claim would only hold up the delete.
		return "", nil, fmt.Errorf("reconcile: app %s is being deleted: %w", service, store.ErrNotFound)
	}
	wait := a.Wait
	if wait <= 0 {
		wait = 15 * time.Second
	}
	deadline := time.Now().Add(wait)
	if dl, ok := r.Context().Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	_, ch, err := a.Demand.Join(r.Context(), res.UID, requestID(r), deadline)
	if err != nil {
		return "", nil, err
	}
	select {
	case <-r.Context().Done():
		return "", nil, r.Context().Err()
	case result := <-ch:
		if result.Err != nil {
			return "", nil, result.Err
		}
		if lease := a.HostLeases[res.Name]; lease != nil {
			return lease.Acquire(r.Context())
		}
		addr := ""
		if a.Upstreams != nil {
			addr = a.Upstreams[res.UID]
			if addr == "" {
				addr = a.Upstreams[res.Name]
			}
		}
		if addr == "" {
			return "", nil, fmt.Errorf("reconcile: no upstream for %s after handoff gen %d", res.UID, result.PlacementGeneration)
		}
		// Upstream-only: gateway must hold the host activity lease itself.
		return addr, func() {}, nil
	}
}

func requestID(r *http.Request) string {
	if id := r.Header.Get("X-Request-Id"); id != "" {
		return id
	}
	return fmt.Sprintf("%p-%d", r, time.Now().UnixNano())
}

// AcknowledgeReadyPlacements hands off pending claims whose resource already
// has a placement. It is a bridge until the host agent posts handoff itself;
// it does not invent placements or upstream addresses.
func (c *Controller) AcknowledgeReadyPlacements(ctx context.Context) error {
	if c.demand == nil {
		return nil
	}
	var claims []resource.ActivationClaim
	var placements []resource.Placement
	err := c.db.View(func(tx *store.Tx) error {
		var err error
		claims, err = tx.ListClaims()
		if err != nil {
			return err
		}
		placements, err = tx.ListPlacements()
		return err
	})
	if err != nil {
		return err
	}
	ready := map[string]uint64{}
	for _, p := range placements {
		if p.Generation > ready[p.ResourceUID] {
			ready[p.ResourceUID] = p.Generation
		}
	}
	for _, claim := range claims {
		gen := ready[claim.ResourceUID]
		if gen == 0 {
			continue
		}
		if err := c.demand.AcknowledgeHandoff(ctx, claim.UID, gen); err != nil && !errors.Is(err, demand.ErrNotFound) && !errors.Is(err, demand.ErrConflict) {
			return err
		}
	}
	return nil
}
