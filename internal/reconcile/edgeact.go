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

// EdgeActivator admits a named App through the demand manager and returns a
// host-local upstream only after handoff. Without a registered upstream map
// entry it still completes handoff when a ready placement exists, but returns
// an error so the edge answers 503 rather than inventing an address.
type EdgeActivator struct {
	Controller *Controller
	Demand     *demand.Manager
	// Upstreams maps resource UID → dial address once a host agent has
	// published one. Empty means activation can hand off but not forward.
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
		addr := ""
		if a.Upstreams != nil {
			addr = a.Upstreams[res.UID]
		}
		if addr == "" {
			return "", nil, fmt.Errorf("reconcile: no upstream for %s after handoff gen %d", res.UID, result.PlacementGeneration)
		}
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
