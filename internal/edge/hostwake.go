package edge

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// HostAcquirer is the host-agent activator surface: wake one local guest and
// return its dial address for the life of the activity lease.
type HostAcquirer interface {
	Acquire(ctx context.Context) (addr string, release func(), err error)
}

// HostWake is a fixed-host edge activator: demand wakes the co-located host
// activator for Service and the edge proxies to the guest address it returns.
// It does not create cloud hosts; the host is assumed already present.
type HostWake struct {
	Service string
	Act     HostAcquirer
	Timeout time.Duration
}

// Acquire implements Activator.
func (h *HostWake) Acquire(r *http.Request, service string) (string, func(), error) {
	if h == nil || h.Act == nil {
		return "", nil, errors.New("edge: host wake is not configured")
	}
	if service != h.Service {
		return "", nil, fmt.Errorf("edge: host wake serves %q, not %q", h.Service, service)
	}
	ctx := r.Context()
	if h.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, h.Timeout)
		defer cancel()
	}
	return h.Act.Acquire(ctx)
}
