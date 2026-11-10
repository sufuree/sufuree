package bootcred

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/sufuree/sufuree/internal/bootscope"
	"github.com/sufuree/sufuree/internal/workload"
)

// AssignmentClient pulls controller-published assignments and authorizes
// session tags before the inner Issuer assumes a role.
type AssignmentClient struct {
	BaseURL string
	HostUID string
	HTTP    *http.Client
}

func (c *AssignmentClient) httpClient() *http.Client {
	if c != nil && c.HTTP != nil {
		return c.HTTP
	}
	return http.DefaultClient
}

// FetchAssignments GETs /v1/hosts/{id}/assignments.
func (c *AssignmentClient) FetchAssignments(ctx context.Context) ([]bootscope.Assignment, error) {
	if c == nil || c.BaseURL == "" || c.HostUID == "" {
		return nil, errors.New("bootcred: assignment client needs base URL and host id")
	}
	url := strings.TrimRight(c.BaseURL, "/") + "/v1/hosts/" + c.HostUID + "/assignments"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	res, err := c.httpClient().Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bootcred: assignments %s: %s", res.Status, strings.TrimSpace(string(body)))
	}
	var out []bootscope.Assignment
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("bootcred: assignments JSON: %w", err)
	}
	return out, nil
}

// AuthorizeTags POSTs /v1/hosts/{id}/credentials/{installation} and returns
// controller-pinned session tags. Does not call STS.
func (c *AssignmentClient) AuthorizeTags(ctx context.Context, installation string, requested map[string]string) (map[string]string, error) {
	if c == nil || c.BaseURL == "" || c.HostUID == "" {
		return nil, errors.New("bootcred: assignment client needs base URL and host id")
	}
	payload, err := json.Marshal(map[string]any{"requested_tags": requested})
	if err != nil {
		return nil, err
	}
	url := strings.TrimRight(c.BaseURL, "/") + "/v1/hosts/" + c.HostUID + "/credentials/" + installation
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := c.httpClient().Do(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 1<<16))
	if err != nil {
		return nil, err
	}
	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bootcred: authorize %s: %s", res.Status, strings.TrimSpace(string(body)))
	}
	var out struct {
		SessionTags map[string]string `json:"session_tags"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("bootcred: authorize JSON: %w", err)
	}
	if len(out.SessionTags) == 0 {
		return nil, errors.New("bootcred: authorize returned no session tags")
	}
	return out.SessionTags, nil
}

// Guarded wraps an Issuer so boot credentials only use tags the controller
// authorizes for this host. Workload ID is the installation (placement replica).
type Guarded struct {
	Inner  Issuer
	Client *AssignmentClient

	mu          sync.Mutex
	assignments []bootscope.Assignment
	fetchedAt   time.Time
	// CacheFor bounds how long FetchAssignments results may be reused when
	// AuthorizeTags is unreachable. Zero means 5s.
	CacheFor time.Duration
}

// Issue authorizes tags with the controller, then calls the inner issuer with
// the pinned session tags. Spec tags alone are not trusted when Client is set.
func (g *Guarded) Issue(ctx context.Context, w workload.Spec, generation uint64) (map[string]string, error) {
	if g == nil || g.Inner == nil {
		return nil, errors.New("bootcred: guarded issuer is incomplete")
	}
	if w.BootCredentials == nil {
		return g.Inner.Issue(ctx, w, generation)
	}
	tags, err := g.authorize(ctx, w.ID, w.BootCredentials.SessionTags)
	if err != nil {
		return nil, err
	}
	pinned := w
	bc := *w.BootCredentials
	bc.SessionTags = tags
	pinned.BootCredentials = &bc
	return g.Inner.Issue(ctx, pinned, generation)
}

func (g *Guarded) authorize(ctx context.Context, installation string, requested map[string]string) (map[string]string, error) {
	if g.Client == nil {
		return nil, errors.New("bootcred: guarded issuer needs an assignment client")
	}
	tags, err := g.Client.AuthorizeTags(ctx, installation, requested)
	if err == nil {
		return tags, nil
	}
	// If the broker is down, fall back to the last pulled assignment list
	// (or a fresh pull) and authorize locally with the same bootscope rules.
	as, fetchErr := g.cachedAssignments(ctx)
	if fetchErr != nil {
		return nil, errors.Join(err, fetchErr)
	}
	for _, a := range as {
		if a.Placement.HostUID == g.Client.HostUID && a.Placement.Replica == installation && bootscope.Active(a.Placement) {
			return bootscope.PinSessionTags(installation, requested)
		}
	}
	return nil, errors.Join(err, bootscope.ErrNotHeld)
}

func (g *Guarded) cachedAssignments(ctx context.Context) ([]bootscope.Assignment, error) {
	ttl := g.CacheFor
	if ttl <= 0 {
		ttl = 5 * time.Second
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.assignments) > 0 && time.Since(g.fetchedAt) < ttl {
		return append([]bootscope.Assignment(nil), g.assignments...), nil
	}
	as, err := g.Client.FetchAssignments(ctx)
	if err != nil {
		return nil, err
	}
	g.assignments = as
	g.fetchedAt = time.Now()
	return append([]bootscope.Assignment(nil), as...), nil
}
