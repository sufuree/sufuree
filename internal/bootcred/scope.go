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
	"github.com/sufuree/sufuree/internal/replicacred"
	"github.com/sufuree/sufuree/internal/workload"
)

// ErrAuthorizeDenied means the controller refused the installation or tags.
// Guarded must not fall back to a cached assignment list after this error.
var ErrAuthorizeDenied = errors.New("bootcred: authorize denied")

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
	var out struct {
		SessionTags map[string]string `json:"session_tags"`
	}
	if err := c.postCredentials(ctx, installation, map[string]any{"requested_tags": requested}, &out); err != nil {
		return nil, err
	}
	if len(out.SessionTags) == 0 {
		return nil, errors.New("bootcred: authorize returned no session tags")
	}
	return out.SessionTags, nil
}

// ReplicaCredentials POSTs /v1/hosts/{id}/credentials/{installation} and
// returns the installation's own replica credentials, which the controller
// issues only to a host holding the installation.
func (c *AssignmentClient) ReplicaCredentials(ctx context.Context, installation string) (replicacred.Credentials, error) {
	var out struct {
		Replica *replicacred.Credentials `json:"replica"`
	}
	if err := c.postCredentials(ctx, installation, map[string]any{}, &out); err != nil {
		return replicacred.Credentials{}, err
	}
	if out.Replica == nil || out.Replica.AccessKeyID == "" || out.Replica.SecretAccessKey == "" {
		return replicacred.Credentials{}, errors.New("bootcred: the controller issues no replica credentials (start it with --replica-credentials)")
	}
	return *out.Replica, nil
}

func (c *AssignmentClient) postCredentials(ctx context.Context, installation string, in any, out any) error {
	if c == nil || c.BaseURL == "" || c.HostUID == "" {
		return errors.New("bootcred: assignment client needs base URL and host id")
	}
	payload, err := json.Marshal(in)
	if err != nil {
		return err
	}
	url := strings.TrimRight(c.BaseURL, "/") + "/v1/hosts/" + c.HostUID + "/credentials/" + installation
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := c.httpClient().Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(io.LimitReader(res.Body, 1<<16))
	if err != nil {
		return err
	}
	if res.StatusCode == http.StatusForbidden || res.StatusCode == http.StatusBadRequest {
		return fmt.Errorf("%w: %s: %s", ErrAuthorizeDenied, res.Status, strings.TrimSpace(string(body)))
	}
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("bootcred: credentials %s: %s", res.Status, strings.TrimSpace(string(body)))
	}
	if err := json.Unmarshal(body, out); err != nil {
		return fmt.Errorf("bootcred: credentials JSON: %w", err)
	}
	return nil
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
	// A controller deny is authoritative: never override with a stale cache.
	if errors.Is(err, ErrAuthorizeDenied) {
		return nil, err
	}
	// Broker unreachable / non-deny failure: fall back to assignments and
	// apply the same bootscope pin rules locally.
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
