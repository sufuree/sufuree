package bootfloor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"
)

// Client is a host's view of the controller's boot-floor API.
type Client struct {
	BaseURL string
	HostUID string
	HTTP    *http.Client
}

func (c *Client) do(ctx context.Context, path string, in, out any) error {
	if c == nil || c.BaseURL == "" || c.HostUID == "" {
		return errors.New("bootfloor: client needs a base URL and host id")
	}
	body, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.BaseURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	hc := c.HTTP
	if hc == nil {
		hc = http.DefaultClient
	}
	res, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	data, err := io.ReadAll(io.LimitReader(res.Body, 1<<20))
	if err != nil {
		return err
	}
	if res.StatusCode != http.StatusOK {
		msg := strings.TrimSpace(string(data))
		if res.StatusCode == http.StatusConflict && strings.Contains(msg, ErrBehindHost.Error()) {
			return fmt.Errorf("%w (%s)", ErrBehindHost, path)
		}
		return fmt.Errorf("bootfloor: %s %s: %s", path, res.Status, msg)
	}
	return json.Unmarshal(data, out)
}

// Boot asks the controller for a fresh H for one boot of installation,
// sending the host's own record (local) to merge first. It is never cached.
func (c *Client) Boot(ctx context.Context, installation string, local Entry) (Grant, error) {
	local.Installation = installation
	var g Grant
	path := "/v1/hosts/" + url.PathEscape(c.HostUID) + "/boot/" + url.PathEscape(installation)
	if err := c.do(ctx, path, BootRequest{Local: &local}, &g); err != nil {
		return Grant{}, err
	}
	if g.Installation != installation || g.Generation == 0 || g.Incarnation == 0 {
		return Grant{}, errors.New("bootfloor: the controller returned an incomplete grant")
	}
	return g, nil
}

// Report sends one batch of entries.
func (c *Client) Report(ctx context.Context, entries []Entry) (ReportResult, error) {
	var res ReportResult
	err := c.do(ctx, "/v1/hosts/"+url.PathEscape(c.HostUID)+"/progress", ReportRequest{Entries: entries}, &res)
	return res, err
}

// Reporter batches one host's progress reports: one request per interval
// carrying only installations whose record changed since the last
// successful report, each at its maximum. Kick sends sooner (a guest exit
// or a clean stop).
type Reporter struct {
	Client   *Client
	Interval time.Duration // default 1 minute (owner decision)
	Logger   *slog.Logger

	mu      sync.Mutex
	pending map[string]Entry
	kick    chan struct{}
	once    sync.Once
}

// DefaultInterval is the owner-decided report interval.
const DefaultInterval = time.Minute

func (r *Reporter) init() {
	r.once.Do(func() {
		r.kick = make(chan struct{}, 1)
		if r.pending == nil {
			r.pending = map[string]Entry{}
		}
	})
}

// Note records an installation's current record for the next report.
func (r *Reporter) Note(e Entry) {
	r.init()
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pending[e.Installation] = mergeEntry(r.pending[e.Installation], e)
}

// Kick asks for a report soon.
func (r *Reporter) Kick() {
	r.init()
	select {
	case r.kick <- struct{}{}:
	default:
	}
}

// mergeEntry keeps the newer of two host entries under the package rule.
func mergeEntry(a, b Entry) Entry {
	if a.Installation == "" {
		return b
	}
	switch {
	case max(a.Incarnation, 1) > max(b.Incarnation, 1):
		return a
	case max(a.Incarnation, 1) < max(b.Incarnation, 1):
		return b
	}
	out := a
	out.Generation = max(a.Generation, b.Generation)
	if progressBelow(a.Epoch, a.Seq, b.Epoch, b.Seq) {
		out.Epoch, out.Seq = b.Epoch, b.Seq
	}
	return out
}

// Run reports every interval and on Kick until ctx ends, then makes one
// last attempt bounded by a few seconds.
func (r *Reporter) Run(ctx context.Context) {
	r.init()
	every := r.Interval
	if every <= 0 {
		every = DefaultInterval
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			fctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			r.Flush(fctx)
			cancel()
			return
		case <-t.C:
		case <-r.kick:
		}
		r.Flush(ctx)
	}
}

// Flush sends everything pending. Entries that fail stay pending, merged
// with anything noted meanwhile.
func (r *Reporter) Flush(ctx context.Context) {
	r.init()
	r.mu.Lock()
	batch := make([]Entry, 0, len(r.pending))
	for _, e := range r.pending {
		batch = append(batch, e)
	}
	r.pending = map[string]Entry{}
	r.mu.Unlock()
	sort.Slice(batch, func(i, j int) bool { return batch[i].Installation < batch[j].Installation })
	for len(batch) > 0 {
		n := min(len(batch), MaxReportEntries)
		chunk := batch[:n]
		res, err := r.Client.Report(ctx, chunk)
		if err != nil {
			r.requeue(batch)
			if r.Logger != nil {
				r.Logger.Warn("boot floor report failed; will retry", "entries", len(batch), "err", err)
			}
			return
		}
		if len(res.NotHeld) > 0 && r.Logger != nil {
			r.Logger.Warn("controller says this host does not hold these installations; their reports were dropped", "installations", res.NotHeld)
		}
		if len(res.Ahead) > 0 && r.Logger != nil {
			r.Logger.Error("ALERT: the controller is behind this host's floor incarnation for these installations; re-apply the lowering", "installations", res.Ahead)
		}
		batch = batch[n:]
	}
}

func (r *Reporter) requeue(batch []Entry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range batch {
		r.pending[e.Installation] = mergeEntry(r.pending[e.Installation], e)
	}
}

// Host is what an activator uses: a fresh grant per boot, and reports.
type Host struct {
	Client   *Client
	Reporter *Reporter
}

// Boot implements activator.FloorAuthority.
func (h *Host) Boot(ctx context.Context, installation string, local Entry) (Grant, error) {
	return h.Client.Boot(ctx, installation, local)
}

// Note implements activator.FloorAuthority.
func (h *Host) Note(e Entry) { h.Reporter.Note(e) }

// Kick implements activator.FloorAuthority.
func (h *Host) Kick() { h.Reporter.Kick() }
