// Package metrics is a lightweight, bounded-cardinality metrics registry with
// Prometheus text exposition (RFC-0001 §9). It is the observation path only: it
// never grants mutation authority, and credentials/request bodies never enter
// it. Label cardinality is bounded by the caller registering fixed label names
// and supplying only known values.
package metrics

import (
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
)

// Registry holds metric families. The zero value is not usable; call New.
type Registry struct {
	mu       sync.Mutex
	families map[string]*family
	order    []string
}

// New creates an empty registry.
func New() *Registry {
	return &Registry{families: map[string]*family{}}
}

type family struct {
	name    string
	help    string
	typ     string // counter | gauge | histogram
	labels  []string
	buckets []float64
	series  map[string]*series
}

type series struct {
	mu     sync.Mutex
	labels []string
	value  float64 // counter / gauge
	// histogram state
	buckets []float64
	counts  []uint64
	sum     float64
	count   uint64
}

func (r *Registry) register(name, help, typ string, labels []string, buckets []float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.families[name]; ok {
		return
	}
	r.families[name] = &family{
		name: name, help: help, typ: typ, labels: labels, buckets: buckets,
		series: map[string]*series{},
	}
	r.order = append(r.order, name)
}

// RegisterCounter declares a monotonic counter family.
func (r *Registry) RegisterCounter(name, help string, labels ...string) {
	r.register(name, help, "counter", labels, nil)
}

// RegisterGauge declares a gauge family.
func (r *Registry) RegisterGauge(name, help string, labels ...string) {
	r.register(name, help, "gauge", labels, nil)
}

// RegisterHistogram declares a histogram family with cumulative buckets.
func (r *Registry) RegisterHistogram(name, help string, buckets []float64, labels ...string) {
	b := append([]float64(nil), buckets...)
	sort.Float64s(b)
	r.register(name, help, "histogram", labels, b)
}

func (r *Registry) seriesFor(name string, values []string) *series {
	r.mu.Lock()
	defer r.mu.Unlock()
	f, ok := r.families[name]
	if !ok {
		return nil
	}
	key := strings.Join(values, "\x00")
	s, ok := f.series[key]
	if !ok {
		s = &series{
			labels:  append([]string(nil), values...),
			buckets: f.buckets,
			counts:  make([]uint64, len(f.buckets)),
		}
		f.series[key] = s
	}
	return s
}

// Counter returns a handle to a counter series. labelValues must match the
// registered label names in order.
func (r *Registry) Counter(name string, labelValues ...string) *Counter {
	return &Counter{s: r.seriesFor(name, labelValues)}
}

// Gauge returns a handle to a gauge series.
func (r *Registry) Gauge(name string, labelValues ...string) *Gauge {
	return &Gauge{s: r.seriesFor(name, labelValues)}
}

// Histogram returns a handle to a histogram series.
func (r *Registry) Histogram(name string, labelValues ...string) *Histogram {
	return &Histogram{s: r.seriesFor(name, labelValues)}
}

// Counter is a monotonic counter handle.
type Counter struct{ s *series }

// Add increments the counter by delta (ignored if negative or nil).
func (c *Counter) Add(delta float64) {
	if c == nil || c.s == nil || delta < 0 {
		return
	}
	c.s.mu.Lock()
	c.s.value += delta
	c.s.mu.Unlock()
}

// Inc increments the counter by one.
func (c *Counter) Inc() { c.Add(1) }

// Gauge is a settable gauge handle.
type Gauge struct{ s *series }

// Set sets the gauge value.
func (g *Gauge) Set(v float64) {
	if g == nil || g.s == nil {
		return
	}
	g.s.mu.Lock()
	g.s.value = v
	g.s.mu.Unlock()
}

// Histogram is an observation handle.
type Histogram struct{ s *series }

// Observe records one value.
func (h *Histogram) Observe(v float64) {
	if h == nil || h.s == nil || math.IsNaN(v) {
		return
	}
	h.s.mu.Lock()
	h.s.sum += v
	h.s.count++
	for i, ub := range h.s.buckets {
		if v <= ub {
			h.s.counts[i]++
		}
	}
	h.s.mu.Unlock()
}

// Handler serves the registry in Prometheus text exposition format. It is
// read-only and carries no mutation authority.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		r.WriteText(w)
	})
}

// WriteText writes the Prometheus text exposition.
func (r *Registry) WriteText(w interface{ Write([]byte) (int, error) }) {
	r.mu.Lock()
	names := append([]string(nil), r.order...)
	fams := make([]*family, 0, len(names))
	for _, n := range names {
		fams = append(fams, r.families[n])
	}
	r.mu.Unlock()

	var b strings.Builder
	for _, f := range fams {
		fmt.Fprintf(&b, "# HELP %s %s\n", f.name, f.help)
		fmt.Fprintf(&b, "# TYPE %s %s\n", f.name, f.typ)
		keys := make([]string, 0, len(f.series))
		for k := range f.series {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			s := f.series[k]
			s.mu.Lock()
			switch f.typ {
			case "counter", "gauge":
				fmt.Fprintf(&b, "%s%s %g\n", f.name, formatLabels(f.labels, s.labels), s.value)
			case "histogram":
				// counts[i] is already cumulative: Observe increments every
				// bucket whose upper bound is >= the observed value.
				for i, ub := range f.buckets {
					lbl := appendLabel(f.labels, s.labels, "le", formatFloat(ub))
					fmt.Fprintf(&b, "%s_bucket%s %d\n", f.name, lbl, s.counts[i])
				}
				lblInf := appendLabel(f.labels, s.labels, "le", "+Inf")
				fmt.Fprintf(&b, "%s_bucket%s %d\n", f.name, lblInf, s.count)
				fmt.Fprintf(&b, "%s_sum%s %g\n", f.name, formatLabels(f.labels, s.labels), s.sum)
				fmt.Fprintf(&b, "%s_count%s %d\n", f.name, formatLabels(f.labels, s.labels), s.count)
			}
			s.mu.Unlock()
		}
	}
	_, _ = w.Write([]byte(b.String()))
}

func formatLabels(names, values []string) string {
	if len(names) == 0 {
		return ""
	}
	parts := make([]string, 0, len(names))
	for i, n := range names {
		v := ""
		if i < len(values) {
			v = values[i]
		}
		parts = append(parts, fmt.Sprintf("%s=%q", n, v))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func appendLabel(names, values []string, extraName, extraValue string) string {
	n := append(append([]string(nil), names...), extraName)
	v := append(append([]string(nil), values...), extraValue)
	return formatLabels(n, v)
}

func formatFloat(v float64) string {
	if v == math.Trunc(v) {
		return fmt.Sprintf("%d", int64(v))
	}
	return fmt.Sprintf("%g", v)
}
