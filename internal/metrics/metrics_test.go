package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRegistryExposition(t *testing.T) {
	r := New()
	r.RegisterCounter("requests_total", "Total requests.", "status")
	r.RegisterGauge("active", "Active requests.")
	r.RegisterHistogram("latency_seconds", "Latency.", []float64{0.1, 0.5, 1})
	r.Counter("requests_total", "200").Inc()
	r.Counter("requests_total", "200").Inc()
	r.Counter("requests_total", "503").Inc()
	r.Gauge("active").Set(3)
	r.Histogram("latency_seconds").Observe(0.05)
	r.Histogram("latency_seconds").Observe(0.3)
	r.Histogram("latency_seconds").Observe(2)

	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	body := rec.Body.String()
	for _, want := range []string{
		`requests_total{status="200"} 2`,
		`requests_total{status="503"} 1`,
		`active 3`,
		`latency_seconds_count 3`,
		`latency_seconds_bucket{le="+Inf"} 3`,
		`latency_seconds_bucket{le="0.1"} 1`,
		`latency_seconds_bucket{le="0.5"} 2`,
		`latency_seconds_bucket{le="1"} 2`,
		`# TYPE requests_total counter`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in:\n%s", want, body)
		}
	}
}

func TestNilHandlesSafe(t *testing.T) {
	r := New()
	r.Counter("nope").Inc()
	r.Gauge("nope").Set(1)
	r.Histogram("nope").Observe(1)
	var c *Counter
	c.Inc()
	var g *Gauge
	g.Set(1)
	var h *Histogram
	h.Observe(1)
}
