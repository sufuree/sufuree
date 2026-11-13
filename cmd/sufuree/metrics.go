package main

import (
	"context"
	"time"

	"github.com/sufuree/sufuree/internal/budget"
	"github.com/sufuree/sufuree/internal/metrics"
	"github.com/sufuree/sufuree/internal/raftnode"
)

// newMetricsRegistry declares the RFC-0001 §9 metric families. Cardinality is
// bounded: fixed names and known label values only. This is the observation
// path; it carries no mutation authority.
func newMetricsRegistry() *metrics.Registry {
	r := metrics.New()
	r.RegisterCounter("reconcile_runs_total", "Reconcile passes.")
	r.RegisterCounter("reconcile_errors_total", "Failed reconcile passes.")
	r.RegisterCounter("edge_requests_total", "Edge requests by response status.", "status")
	r.RegisterGauge("edge_active", "Edge in-flight admitted requests.")
	r.RegisterGauge("raft_role", "Raft role: 1 leader, 0 follower.")
	r.RegisterGauge("raft_term", "Raft term.")
	r.RegisterGauge("raft_watermark", "Raft progress watermark by kind.", "kind")
	r.RegisterGauge("budget_hosts", "Growth ledger reserved+confirmed hosts.")
	r.RegisterGauge("budget_memory_mib", "Growth ledger reserved+confirmed memory (MiB).")
	r.RegisterHistogram("raft_store_seconds", "Raft storage operation duration.", []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1})
	r.RegisterHistogram("raft_apply_seconds", "Raft FSM apply batch duration.", []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1})
	return r
}

// raftMetricsHooks returns the driver storage/apply latency observers.
func raftMetricsHooks(r *metrics.Registry) (func(string, time.Duration), func(int, time.Duration)) {
	store := r.Histogram("raft_store_seconds")
	apply := r.Histogram("raft_apply_seconds")
	return func(_ string, d time.Duration) { store.Observe(d.Seconds()) },
		func(_ int, d time.Duration) { apply.Observe(d.Seconds()) }
}

// pollMetrics refreshes gauge families read from live state rather than
// incremented on events. A nil node/ledger leaves its gauges unset (unknown),
// never fabricated as zero.
func pollMetrics(ctx context.Context, r *metrics.Registry, node *raftnode.Node, ledger *budget.Ledger) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if node != nil {
				role := 0.0
				if node.IsLeader() {
					role = 1
				}
				r.Gauge("raft_role").Set(role)
				if term, err := node.Term(); err == nil {
					r.Gauge("raft_term").Set(float64(term))
				}
				d, c, p, di, a := node.Watermarks()
				r.Gauge("raft_watermark", "durable").Set(float64(d))
				r.Gauge("raft_watermark", "commit").Set(float64(c))
				r.Gauge("raft_watermark", "persisted").Set(float64(p))
				r.Gauge("raft_watermark", "dispatched").Set(float64(di))
				r.Gauge("raft_watermark", "applied").Set(float64(a))
			}
			if ledger != nil {
				hosts, mem := ledger.Usage()
				r.Gauge("budget_hosts").Set(float64(hosts))
				r.Gauge("budget_memory_mib").Set(float64(mem))
			}
		}
	}
}
