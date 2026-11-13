package server

import (
	"github.com/prometheus/client_golang/prometheus"
	"time"
)

// Runtime observations are bounded by the deployment catalog. Collection never
// calls Kubernetes, reads Raft, or scans durable job records.
type releaseRuntimeCollector struct {
	server *topLevelServer
	desc   *prometheus.Desc
}

func newReleaseRuntimeCollector(s *topLevelServer) *releaseRuntimeCollector {
	return &releaseRuntimeCollector{s, prometheus.NewDesc("djob_release_runtime_observations", "Local cached deployment runtime observations; observations live only on the reconciling member", []string{"state"}, nil)}
}
func (c *releaseRuntimeCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }
func (c *releaseRuntimeCollector) Collect(ch chan<- prometheus.Metric) {
	counts := map[string]uint64{"available": 0, "unavailable": 0, "error": 0, "stale": 0}
	if r := c.server.workerReconciler; r != nil {
		for _, o := range r.Observations() {
			if o.Desired == 0 {
				continue
			}
			state := "unavailable"
			if time.Since(o.ObservedAt) > 30*time.Second || o.ObservedAt.After(time.Now().Add(5*time.Second)) {
				state = "stale"
			} else if o.Error != "" {
				state = "error"
			} else if o.State == "available" && o.Available > 0 {
				state = "available"
			}
			counts[state]++
		}
	}
	for state, n := range counts {
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, float64(n), state)
	}
}
