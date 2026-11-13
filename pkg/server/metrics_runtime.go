package server

import (
	"runtime/metrics"

	"github.com/prometheus/client_golang/prometheus"
)

// Runtime samples require no filesystem or database access. Process RSS, file
// descriptors and worker processes are measured externally by the soak harness.
type runtimeResourcesCollector struct {
	goroutines, heap             *prometheus.Desc
	liveHeap, heapGoal, gcCycles *prometheus.Desc
}

func newRuntimeResourcesCollector() *runtimeResourcesCollector {
	return &runtimeResourcesCollector{
		goroutines: prometheus.NewDesc("djob_runtime_goroutines", "Current Go goroutines in this controller process.", nil, nil),
		heap:       prometheus.NewDesc("djob_runtime_heap_objects_bytes", "Bytes occupied by live or unswept Go heap objects in this controller process; excludes stacks and other runtime memory.", nil, nil),
		liveHeap:   prometheus.NewDesc("djob_runtime_gc_live_heap_bytes", "Heap bytes marked live by the most recent completed GC; this observation does not force GC.", nil, nil),
		heapGoal:   prometheus.NewDesc("djob_runtime_gc_heap_goal_bytes", "Current Go heap target; not a process RSS limit.", nil, nil),
		gcCycles:   prometheus.NewDesc("djob_runtime_gc_cycles_total", "Completed Go garbage collection cycles.", nil, nil),
	}
}

func (c *runtimeResourcesCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.goroutines
	ch <- c.heap
	ch <- c.liveHeap
	ch <- c.heapGoal
	ch <- c.gcCycles
}

func (c *runtimeResourcesCollector) Collect(ch chan<- prometheus.Metric) {
	samples := []metrics.Sample{{Name: "/sched/goroutines:goroutines"}, {Name: "/memory/classes/heap/objects:bytes"}, {Name: "/gc/heap/live:bytes"}, {Name: "/gc/heap/goal:bytes"}, {Name: "/gc/cycles/total:gc-cycles"}}
	metrics.Read(samples)
	for i, desc := range []*prometheus.Desc{c.goroutines, c.heap, c.liveHeap, c.heapGoal, c.gcCycles} {
		kind := prometheus.GaugeValue
		if i == 4 {
			kind = prometheus.CounterValue
		}
		ch <- prometheus.MustNewConstMetric(desc, kind, float64(samples[i].Value.Uint64()))
	}
}
