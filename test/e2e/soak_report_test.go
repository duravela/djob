package e2e_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcexec "github.com/testcontainers/testcontainers-go/exec"
)

type soakResources struct {
	ResidentBytes             uint64 `json:"resident_bytes"`
	OpenFiles                 uint64 `json:"open_files"`
	Threads                   uint64 `json:"threads"`
	Goroutines                uint64 `json:"controller_goroutines,omitempty"`
	HeapBytes                 uint64 `json:"controller_heap_objects_bytes,omitempty"`
	DatabaseBytes             uint64 `json:"controller_database_file_bytes,omitempty"`
	DatabaseMetadataObserved  bool   `json:"controller_database_metadata_observed"`
	DatabaseStatsAppliedIndex uint64 `json:"controller_database_stats_applied_index"`
	LiveHeapBytes             uint64 `json:"controller_gc_live_heap_bytes,omitempty"`
	HeapGoalBytes             uint64 `json:"controller_gc_heap_goal_bytes,omitempty"`
	GCCycles                  uint64 `json:"controller_gc_cycles,omitempty"`
	PageSize                  uint64 `json:"controller_database_page_size_bytes,omitempty"`
	FreePages                 uint64 `json:"controller_database_free_pages"`
	PendingPages              uint64 `json:"controller_database_pending_pages"`
	OpenReadTransactions      uint64 `json:"controller_database_open_read_transactions"`
}

type soakProcess struct {
	First soakResources `json:"first"`
	Last  soakResources `json:"last"`
	Peak  soakResources `json:"peak"`
}

type soakReport struct {
	MutationMaxAttempts   int `json:"uncertain_acknowledgement_max_attempts"`
	RetriedMutationCalls  int `json:"retried_uncertain_mutation_calls"`
	MutationRetries       int `json:"uncertain_mutation_retry_attempts"`
	Purged                int `json:"purged_successful_runs"`
	Compacted             int `json:"compacted_run_histories"`
	Replayed              int `json:"verified_compacted_launch_replays"`
	replays               map[string]soakReplay
	ConfigurationSweeps   int    `json:"configuration_receipt_sweeps"`
	ConfigurationReceipts uint64 `json:"compacted_configuration_receipts"`
	ConfigurationReplays  int    `json:"verified_compacted_configuration_replays"`
	configurationReplays  map[string]soakReplay
	Config                soakConfig              `json:"configuration"`
	Started               time.Time               `json:"started_at"`
	Finished              time.Time               `json:"finished_at"`
	Status                string                  `json:"status"`
	Cycles                int                     `json:"cycles"`
	FaultRounds           int                     `json:"fault_rounds"`
	Succeeded             int                     `json:"successful_immediate_runs"`
	Unknown               int                     `json:"expected_unknown_runs"`
	Scheduled             int                     `json:"successful_scheduled_runs"`
	WireRequests          int                     `json:"confirmed_wire_requests"`
	LatencyCount          int                     `json:"observed_latency_samples"`
	LatencySum            time.Duration           `json:"observed_latency_sum_ns"`
	LatencyMax            time.Duration           `json:"observed_latency_max_ns"`
	Latency               map[string]int          `json:"observed_latency_non_cumulative_buckets"`
	Processes             map[string]*soakProcess `json:"processes"`
	h                     *harness
	components            map[string]*testcontainers.DockerContainer
	output                *json.Encoder
}

func newSoakReport(h *harness, cfg soakConfig, components map[string]*testcontainers.DockerContainer) *soakReport {
	dir := os.Getenv("DJOB_TEST_ARTIFACT_DIR")
	if dir == "" {
		dir = h.t.TempDir()
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		h.t.Fatal(err)
	}
	path := filepath.Join(dir, "simulator-soak")
	file, err := os.OpenFile(path+"-samples.jsonl", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		h.t.Fatal(err)
	}
	r := &soakReport{MutationMaxAttempts: soakMutationAttempts, Config: cfg, Started: time.Now().UTC(), Status: "incomplete", replays: make(map[string]soakReplay), Latency: make(map[string]int), Processes: make(map[string]*soakProcess), h: h, components: components, output: json.NewEncoder(file)}
	h.t.Cleanup(func() {
		r.Finished = time.Now().UTC()
		if h.t.Failed() {
			r.Status = "failed"
		}
		data, err := json.MarshalIndent(r, "", "  ")
		if err == nil {
			err = os.WriteFile(path+"-report.json", append(data, '\n'), 0600)
		}
		if err != nil {
			h.t.Error(err)
		}
		if err := file.Close(); err != nil {
			h.t.Error(err)
		}
		h.t.Logf("soak status=%s cycles=%d successes=%d scheduled=%d unknown=%d faults=%d max_observed_latency=%s; report %s-report.json", r.Status, r.Cycles, r.Succeeded, r.Scheduled, r.Unknown, r.FaultRounds, r.LatencyMax, path)
	})
	return r
}

func (r *soakReport) observe(start time.Time, unknown bool) {
	elapsed := time.Since(start)
	r.LatencyCount++
	r.LatencySum += elapsed
	r.LatencyMax = max(r.LatencyMax, elapsed)
	bucket := ">1m"
	for _, bound := range []time.Duration{100 * time.Millisecond, 500 * time.Millisecond, time.Second, 2 * time.Second, 5 * time.Second, 10 * time.Second, 30 * time.Second, time.Minute} {
		if elapsed <= bound {
			bucket = bound.String()
			break
		}
	}
	r.Latency[bucket]++
	if unknown {
		r.Unknown++
	} else {
		r.Succeeded++
	}
	if elapsed > r.Config.MaxLatency {
		r.h.t.Fatalf("submission-to-observation latency %s exceeds %s", elapsed, r.Config.MaxLatency)
	}
}

func (r *soakReport) sample() {
	snapshot := make(map[string]soakResources)
	for name, container := range r.components {
		ctx, cancel := context.WithTimeout(r.h.ctx, 10*time.Second)
		code, reader, err := container.Exec(ctx, []string{"/gateway", "--process-stats"}, tcexec.Multiplexed())
		var current soakResources
		if err == nil && code == 0 {
			err = json.NewDecoder(io.LimitReader(reader, 4096)).Decode(&current)
		}
		cancel()
		if err != nil || code != 0 {
			r.h.t.Fatalf("sample %s: exit=%d error=%v", name, code, err)
		}
		if strings.HasPrefix(name, "member-") {
			i, _ := strconv.Atoi(strings.TrimPrefix(name, "member-"))
			values, err := r.h.monitorValues(i - 1)
			if err != nil {
				r.h.t.Fatal(err)
			}
			for _, key := range []string{"djob_runtime_goroutines", "djob_runtime_heap_objects_bytes", "djob_runtime_gc_live_heap_bytes", "djob_runtime_gc_heap_goal_bytes", "djob_runtime_gc_cycles_total", "djob_store_file_bytes"} {
				if _, ok := values[key]; !ok {
					r.h.t.Fatalf("missing soak observation %s", key)
				}
			}
			current.Goroutines = uint64(values["djob_runtime_goroutines"])
			current.HeapBytes = uint64(values["djob_runtime_heap_objects_bytes"])
			current.DatabaseBytes = uint64(values["djob_store_file_bytes"])
			current.LiveHeapBytes = uint64(values["djob_runtime_gc_live_heap_bytes"])
			current.HeapGoalBytes = uint64(values["djob_runtime_gc_heap_goal_bytes"])
			current.GCCycles = uint64(values["djob_runtime_gc_cycles_total"])
			_, current.DatabaseMetadataObserved = values["djob_store_page_size_bytes"]
			// A busy first background probe has no observation yet. It must not
			// be represented as a measured zero or fail an otherwise ready node.
			if current.DatabaseMetadataObserved {
				for _, key := range []string{"djob_store_stats_applied_index", "djob_store_free_pages", "djob_store_pending_pages", "djob_store_open_read_transactions"} {
					if _, ok := values[key]; !ok {
						r.h.t.Fatalf("incomplete database metadata observation %s", key)
					}
				}
			}
			current.DatabaseStatsAppliedIndex = uint64(values["djob_store_stats_applied_index"])
			current.PageSize = uint64(values["djob_store_page_size_bytes"])
			current.FreePages = uint64(values["djob_store_free_pages"])
			current.PendingPages = uint64(values["djob_store_pending_pages"])
			current.OpenReadTransactions = uint64(values["djob_store_open_read_transactions"])
		}
		process := r.Processes[name]
		if process == nil {
			process = &soakProcess{First: current}
			r.Processes[name] = process
		}
		process.Last = current
		p := &process.Peak
		p.ResidentBytes = max(p.ResidentBytes, current.ResidentBytes)
		p.OpenFiles = max(p.OpenFiles, current.OpenFiles)
		p.Threads = max(p.Threads, current.Threads)
		p.Goroutines = max(p.Goroutines, current.Goroutines)
		p.HeapBytes = max(p.HeapBytes, current.HeapBytes)
		p.LiveHeapBytes = max(p.LiveHeapBytes, current.LiveHeapBytes)
		p.HeapGoalBytes = max(p.HeapGoalBytes, current.HeapGoalBytes)
		p.GCCycles = max(p.GCCycles, current.GCCycles)
		p.DatabaseMetadataObserved = p.DatabaseMetadataObserved || current.DatabaseMetadataObserved
		p.DatabaseStatsAppliedIndex = max(p.DatabaseStatsAppliedIndex, current.DatabaseStatsAppliedIndex)
		p.PageSize = current.PageSize
		p.FreePages = max(p.FreePages, current.FreePages)
		p.PendingPages = max(p.PendingPages, current.PendingPages)
		p.OpenReadTransactions = max(p.OpenReadTransactions, current.OpenReadTransactions)
		p.DatabaseBytes = max(p.DatabaseBytes, current.DatabaseBytes)
		snapshot[name] = current
		if cap := r.Config.MaxRSSMiB; cap > 0 && current.ResidentBytes > uint64(cap)<<20 {
			r.h.t.Fatalf("%s resident bytes %d exceed %d MiB", name, current.ResidentBytes, cap)
		}
		if cap := r.Config.MaxOpenFiles; cap > 0 && current.OpenFiles > uint64(cap) {
			r.h.t.Fatalf("%s open files %d exceed %d", name, current.OpenFiles, cap)
		}
	}
	if err := r.output.Encode(object{"at": time.Now().UTC(), "cycles": r.Cycles, "processes": snapshot}); err != nil {
		r.h.t.Fatal(err)
	}
}

func (h *harness) monitorValues(member int) (map[string]float64, error) {
	req, err := http.NewRequestWithContext(h.ctx, http.MethodGet, h.urls[member]+"/metrics", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+h.tokens["monitor"])
	response, err := h.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return nil, fmt.Errorf("metrics status %d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		return nil, err
	}
	values := make(map[string]float64)
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 || strings.Contains(fields[0], "{") {
			continue
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		if err != nil {
			return nil, err
		}
		values[fields[0]] = value
	}
	return values, nil
}
