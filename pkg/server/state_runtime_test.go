package server

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"github.com/duravela/djob/pkg/util/stop"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/require"
)

func TestStateMetricsRemainUnknownUntilBackgroundProbeAndScrapesNeverProbe(t *testing.T) {
	cfg := apiTestConfig(filepath.Join(t.TempDir(), "unopened"))
	cfg.APIAuth = genericCredentials()
	startup, err := NewServer(cfg, stop.NewStopper())
	require.NoError(t, err)
	s := startup.(*Server)
	t.Cleanup(func() { require.NoError(t, s.Close(context.Background())) })
	var probes atomic.Uint32
	s.states.probe = func(context.Context, int) (db.StateInventory, error) {
		probes.Add(1)
		return completeStateInventory(0), nil
	}
	r := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	r.Header.Set("Authorization", "Bearer "+genericToken("ops/alice"))
	w := httptest.NewRecorder()
	s.metricsHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code)
	var parser expfmt.TextParser
	families, err := parser.TextToMetricFamilies(bytes.NewReader(w.Body.Bytes()))
	require.NoError(t, err)
	require.Zero(t, probes.Load())
	require.Nil(t, families["djob_state_inventory"])
	require.Nil(t, families["djob_state_inventory_observed"])
	require.True(t, math.IsInf(metricValue(t, families, "djob_state_sample_age_seconds"), 1))
	require.Zero(t, metricValue(t, families, "djob_state_probe_success"))
	require.Equal(t, DefaultStateSampleInterval.Seconds(), metricValue(t, families, "djob_state_sample_interval_seconds"))
	_, err = os.Stat(cfg.Raft.DataDir)
	require.True(t, os.IsNotExist(err))
}

func TestStateMetricsReportUnknownAndBlockedStateThroughRetentionAndRestart(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	cfg.StateMonitor = StateMonitorConfig{SampleInterval: MinStateSampleInterval, ScanLimit: 2}
	s := startAPI(t, cfg) // No worker is enabled; inventory covers retained state.
	c := genericClient(t, s)
	var seq int
	apply := func(command db.StateCommand) db.StateResult {
		t.Helper()
		seq++
		command.RequestID = fmt.Sprintf("state-inventory/%d", seq)
		if command.At.IsZero() {
			command.At = time.Now().UTC()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		r, err := s.application.Store.ProposeStateCommand(ctx, command)
		require.NoError(t, err)
		require.NoError(t, r.Err())
		return r
	}
	apply(db.StateCommand{Configure: &db.ConfigureChannel{ID: "private-gateway", GatewayID: "private-gateway", Protocol: "unregistered", Endpoint: "private-endpoint:502"}})
	apply(db.StateCommand{Acquire: &db.AcquireChannel{ChannelID: "private-gateway", OwnerID: "private-owner", LeaseUntil: time.Now().UTC().Add(time.Hour)}})
	apply(db.StateCommand{CreateRun: &db.CreateRun{ID: "private-run", ChannelID: "private-gateway", Executor: "unregistered", Payload: []byte("private payload")}})
	owner := db.ChannelOwner{ChannelID: "private-gateway", Epoch: 1, OwnerID: "private-owner"}
	apply(db.StateCommand{Claim: &db.ClaimRun{ChannelOwner: owner, RunID: "private-run", AttemptID: "private-attempt", ClaimToken: "private-token"}})
	apply(db.StateCommand{Finish: &db.FinishAttempt{ChannelOwner: owner, RunID: "private-run", AttemptID: "private-attempt", ClaimToken: "private-token", Outcome: db.AttemptUnknown, Quiescent: false}})
	tenantAPI(t, c, "alpha")
	apply(db.StateCommand{Job: &db.JobCommand{PutResource: &db.PutJobResource{Spec: db.ResourceSpec{ID: "private-resource", Duplex: "half", Capacity: 1}}}})
	apply(db.StateCommand{Job: &db.JobCommand{Grant: &db.PutResourceGrant{Grant: db.ResourceGrant{TenantID: "alpha", ResourceID: "private-resource", Allowed: true}}}})
	apply(db.StateCommand{Job: &db.JobCommand{PutDefinition: &db.PutJobDefinition{Definition: db.JobDefinition{TenantID: "alpha", ID: "work", Runtime: db.StepRuntime, Handler: "unregistered-handler", HandlerVersion: "1", Resources: []string{"private-resource"}, Input: []byte("private input")}}}})
	for _, id := range []string{"a", "b", "c"} {
		launchAPI(t, c, "alpha", "work", id)
	}
	jobOwner := db.JobOwner{TenantID: "alpha", RunID: "a", AttemptID: "private-attempt", OwnerID: "private-owner"}
	lease := time.Now().UTC().Add(time.Minute)
	apply(db.StateCommand{Job: &db.JobCommand{Claim: &db.ClaimJob{JobOwner: jobOwner, LeaseUntil: lease}}})
	apply(db.StateCommand{At: lease, Job: &db.JobCommand{Expire: &db.ExpireJob{JobOwner: jobOwner, LeaseUntil: lease}}})
	mutateJob(t, c, "alpha/writer", &jp.MutationRequest{Command: genericMeta("cancel/b"), Operation: &jp.MutationRequest_Cancel{Cancel: &jp.Cancel{RunId: "b", Reason: "cancel queued work"}}})
	index := s.application.Store.LastAppliedIndex()
	waitInventory := func(server *Server, complete bool) {
		t.Helper()
		require.Eventually(t, func() bool {
			o := server.states.observation()
			return o.haveSample && o.stats.AppliedIndex >= index && o.stats.Kinds[db.InventoryGenericRuns].Complete == complete &&
				o.stats.Kinds[db.InventoryGenericRuns].Observed == 2 && o.stats.Kinds[db.InventoryGenericRuns].Counts[db.InventoryUnknown] == 1 &&
				o.stats.Kinds[db.InventoryChannels].Counts[db.InventoryBlocked] == 1 && o.stats.Kinds[db.InventoryResources].Counts[db.InventoryBlocked] == 1
		}, 5*time.Second, 10*time.Millisecond)
	}
	waitInventory(s, false)
	families := scrapeMetrics(t, s)
	require.Zero(t, metricValue(t, families, "djob_state_sample_complete", "kind", "generic_runs"))
	require.Equal(t, float64(1), metricValue(t, families, "djob_state_inventory_observed", "kind", "generic_runs", "state", "unknown"))
	for _, metric := range families["djob_state_inventory"].Metric {
		for _, label := range metric.Label {
			require.False(t, label.GetName() == "kind" && label.GetValue() == "generic_runs")
		}
	}
	// Deleting an eligible retained run makes the remaining inventory complete.
	mutateJob(t, c, "alpha/writer", &jp.MutationRequest{Command: genericMeta("cancel/c"), Operation: &jp.MutationRequest_Cancel{Cancel: &jp.Cancel{RunId: "c", Reason: "remove queued work"}}})
	run, err := s.application.Store.GetJobRun("alpha", "c")
	require.NoError(t, err)
	apply(db.StateCommand{Job: &db.JobCommand{Purge: &db.PurgeJob{TenantID: "alpha", RunID: "c", ExpectedStep: run.Step, FinishedBefore: time.Now().UTC(), BatchSize: 128, Actor: "operator", Reason: "terminal history cleanup"}}})
	index = s.application.Store.LastAppliedIndex()
	waitInventory(s, true)
	for range 3 {
		families = scrapeMetrics(t, s)
		require.Equal(t, float64(1), metricValue(t, families, "djob_state_inventory", "kind", "gateway_runs", "state", "unknown"))
		require.Equal(t, float64(1), metricValue(t, families, "djob_state_inventory", "kind", "generic_runs", "state", "unknown"))
		require.Equal(t, float64(1), metricValue(t, families, "djob_state_inventory", "kind", "channels", "state", "blocked"))
		require.Equal(t, float64(1), metricValue(t, families, "djob_state_inventory", "kind", "resources", "state", "blocked"))
	}
	require.Equal(t, index, s.application.Store.LastAppliedIndex())
	data := authenticatedHTTP(t, http.DefaultClient, "http://"+s.HTTPAddr(), http.MethodGet, "/metrics", genericToken("ops/alice"), nil, nil, 200, nil)
	for _, private := range []string{"private-gateway", "private-endpoint", "private-owner", "private-resource", "private-run", "private-attempt", "private-token", "private payload", "private input", "unregistered-handler", cfg.Raft.DataDir} {
		require.NotContains(t, string(data), private)
	}
	require.NoError(t, s.Close(context.Background()))
	require.False(t, s.states.observation().running)
	restarted := startAPI(t, cfg)
	waitInventory(restarted, true)
	require.Equal(t, float64(1), metricValue(t, scrapeMetrics(t, restarted), "djob_state_inventory", "kind", "channels", "state", "blocked"))
}

func TestStateMonitorConfigurationIsRejectedBeforeStorageOpens(t *testing.T) {
	for _, config := range []StateMonitorConfig{
		{SampleInterval: -time.Second, ScanLimit: 1}, {SampleInterval: MinStateSampleInterval - 1, ScanLimit: 1},
		{SampleInterval: MaxStateSampleInterval + 1, ScanLimit: 1}, {SampleInterval: time.Second, ScanLimit: -1},
		{SampleInterval: time.Second, ScanLimit: db.MaxStateSampleRows + 1},
	} {
		cfg := apiTestConfig(filepath.Join(t.TempDir(), "unopened"))
		cfg.StateMonitor = config
		stopper := stop.NewStopper()
		_, err := NewServer(cfg, stopper)
		require.Error(t, err)
		stopper.Stop(context.Background())
		_, err = os.Stat(cfg.Raft.DataDir)
		require.True(t, os.IsNotExist(err))
	}
}
