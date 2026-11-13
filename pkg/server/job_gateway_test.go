package server

import (
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor"
	"github.com/duravela/djob/pkg/executor/fake"
	"github.com/duravela/djob/pkg/jobs"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	old "github.com/duravela/djob/pkg/proto/scheduler/v1"
	"github.com/stretchr/testify/require"
)

func finishGatewayCall(call *fake.Call, data string) {
	call.Reply <- fake.Response{Outcome: executor.Outcome{Status: db.AttemptSucceeded, Data: []byte(data)}}
	call.Recover <- nil
	call.Close <- nil
}

func TestPublicGenericConversationAndLegacyJobsShareGatewayThroughIdleHandover(t *testing.T) {
	g := fake.New(fake.Options{})
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	cfg.TransportFactory = g.Factory
	cfg.Dispatcher.SharedIdleTimeout = 20 * time.Millisecond
	cfg.JobDispatch.PollInterval = 5 * time.Millisecond
	cfg.JobDispatch.MaxConcurrent, cfg.JobDispatch.MaxPerTenant = 4, 2
	cfg.JobRegistry = func(backend JobBackend) (jobs.Registry, error) {
		r := jobs.Builtins()
		r.Handlers[jobs.Version{Name: "sequence", Version: "1"}] = func(v jobs.View) (jobs.Decision, error) {
			switch string(v.Run.State) {
			case "":
				return jobs.Decision{Kind: db.JobDoAction, State: []byte("selected"), Operation: "select"}, nil
			case "selected":
				return jobs.Decision{Kind: db.JobWait, State: []byte("waiting"), WaitKey: "continue"}, nil
			case "waiting":
				return jobs.Decision{Kind: db.JobDoAction, State: []byte("read"), Operation: "read", Input: v.Event.Data}, nil
			case "read":
				return jobs.Decision{Kind: db.JobDoAction, State: []byte("finished"), Operation: "finish", Input: v.Run.PendingAction.Result}, nil
			default:
				return jobs.Decision{Kind: db.JobComplete, Status: db.JobSucceeded, Result: v.Run.PendingAction.Result}, nil
			}
		}
		r.Drivers[jobs.Version{Name: "gateway", Version: "1"}] = jobs.GatewayFactory(backend, g.Factory, func(_ db.JobBinding, a jobs.Action) (string, []byte, error) {
			return "fake", append([]byte(a.Operation+":"), a.Input...), nil
		}, time.Second)
		return r, nil
	}
	s := startAPI(t, cfg, g)
	c := genericClient(t, s)
	legacy := apiClient(t, s)
	operator := genericContext("ops/alice")
	_, err := legacy.ConfigureChannel(operator, &old.ConfigureChannelRequest{Command: meta("gateway"), Id: "gateway", GatewayId: "gateway", Protocol: "fake", Endpoint: "gateway"})
	require.NoError(t, err)
	_, err = legacy.CreateRun(operator, &old.CreateRunRequest{Command: meta("legacy-first"), Id: "legacy-first", ChannelId: "gateway", Executor: "fake"})
	require.NoError(t, err)
	first := apiCall(t, g)
	require.Equal(t, "legacy-first", first.Request.RunID)
	finishGatewayCall(first, "first")
	require.Eventually(t, func() bool {
		r, err := s.application.Store.GetRun("legacy-first")
		return err == nil && r.State == db.RunSucceeded
	}, time.Second, time.Millisecond)
	channel, err := s.application.Store.GetChannel("gateway")
	require.NoError(t, err)
	require.Equal(t, db.ChannelOwned, channel.State, "legacy owner is cached before the resource catalog enables handover")
	mutateJob(t, c, "ops/alice", &jp.MutationRequest{Command: genericMeta("resource"), Operation: &jp.MutationRequest_PutResource{PutResource: &jp.PutResource{Spec: &jp.ResourceSpec{Id: "gateway", ChannelId: "gateway", Duplex: "half", Capacity: 1}}}})
	for _, tenant := range []string{"alpha", "beta"} {
		tenantAPI(t, c, tenant)
		mutateJob(t, c, "ops/alice", &jp.MutationRequest{Command: genericMeta("grant"), TenantId: tenant, Operation: &jp.MutationRequest_PutGrant{PutGrant: &jp.PutGrant{ResourceId: "gateway", Allowed: true}}})
		mutateJob(t, c, "ops/alice", &jp.MutationRequest{Command: genericMeta("binding"), TenantId: tenant, Operation: &jp.MutationRequest_PutBinding{PutBinding: &jp.PutBinding{Binding: &jp.Binding{Id: "device", Driver: "gateway", DriverVersion: "1", Config: []byte(`{"channel_id":"gateway"}`), Resources: []string{"gateway"}}}}})
	}
	definitionAPI(t, c, "alpha", "sequence", "sequence", "device", nil)
	launchAPI(t, c, "alpha", "sequence", "alpha-run")
	selected := apiCall(t, g)
	require.Equal(t, "alpha-run", selected.Request.RunID)
	selected.Reply <- fake.Response{Outcome: executor.Outcome{Status: db.AttemptSucceeded, Data: []byte("selected")}}
	apiPhase(t, selected.RecoveryStarted)
	families := scrapeMetrics(t, s)
	require.Zero(t, actionHistogram(t, families, "djob_job_action_driver_duration_seconds", "state", "succeeded").GetSampleCount(), "driver timing waits for protocol cleanup and recording")
	selected.Recover <- nil
	apiPhase(t, selected.CloseStarted)
	families = scrapeMetrics(t, s)
	require.Zero(t, actionHistogram(t, families, "djob_job_action_record_duration_seconds", "result", "confirmed").GetSampleCount())
	selected.Close <- nil
	require.Eventually(t, func() bool {
		r, err := s.application.Store.GetJobView("alpha", "alpha-run")
		return err == nil && r.Run.Status == db.JobWaiting
	}, time.Second, time.Millisecond)
	families = scrapeMetrics(t, s)
	require.Equal(t, float64(1), metricValue(t, families, "djob_execution_in_flight", "profile", "generic"), "a conversation holding a half-duplex resource stays in flight while waiting")
	require.Zero(t, metricValue(t, families, "djob_execution_returns_total", "profile", "generic", "result", "waiting"))
	require.Equal(t, float64(1), metricValue(t, families, "djob_job_action_results_total", "state", "succeeded", "result", "confirmed"))
	require.Zero(t, timingHistogram(t, families, "djob_event_consumption_delay_seconds", "generic").GetSampleCount())
	definitionAPI(t, c, "beta", "invoke", "invoke", "device", nil)
	launchAPI(t, c, "beta", "invoke", "beta-run")
	_, err = legacy.CreateRun(operator, &old.CreateRunRequest{Command: meta("legacy-second"), Id: "legacy-second", ChannelId: "gateway", Executor: "fake"})
	require.NoError(t, err)
	definitionAPI(t, c, "beta", "hash", "sha256", "", []byte("independent"))
	launchAPI(t, c, "beta", "hash", "compute")
	require.Eventually(t, func() bool {
		r, err := s.application.Store.GetJobView("beta", "compute")
		return err == nil && r.Run.Status == db.JobSucceeded
	}, time.Second, time.Millisecond)
	require.Equal(t, 2, g.Stats().Started, "neither contender may exchange during the selected device's wait")
	mutateJob(t, c, "alpha/callback", &jp.MutationRequest{Command: genericMeta("next"), Operation: &jp.MutationRequest_Signal{Signal: &jp.Signal{RunId: "alpha-run", EventId: "next", Key: "continue", Data: []byte("request")}}})
	read := apiCall(t, g)
	require.Equal(t, "alpha-run", read.Request.RunID)
	require.EqualValues(t, 1, timingHistogram(t, scrapeMetrics(t, s), "djob_event_consumption_delay_seconds", "generic").GetSampleCount(), "event consumption is acknowledged before the next device command starts")
	require.Equal(t, "read:request", string(read.Request.Payload))
	finishGatewayCall(read, "reading")
	finish := apiCall(t, g)
	require.Equal(t, "alpha-run", finish.Request.RunID)
	require.Equal(t, "finish:reading", string(finish.Request.Payload))
	finishGatewayCall(finish, "finished")
	seen := make(map[string]bool)
	for range 2 {
		call := apiCall(t, g)
		seen[call.Request.RunID] = true
		finishGatewayCall(call, "done")
	}
	require.True(t, seen["legacy-second"])
	require.True(t, seen["beta-run"])
	require.Eventually(t, func() bool {
		r, err := s.application.Store.GetJobView("beta", "beta-run")
		old, oldErr := s.application.Store.GetRun("legacy-second")
		return err == nil && oldErr == nil && r.Run.Status == db.JobSucceeded && old.State == db.RunSucceeded
	}, time.Second, time.Millisecond)
	require.Equal(t, 1, g.Stats().MaxActive)
	families = scrapeMetrics(t, s)
	require.Equal(t, float64(4), metricValue(t, families, "djob_job_action_results_total", "state", "succeeded", "result", "confirmed"), "three conversation actions and one other tenant action are observed independently of legacy work")
	require.EqualValues(t, 4, actionHistogram(t, families, "djob_job_action_driver_duration_seconds", "state", "succeeded").GetSampleCount())
	require.EqualValues(t, 4, actionHistogram(t, families, "djob_job_action_record_duration_seconds", "result", "confirmed").GetSampleCount())
	_, err = c.Get(genericContext("alpha/reader"), &jp.GetRequest{Reference: &jp.Reference{Kind: jp.Kind_RUN, Id: "beta-run", TenantId: "beta"}})
	require.Error(t, err)
}
