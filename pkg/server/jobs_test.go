package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/node"
	"github.com/duravela/djob/pkg/scheduler"
	"github.com/stretchr/testify/require"
)

func TestControllerRestoresGenericClaimsAndTimersWithoutJobWorkers(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	wakes := make(chan scheduler.DueWorkObservation, 4)
	cfg.JobMaintenance.OnWoken = func(o scheduler.DueWorkObservation) { wakes <- o }
	cfg.JobMaintenance.PollInterval, cfg.JobMaintenance.BatchSize = 5*time.Millisecond, 1
	cfg.Raft.ID = *cfg.NodeID
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	core, err := node.OpenApplication(ctx, cfg.Raft)
	require.NoError(t, err)
	t.Cleanup(core.Close)
	_, err = core.Raft.WaitForLeaderContext(ctx)
	require.NoError(t, err)
	base := time.Now().UTC().Add(-time.Minute)
	_, err = core.Store.ProposeStateCommand(ctx, db.StateCommand{RequestID: "bus", At: base, Configure: &db.ConfigureChannel{ID: "bus", GatewayID: "bus", Protocol: "fake", Endpoint: "bus"}})
	require.NoError(t, err)
	commands := []db.JobCommand{
		{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: "tenant"}}},
		{PutResource: &db.PutJobResource{Spec: db.ResourceSpec{ID: "bus", ChannelID: "bus", Duplex: "half", Capacity: 1}}},
		{Grant: &db.PutResourceGrant{Grant: db.ResourceGrant{TenantID: "tenant", ResourceID: "bus", Allowed: true}}},
		{PutBinding: &db.PutJobBinding{Binding: db.JobBinding{TenantID: "tenant", ID: "driver", Driver: "fake", DriverVersion: "1"}}},
		{PutDefinition: &db.PutJobDefinition{Definition: db.JobDefinition{TenantID: "tenant", ID: "device", Handler: "test", HandlerVersion: "1", Runtime: db.StepRuntime, BindingID: "driver", Resources: []string{"bus"}}}},
		{PutDefinition: &db.PutJobDefinition{Definition: db.JobDefinition{TenantID: "tenant", ID: "compute", Handler: "test", HandlerVersion: "1", Runtime: db.StepRuntime}}},
	}
	for _, run := range []string{"device", "compute", "timer"} {
		definition := run
		if run == "timer" {
			definition = "compute"
		}
		owner := db.JobOwner{TenantID: "tenant", RunID: run, AttemptID: "attempt", OwnerID: "old/" + run}
		commands = append(commands,
			db.JobCommand{Launch: &db.LaunchJob{TenantID: "tenant", DefinitionID: definition, DefinitionRevision: 1, RunID: run}},
			db.JobCommand{Claim: &db.ClaimJob{JobOwner: owner, LeaseUntil: base.Add(20 * time.Second)}},
		)
		if run == "device" {
			commands = append(commands, db.JobCommand{Advance: &db.AdvanceJob{JobOwner: owner, Kind: db.JobDoAction, Operation: "write"}})
		}
		if run == "timer" {
			commands = append(commands, db.JobCommand{Advance: &db.AdvanceJob{JobOwner: owner, Kind: db.JobWait, WakeAt: base.Add(10 * time.Second)}})
		}
	}
	for i, c := range commands {
		_, err := core.Store.ProposeStateCommand(ctx, db.StateCommand{RequestID: fmt.Sprintf("generic-seed/%d", i), At: base.Add(time.Duration(i) * time.Millisecond), Job: &c})
		require.NoError(t, err)
	}
	core.Close()
	s := startAPI(t, cfg)
	require.Nil(t, s.dispatcher)
	require.Eventually(t, func() bool {
		for id, want := range map[string]db.JobStatus{"device": db.JobUnknown, "compute": db.JobReady, "timer": db.JobReady} {
			v, err := s.application.Store.GetJobView("tenant", id)
			if err != nil || v.Run.Status != want || v.Run.OwnerID != "" {
				return false
			}
		}
		resource, err := s.application.Store.GetJobResource("bus")
		if err != nil || resource.State != db.ResourceBlocked {
			return false
		}
		channel, err := s.application.Store.GetChannel("bus")
		return err == nil && channel.State == db.ChannelBlocked
	}, 3*time.Second, 5*time.Millisecond)
	page, err := s.application.Store.ListRuns(nil, 256)
	require.NoError(t, err)
	require.Empty(t, page.Runs)
	select {
	case observation := <-wakes:
		require.True(t, observation.TimeKnown)
		require.GreaterOrEqual(t, observation.Lateness, 50*time.Second)
	case <-time.After(3 * time.Second):
		t.Fatal("recovered timer did not produce a wake observation")
	}
	families := scrapeMetrics(t, s)
	woken := timingHistogram(t, families, "djob_timer_wake_lateness_seconds", "generic")
	require.EqualValues(t, 1, woken.GetSampleCount())
	require.GreaterOrEqual(t, woken.GetSampleSum(), float64(50))
	require.Zero(t, timingHistogram(t, families, "djob_queue_age_at_first_admission_seconds", "generic").GetSampleCount())
	require.Zero(t, timingHistogram(t, families, "djob_retry_release_lateness_seconds", "gateway").GetSampleCount())
	require.Empty(t, wakes)
	require.NoError(t, s.Close(context.Background()))
	restarted := startAPI(t, cfg)
	require.Zero(t, timingHistogram(t, scrapeMetrics(t, restarted), "djob_timer_wake_lateness_seconds", "generic").GetSampleCount(), "a ready timer is not re-observed from history after restart")
	require.Empty(t, wakes)
}
