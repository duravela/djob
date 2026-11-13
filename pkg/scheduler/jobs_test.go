package scheduler

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor/fake"
	"github.com/stretchr/testify/require"
)

func setupMaintenanceJobs(t *testing.T, f *runtimeFixture) {
	t.Helper()
	for _, c := range []db.JobCommand{
		{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: "tenant"}}},
		{PutResource: &db.PutJobResource{Spec: db.ResourceSpec{ID: "bus", ChannelID: "bus", Duplex: "half", Capacity: 1}}},
		{Grant: &db.PutResourceGrant{Grant: db.ResourceGrant{TenantID: "tenant", ResourceID: "bus", Allowed: true}}},
		{PutBinding: &db.PutJobBinding{Binding: db.JobBinding{TenantID: "tenant", ID: "driver", Driver: "fake", DriverVersion: "1"}}},
		{PutDefinition: &db.PutJobDefinition{Definition: db.JobDefinition{TenantID: "tenant", ID: "generic", Runtime: db.StepRuntime, Handler: "handler", HandlerVersion: "1", BindingID: "driver"}}},
		{PutDefinition: &db.PutJobDefinition{Definition: db.JobDefinition{TenantID: "tenant", ID: "device", Runtime: db.StepRuntime, Handler: "handler", HandlerVersion: "1", BindingID: "driver", Resources: []string{"bus"}}}},
	} {
		f.state(db.StateCommand{Job: &c})
	}
}

type maintenanceBackend struct {
	*runtimeBackend
	lost   atomic.Bool
	before func(db.StateCommand)
}

func (b *maintenanceBackend) ProposeStateCommand(ctx context.Context, c db.StateCommand) (db.StateResult, error) {
	if b.before != nil {
		f := b.before
		b.before = nil
		f(c)
	}
	r, err := b.runtimeBackend.ProposeStateCommand(ctx, c)
	if err == nil && b.lost.CompareAndSwap(true, false) {
		return db.StateResult{}, context.DeadlineExceeded
	}
	return r, err
}

func TestJobMaintenanceBoundsDiscoveryAndPreservesEffectUncertainty(t *testing.T) {
	f := newRuntime(t, fake.Options{}, "bus")
	setupMaintenanceJobs(t, f)
	until := time.Now().UTC().Add(time.Minute)
	for _, id := range []string{"a-checkpoint", "b-effect", "c-bus", "d-timer"} {
		definition := "generic"
		if id == "c-bus" {
			definition = "device"
		}
		f.state(db.StateCommand{Job: &db.JobCommand{Launch: &db.LaunchJob{TenantID: "tenant", DefinitionID: definition, DefinitionRevision: 1, RunID: id}}})
		owner := db.JobOwner{TenantID: "tenant", RunID: id, AttemptID: "attempt", OwnerID: "worker/" + id}
		f.state(db.StateCommand{Job: &db.JobCommand{Claim: &db.ClaimJob{JobOwner: owner, LeaseUntil: until}}})
		if id == "b-effect" {
			f.state(db.StateCommand{Job: &db.JobCommand{Advance: &db.AdvanceJob{JobOwner: owner, Kind: db.JobDoAction, Operation: "write"}}})
		}
		if id == "d-timer" {
			f.state(db.StateCommand{Job: &db.JobCommand{Advance: &db.AdvanceJob{JobOwner: owner, Kind: db.JobWait, WakeAt: until}}})
		}
	}
	backend := &maintenanceBackend{runtimeBackend: f.backend}
	backend.lost.Store(true)
	leader := false
	var observations []DueWorkObservation
	s, err := NewJobMaintenance(JobMaintenanceConfig{BatchSize: 1, OnWoken: func(o DueWorkObservation) { observations = append(observations, o) }}, backend, func() bool { return leader })
	require.NoError(t, err)
	n, err := s.Tick(context.Background(), until)
	require.NoError(t, err)
	require.Zero(t, n)
	leader = true
	for _, want := range []int{2, 1, 1, 0} {
		n, err = s.Tick(context.Background(), until)
		require.NoError(t, err)
		require.Equal(t, want, n)
	}
	for id, want := range map[string]db.JobStatus{"a-checkpoint": db.JobReady, "b-effect": db.JobUnknown, "c-bus": db.JobUnknown, "d-timer": db.JobReady} {
		run, err := f.backend.GetJobRun("tenant", id)
		require.NoError(t, err)
		require.Equal(t, want, run.Status)
	}
	require.Len(t, observations, 1, "lease expiry and abandoned effects are not timer-wake samples")
	require.True(t, observations[0].TimeKnown)
	channel, err := f.backend.GetChannel("bus")
	require.NoError(t, err)
	require.Equal(t, db.ChannelBlocked, channel.State)
	resource, err := f.backend.GetJobResource("bus")
	require.NoError(t, err)
	require.Equal(t, db.ResourceBlocked, resource.State)
}

func TestJobMaintenanceRechecksRenewedLeaseAndChangedTimer(t *testing.T) {
	for _, timer := range []bool{false, true} {
		t.Run(map[bool]string{false: "lease", true: "timer"}[timer], func(t *testing.T) {
			f := newRuntime(t, fake.Options{}, "bus")
			setupMaintenanceJobs(t, f)
			until := time.Now().UTC().Add(time.Minute)
			owner := db.JobOwner{TenantID: "tenant", RunID: "r", AttemptID: "a", OwnerID: "worker"}
			f.state(db.StateCommand{Job: &db.JobCommand{Launch: &db.LaunchJob{TenantID: "tenant", DefinitionID: "generic", DefinitionRevision: 1, RunID: "r"}}})
			f.state(db.StateCommand{Job: &db.JobCommand{Claim: &db.ClaimJob{JobOwner: owner, LeaseUntil: until}}})
			if timer {
				f.state(db.StateCommand{Job: &db.JobCommand{Advance: &db.AdvanceJob{JobOwner: owner, Kind: db.JobWait, WakeAt: until}}})
			}
			b := &maintenanceBackend{runtimeBackend: f.backend, before: func(c db.StateCommand) {
				if timer {
					f.state(db.StateCommand{Job: &db.JobCommand{Cancel: &db.CancelJob{TenantID: "tenant", RunID: "r"}}})
				} else {
					f.state(db.StateCommand{Job: &db.JobCommand{Renew: &db.RenewJob{JobOwner: owner, LeaseUntil: until.Add(time.Minute)}}})
				}
			}}
			var observations []DueWorkObservation
			s, err := NewJobMaintenance(JobMaintenanceConfig{OnWoken: func(o DueWorkObservation) { observations = append(observations, o) }}, b, func() bool { return true })
			require.NoError(t, err)
			n, err := s.Tick(context.Background(), until)
			require.NoError(t, err)
			require.Zero(t, n)
			require.Empty(t, observations, "a stale timer or lease discovery emits no timing sample")
			run, err := b.GetJobRun("tenant", "r")
			require.NoError(t, err)
			if timer {
				require.Equal(t, db.JobCancelled, run.Status)
			} else {
				require.Equal(t, until.Add(time.Minute), run.LeaseUntil)
				require.Equal(t, db.JobRunning, run.Status)
			}
		})
	}
}
