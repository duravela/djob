package db

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func (h *channelHarness) trigger(tenant, id string, maximum int, resources ...string) *JobTrigger {
	h.t.Helper()
	h.job(tenant, "group", "work", "driver", resources...)
	d := JobTriggerDefinition{TenantID: tenant, ID: id, DefinitionID: "work", DefinitionRevision: 1, MaxPending: maximum, Timing: ScheduleTiming{Spec: ScheduleSpec{Kind: ScheduleInterval, StartAt: h.at, Interval: time.Second}, MissedPolicy: MissedCoalesce}}
	return h.ok(jc(JobCommand{PutTrigger: &PutJobTrigger{Definition: d, NextAt: h.at}})).Job.Trigger
}
func triggerPlan(t *testing.T, trigger JobTrigger, at time.Time) JobCommand {
	t.Helper()
	plan, err := IntervalTimingPlan(trigger.TimingCursor(), at)
	require.NoError(t, err)
	return JobCommand{Materialize: &MaterializeJobs{TenantID: trigger.TenantID, Plan: plan}}
}

func TestJobTriggerPinsConfigurationWithoutReservingResources(t *testing.T) {
	h := newChannelHarness(t)
	h.ok(StateCommand{Configure: &ConfigureChannel{ID: "bus", GatewayID: "bus", Protocol: "fake", Endpoint: "localhost:1"}})
	h.ok(jc(JobCommand{PutResource: &PutJobResource{Spec: ResourceSpec{ID: "bus", ChannelID: "bus", Duplex: "half", Capacity: 1}}}))
	trigger := h.trigger("tenant", "poll", 2, "bus")
	// Future occurrences keep the configuration selected when the trigger was put.
	h.ok(jc(JobCommand{PutDefinition: &PutJobDefinition{ExpectedRevision: 1, Definition: JobDefinition{TenantID: "tenant", ID: "work", Handler: "replacement", HandlerVersion: "2", Runtime: StepRuntime, Input: []byte("new")}}}))
	h.ok(jc(JobCommand{PutBinding: &PutJobBinding{ExpectedRevision: 1, Binding: JobBinding{TenantID: "tenant", ID: "driver", Driver: "replacement", DriverVersion: "2", Config: []byte("new")}}}))
	h.ok(jc(JobCommand{PutGroup: &PutJobGroup{ExpectedRevision: 1, Group: JobGroup{TenantID: "tenant", ID: "group", Runtime: "different"}}}))
	c := jc(triggerPlan(t, *trigger, h.at))
	c.RequestID = "occurrence"
	first := h.ok(c)
	require.Equal(t, 1, first.Job.Created)
	require.Equal(t, first.Fingerprint, h.ok(c).Fingerprint, "same request returns its original receipt")
	h.reject(jc(*c.Job), "schedule_conflict")
	id := JobOccurrenceRunID("tenant", "poll", 1, h.at)
	run, err := h.db.GetJobRun("tenant", id)
	require.NoError(t, err)
	require.Equal(t, "test", run.Definition.Handler)
	require.Equal(t, uint64(1), run.Definition.Revision)
	require.Equal(t, "test", run.Binding.Driver)
	require.Equal(t, uint64(1), run.Group.Revision)
	require.Equal(t, []string{"bus"}, run.Resources)
	require.Equal(t, "poll", run.TriggerID)
	require.Equal(t, h.at, *run.ScheduledAt)
	resource, err := h.db.GetJobResource("bus")
	require.NoError(t, err)
	require.Equal(t, ResourceAvailable, resource.State)
	require.Zero(t, resource.Epoch)
	channel, err := h.db.GetChannel("bus")
	require.NoError(t, err)
	require.Empty(t, channel.OwnerID)
	h.claimJob("tenant", id, "attempt")
	resource, err = h.db.GetJobResource("bus")
	require.NoError(t, err)
	require.Equal(t, ResourceOwned, resource.State)
	// Receipts own their nested snapshots.
	clone := first.Clone()
	clone.Job.Trigger.Template.Resources[0] = "changed"
	require.Equal(t, "bus", first.Job.Trigger.Template.Resources[0])
}

func TestJobTriggerAdmissionRevocationAndPendingLimitAreAtomic(t *testing.T) {
	h := newChannelHarness(t)
	trigger := h.trigger("tenant", "poll", 1, "bus")
	plan := triggerPlan(t, *trigger, h.at)
	h.ok(jc(JobCommand{Grant: &PutResourceGrant{ExpectedRevision: 1, Grant: ResourceGrant{TenantID: "tenant", ResourceID: "bus", Allowed: false}}}))
	h.reject(jc(plan), "resource_denied")
	page, err := h.db.ListJobRuns("tenant", nil, 10)
	require.NoError(t, err)
	require.Empty(t, page.Runs)
	current, err := h.db.GetJobTrigger("tenant", "poll")
	require.NoError(t, err)
	require.Equal(t, h.at, current.NextAt)
	h.ok(jc(JobCommand{Grant: &PutResourceGrant{ExpectedRevision: 2, Grant: ResourceGrant{TenantID: "tenant", ResourceID: "bus", Allowed: true}}}))
	first := h.ok(jc(plan))
	h.at = h.at.Add(time.Second)
	secondPlan := triggerPlan(t, *first.Job.Trigger, h.at)
	h.reject(jc(secondPlan), "backpressure")
	all, own, err := h.db.ScheduledJobPendingCounts("tenant", "poll")
	require.NoError(t, err)
	require.Equal(t, 1, all)
	require.Equal(t, 1, own)
	id := JobOccurrenceRunID("tenant", "poll", 1, trigger.NextAt)
	owner := h.claimJob("tenant", id, "attempt")
	h.at = h.at.Add(time.Minute)
	h.ok(jc(JobCommand{Expire: &ExpireJob{JobOwner: owner, LeaseUntil: h.at}}))
	h.reject(jc(triggerPlan(t, *first.Job.Trigger, h.at)), "backpressure")
	all, _, err = h.db.ScheduledJobPendingCounts("tenant", "poll")
	require.NoError(t, err)
	require.Equal(t, 1, all, "unknown effects hold their pending slot")
	h.ok(jc(JobCommand{Resolve: &ResolveJob{TenantID: "tenant", RunID: id, Status: JobFailed, Actor: "operator", Reason: "confirmed device result"}}))
	current, err = h.db.GetJobTrigger("tenant", "poll")
	require.NoError(t, err)
	h.ok(jc(triggerPlan(t, *current, h.at)))
	resource, err := h.db.GetJobResource("bus")
	require.NoError(t, err)
	require.Equal(t, ResourceBlocked, resource.State, "new ready work never releases an uncertain physical reservation")
}

func TestJobTriggerTenantBoundCannotBeEvadedWithAdditionalTriggers(t *testing.T) {
	h := newChannelHarness(t)
	h.job("tenant", "", "work", "")
	var triggers []*JobTrigger
	for _, id := range []string{"first", "second", "third"} {
		timing := ScheduleTiming{Spec: ScheduleSpec{Kind: ScheduleInterval, StartAt: h.at, Interval: time.Second}, MissedPolicy: MissedReplay, CatchUpLimit: 128}
		if id == "third" {
			timing.MissedPolicy = MissedCoalesce
		}
		d := JobTriggerDefinition{TenantID: "tenant", ID: id, DefinitionID: "work", DefinitionRevision: 1, MaxPending: 128, Timing: timing}
		triggers = append(triggers, h.ok(jc(JobCommand{PutTrigger: &PutJobTrigger{Definition: d, NextAt: h.at}})).Job.Trigger)
	}
	h.at = h.at.Add(127 * time.Second)
	for _, trigger := range triggers[:2] {
		require.Equal(t, 128, h.ok(jc(triggerPlan(t, *trigger, h.at))).Job.Created)
	}
	third := jc(triggerPlan(t, *triggers[2], h.at))
	h.reject(third, "backpressure")
	id := JobOccurrenceRunID("tenant", "first", 1, triggers[0].NextAt)
	h.ok(jc(JobCommand{Cancel: &CancelJob{TenantID: "tenant", RunID: id, Reason: "cancel queued occurrence"}}))
	h.ok(third)
	all, own, err := h.db.ScheduledJobPendingCounts("tenant", "third")
	require.NoError(t, err)
	require.Equal(t, MaxTenantScheduledPending, all)
	require.Equal(t, 1, own)
}

func TestJobTriggerPauseUpdateAndValidationRejectStalePlans(t *testing.T) {
	h := newChannelHarness(t)
	trigger := h.trigger("tenant", "poll", 2)
	plan := triggerPlan(t, *trigger, h.at)
	paused := h.ok(jc(JobCommand{PauseTrigger: &PauseJobTrigger{TenantID: "tenant", ID: "poll", ExpectedRevision: 1, Paused: true}})).Job.Trigger
	require.Equal(t, uint64(2), paused.Revision)
	h.reject(jc(plan), "schedule_conflict")
	pausedPlan := triggerPlan(t, *paused, h.at)
	h.reject(jc(pausedPlan), "schedule_inactive")
	due, err := h.db.ListDueJobTriggers(h.at, nil, 10)
	require.NoError(t, err)
	require.Empty(t, due.Triggers)
	resumed := h.ok(jc(JobCommand{PauseTrigger: &PauseJobTrigger{TenantID: "tenant", ID: "poll", ExpectedRevision: 2}})).Job.Trigger
	plan = triggerPlan(t, *resumed, h.at)
	malformed := *plan.Materialize
	malformed.Plan.Occurrences = []time.Time{h.at.Add(time.Millisecond)}
	h.reject(jc(JobCommand{Materialize: &malformed}), "invalid")
	require.Equal(t, 1, h.ok(jc(plan)).Job.Created)
	d := resumed.JobTriggerDefinition
	// Reconfiguration cannot reinterpret an occurrence already due under a new revision.
	h.reject(jc(JobCommand{PutTrigger: &PutJobTrigger{Definition: d, ExpectedRevision: 3, NextAt: h.at}}), "invalid")
	h.at = h.at.Add(time.Second)
	next := h.ok(jc(JobCommand{PutTrigger: &PutJobTrigger{Definition: d, ExpectedRevision: 3, NextAt: h.at.Add(time.Second)}})).Job.Trigger
	require.Equal(t, uint64(4), next.Revision)
	require.NotEqual(t, JobOccurrenceRunID("tenant", "poll", 3, h.at), JobOccurrenceRunID("other", "poll", 3, h.at))
}

func TestJobTriggerSnapshotRestoreAndFormatActivation(t *testing.T) {
	h := newChannelHarness(t)
	h.reject(jc(JobCommand{PauseTrigger: &PauseJobTrigger{TenantID: "missing", ID: "missing"}}), "not_found")
	require.NoError(t, h.db.db.View(func(tx *bbolt.Tx) error {
		require.Equal(t, []byte{3}, tx.Bucket(metadataBucket).Get(formatKey))
		return nil
	}))
	trigger := h.trigger("tenant", "poll", 2)
	materialized := h.ok(jc(triggerPlan(t, *trigger, h.at))).Job.Trigger
	// A subsequent v8 command must never downgrade the format marker.
	h.launch("tenant", "work", "manual")
	var snapshot bytes.Buffer
	require.NoError(t, h.db.Snapshot(&snapshot))
	restored, err := NewDB(filepath.Join(t.TempDir(), "restored.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = restored.Close() })
	require.NoError(t, restored.RestoreAt(&snapshot, h.index))
	got, err := restored.GetJobTrigger("tenant", "poll")
	require.NoError(t, err)
	require.Equal(t, *materialized, *got)
	capacity, err := restored.JobTriggerCapacity("tenant", "poll")
	require.NoError(t, err)
	require.Equal(t, 1, capacity)
	due, err := restored.ListDueJobTriggers(h.at.Add(time.Second), nil, 1)
	require.NoError(t, err)
	require.Len(t, due.Triggers, 1)
	require.NoError(t, restored.db.View(func(tx *bbolt.Tx) error {
		require.Equal(t, []byte{3}, tx.Bucket(metadataBucket).Get(formatKey))
		return nil
	}))
	require.NoError(t, restored.db.Update(func(tx *bbolt.Tx) error { return tx.DeleteBucket(jobTriggerPendingBucket) }))
	require.ErrorContains(t, restored.db.Update(initializeJobTriggerState), "missing bucket")
}

func TestJobTriggerFingerprintsNormalizeCalendarTimesAndRetainV8(t *testing.T) {
	at := time.Date(2026, 11, 4, 12, 0, 0, 0, time.FixedZone("zone", 3600))
	for i, c := range []JobCommand{
		{PutTrigger: &PutJobTrigger{Definition: JobTriggerDefinition{TenantID: "t", ID: "trigger", Timing: ScheduleTiming{Spec: ScheduleSpec{StartAt: at}}}, NextAt: at}},
		{Materialize: &MaterializeJobs{TenantID: "t", Plan: MaterializeRuns{ScheduleID: "trigger", ExpectedNext: at, Occurrences: []time.Time{at}, NextAt: at.Add(time.Hour)}}},
	} {
		command := StateCommand{RequestID: fmt.Sprint(i), At: at, Job: &c}
		require.Equal(t, JobTriggerCommandVersion, command.CommandVersion())
		hash, err := command.Fingerprint()
		require.NoError(t, err)
		var encoded bytes.Buffer
		require.NoError(t, gob.NewEncoder(&encoded).Encode(command))
		var decoded StateCommand
		require.NoError(t, gob.NewDecoder(&encoded).Decode(&decoded))
		other, err := decoded.Fingerprint()
		require.NoError(t, err)
		require.Equal(t, hash, other)
	}
	old := StateCommand{RequestID: "old", At: at, Job: &JobCommand{Launch: &LaunchJob{TenantID: "t", RunID: "r", DefinitionID: "d", DefinitionRevision: 1}}}
	require.Equal(t, JobCommandVersion, old.CommandVersion())
}
