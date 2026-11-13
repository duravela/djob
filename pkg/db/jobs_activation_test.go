package db

import (
	"bytes"
	"encoding/gob"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func (h *channelHarness) eventTrigger(tenant, id, source string, maximum int) *JobTrigger {
	h.t.Helper()
	return h.ok(jc(JobCommand{PutTrigger: &PutJobTrigger{Definition: JobTriggerDefinition{TenantID: tenant, ID: id, DefinitionID: "work", DefinitionRevision: 1, MaxPending: maximum, Event: &EventTriggerSpec{Source: source}}}})).Job.Trigger
}
func activation(tenant, trigger, source, id string, at time.Time) JobCommand {
	return JobCommand{Activate: &ActivateJob{TenantID: tenant, TriggerID: trigger, Source: source, EventID: id, OccurredAt: at, Data: []byte("measurement")}}
}

func TestJobActivationPinsConfigurationAndNeverReservesDeviceOnIngestion(t *testing.T) {
	h := newChannelHarness(t)
	h.ok(StateCommand{Configure: &ConfigureChannel{ID: "bus", GatewayID: "bus", Protocol: "fake", Endpoint: "localhost:1"}})
	h.ok(jc(JobCommand{PutResource: &PutJobResource{Spec: ResourceSpec{ID: "bus", ChannelID: "bus", Capacity: 1, Duplex: "half"}}}))
	h.job("tenant", "group", "work", "driver", "bus")
	trigger := h.eventTrigger("tenant", "ingest", "meters", 2)
	h.ok(jc(JobCommand{PutDefinition: &PutJobDefinition{ExpectedRevision: 1, Definition: JobDefinition{TenantID: "tenant", ID: "work", Runtime: StepRuntime, Handler: "replacement", HandlerVersion: "2", Input: []byte("new")}}}))
	c := jc(activation("tenant", "ingest", "meters", "e1", h.at.Add(-time.Hour)))
	c.RequestID = "ingest-first"
	accepted := h.ok(c)
	require.Equal(t, accepted.Fingerprint, h.ok(c).Fingerprint)
	require.Equal(t, accepted.Job.Activation, h.ok(jc(*c.Job)).Job.Activation)
	run, err := h.db.GetJobRun("tenant", accepted.Job.Activation.RunID)
	require.NoError(t, err)
	require.Equal(t, "test", run.Definition.Handler)
	require.Equal(t, uint64(1), run.Definition.Revision)
	require.Equal(t, "measurement", string(run.Activation.Data))
	require.Nil(t, run.ScheduledAt)
	require.Equal(t, "ingest", run.TriggerID)
	require.Equal(t, trigger.Revision, run.TriggerRevision)
	require.Equal(t, h.at.Add(-time.Hour), run.Activation.OccurredAt)
	require.Equal(t, h.at, run.Activation.AcceptedAt)
	resource, err := h.db.GetJobResource("bus")
	require.NoError(t, err)
	require.Equal(t, ResourceAvailable, resource.State)
	require.Zero(t, resource.Epoch)
	channel, err := h.db.GetChannel("bus")
	require.NoError(t, err)
	require.Empty(t, channel.OwnerID)
	page, err := h.db.ListDueJobTriggers(h.at.Add(time.Hour), nil, 10)
	require.NoError(t, err)
	require.Empty(t, page.Triggers)
	h.claimJob("tenant", run.ID, "attempt")
	resource, err = h.db.GetJobResource("bus")
	require.NoError(t, err)
	require.Equal(t, ResourceOwned, resource.State)
	clone := accepted.Clone()
	clone.Job.Activation.Data[0] = 'x'
	require.Equal(t, "measurement", string(accepted.Job.Activation.Data))
	clonedRun := (StateResult{Job: &JobResult{Run: run, Trigger: trigger}}).Clone()
	clonedRun.Job.Run.Activation.Data[0] = 'x'
	clonedRun.Job.Trigger.Event.Source = "changed"
	require.Equal(t, "measurement", string(run.Activation.Data))
	require.Equal(t, "meters", trigger.Event.Source)
}

func TestJobActivationDeduplicatesAcrossTriggerRevisionsAndRejectsConflictingData(t *testing.T) {
	h := newChannelHarness(t)
	h.job("tenant", "", "work", "")
	trigger := h.eventTrigger("tenant", "ingest", "meters", 1)
	event := activation("tenant", "ingest", "meters", "e1", h.at)
	accepted := h.ok(jc(event)).Job.Activation
	changed := *event.Activate
	changed.Data = []byte("different")
	h.reject(jc(JobCommand{Activate: &changed}), "activation_conflict")
	changed = *event.Activate
	changed.OccurredAt = changed.OccurredAt.Add(time.Second)
	h.reject(jc(JobCommand{Activate: &changed}), "activation_conflict")
	d := trigger.JobTriggerDefinition
	d.Event = &EventTriggerSpec{Source: "replacement"}
	d.Paused = true
	h.ok(jc(JobCommand{PutTrigger: &PutJobTrigger{Definition: d, ExpectedRevision: 1}}))
	require.Equal(t, accepted, h.ok(jc(event)).Job.Activation, "accepted events survive source and pause changes")
	h.reject(jc(activation("tenant", "ingest", "meters", "e2", h.at)), "source_mismatch")
	h.reject(jc(activation("tenant", "ingest", "replacement", "e2", h.at)), "trigger_inactive")
	h.ok(jc(JobCommand{Cancel: &CancelJob{TenantID: "tenant", RunID: accepted.RunID}}))
	h.ok(jc(JobCommand{PauseTrigger: &PauseJobTrigger{TenantID: "tenant", ID: "ingest", ExpectedRevision: 2}}))
	next := h.ok(jc(activation("tenant", "ingest", "replacement", "e2", h.at))).Job.Activation
	require.Equal(t, uint64(3), next.TriggerRevision)
	require.NotEqual(t, accepted.RunID, next.RunID)
	require.Equal(t, accepted, h.ok(jc(event)).Job.Activation)
}

func TestJobActivationBackpressureAndRevocationNeverAcceptOrLoseRejectedEvents(t *testing.T) {
	h := newChannelHarness(t)
	h.job("tenant", "group", "work", "", "bus")
	h.eventTrigger("tenant", "ingest", "meters", 1)
	first := h.ok(jc(activation("tenant", "ingest", "meters", "first", h.at))).Job.Activation
	second := jc(activation("tenant", "ingest", "meters", "second", h.at))
	second.RequestID = "rejected-envelope"
	h.reject(second, "backpressure")
	_, err := h.db.GetJobActivation("tenant", "ingest", "meters", "second")
	require.Error(t, err)
	_, err = h.db.GetJobRun("tenant", EventJobRunID("tenant", "ingest", "meters", "second"))
	require.Error(t, err)
	h.ok(jc(JobCommand{Cancel: &CancelJob{TenantID: "tenant", RunID: first.RunID}}))
	h.reject(second, "backpressure") // A durable rejected envelope keeps its original receipt.
	second.RequestID = "new-envelope"
	h.ok(jc(JobCommand{Grant: &PutResourceGrant{ExpectedRevision: 1, Grant: ResourceGrant{TenantID: "tenant", ResourceID: "bus", Allowed: false}}}))
	h.reject(second, "resource_denied")
	h.ok(jc(JobCommand{Grant: &PutResourceGrant{ExpectedRevision: 2, Grant: ResourceGrant{TenantID: "tenant", ResourceID: "bus", Allowed: true}}}))
	second.RequestID = "admitted-envelope"
	accepted := h.ok(second).Job.Activation
	require.Equal(t, EventJobRunID("tenant", "ingest", "meters", "second"), accepted.RunID)
	all, own, err := h.db.ScheduledJobPendingCounts("tenant", "ingest")
	require.NoError(t, err)
	require.Equal(t, 1, all)
	require.Equal(t, 1, own)
}

func TestJobActivationSharesTenantBacklogWithCalendarTriggers(t *testing.T) {
	h := newChannelHarness(t)
	h.job("tenant", "", "work", "")
	var calendars []*JobTrigger
	for _, id := range []string{"first", "second"} {
		d := JobTriggerDefinition{TenantID: "tenant", ID: id, DefinitionID: "work", DefinitionRevision: 1, MaxPending: 128, Timing: ScheduleTiming{Spec: ScheduleSpec{Kind: ScheduleInterval, StartAt: h.at, Interval: time.Second}, MissedPolicy: MissedReplay, CatchUpLimit: 128}}
		calendars = append(calendars, h.ok(jc(JobCommand{PutTrigger: &PutJobTrigger{Definition: d, NextAt: h.at}})).Job.Trigger)
	}
	h.eventTrigger("tenant", "ingest", "meters", 2)
	h.at = h.at.Add(127 * time.Second)
	for _, c := range calendars {
		h.ok(jc(triggerPlan(t, *c, h.at)))
	}
	event := jc(activation("tenant", "ingest", "meters", "e1", h.at))
	h.reject(event, "backpressure")
	h.ok(jc(JobCommand{Cancel: &CancelJob{TenantID: "tenant", RunID: JobOccurrenceRunID("tenant", "first", 1, calendars[0].NextAt)}}))
	h.ok(event)
	all, _, err := h.db.ScheduledJobPendingCounts("tenant", "ingest")
	require.NoError(t, err)
	require.Equal(t, MaxTenantScheduledPending, all)
}

func TestJobActivationModeChangesKeepIndexesAndDedupHistoryConsistent(t *testing.T) {
	h := newChannelHarness(t)
	calendar := h.trigger("tenant", "ingest", 2)
	eventDefinition := calendar.JobTriggerDefinition
	eventDefinition.Timing = ScheduleTiming{}
	eventDefinition.Event = &EventTriggerSpec{Source: "meters"}
	eventTrigger := h.ok(jc(JobCommand{PutTrigger: &PutJobTrigger{Definition: eventDefinition, ExpectedRevision: 1}})).Job.Trigger
	due, err := h.db.ListDueJobTriggers(h.at, nil, 10)
	require.NoError(t, err)
	require.Empty(t, due.Triggers)
	h.reject(jc(JobCommand{Materialize: &MaterializeJobs{TenantID: "tenant", Plan: MaterializeRuns{ScheduleID: "ingest", Revision: 2}}}), "invalid")
	event := activation("tenant", "ingest", "meters", "e1", h.at)
	accepted := h.ok(jc(event)).Job.Activation
	calendarDefinition := calendar.JobTriggerDefinition
	h.ok(jc(JobCommand{PutTrigger: &PutJobTrigger{Definition: calendarDefinition, ExpectedRevision: eventTrigger.Revision, NextAt: h.at.Add(time.Second)}}))
	require.Equal(t, accepted, h.ok(jc(event)).Job.Activation)
	h.reject(jc(activation("tenant", "ingest", "meters", "e2", h.at)), "source_mismatch")
	due, err = h.db.ListDueJobTriggers(h.at.Add(time.Second), nil, 10)
	require.NoError(t, err)
	require.Len(t, due.Triggers, 1)
	both := calendarDefinition
	both.Event = &EventTriggerSpec{Source: "meters"}
	h.reject(jc(JobCommand{PutTrigger: &PutJobTrigger{Definition: both}}), "invalid")
}

func TestJobActivationSnapshotAndLookupPreserveNamespacesAndFormatGuard(t *testing.T) {
	h := newChannelHarness(t)
	h.reject(jc(activation("missing", "trigger", "source", "event", h.at)), "not_found")
	for _, tenant := range []string{"a", "a/b"} {
		h.job(tenant, "", "work", "")
		h.eventTrigger(tenant, "trigger", "source", 2)
		h.ok(jc(activation(tenant, "trigger", "source", "event", h.at)))
	}
	require.NotEqual(t, EventJobRunID("a", "b/c", "source", "event"), EventJobRunID("a/b", "c", "source", "event"))
	h.launch("a", "work", "manual") // v8 cannot lower the v10 format marker.
	var snapshot bytes.Buffer
	require.NoError(t, h.db.Snapshot(&snapshot))
	q, err := NewDB(filepath.Join(t.TempDir(), "restored.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close() })
	require.NoError(t, q.RestoreAt(&snapshot, h.index))
	accepted, err := q.GetJobActivation("a", "trigger", "source", "event")
	require.NoError(t, err)
	require.Equal(t, "measurement", string(accepted.Data))
	run, err := q.GetJobRun("a", accepted.RunID)
	require.NoError(t, err)
	require.Equal(t, accepted, run.Activation)
	require.Equal(t, "test", run.Definition.Handler)
	capacity, err := q.JobTriggerCapacity("a", "trigger")
	require.NoError(t, err)
	require.Equal(t, 1, capacity)
	query := JobRecordQuery{Kind: JobActivationRecord, TenantID: "a", TriggerID: "trigger"}
	page, err := q.ListJobRecords(query, nil, 1)
	require.NoError(t, err)
	require.Len(t, page.Entries, 1)
	require.Equal(t, "a", page.Entries[0].Record.Activation.TenantID)
	_, err = q.ListJobRecords(JobRecordQuery{Kind: JobActivationRecord, TenantID: "a/b", TriggerID: "trigger"}, page.Entries[0].Cursor, 1)
	require.Error(t, err)
	require.NoError(t, q.db.View(func(tx *bbolt.Tx) error {
		require.Equal(t, []byte{4}, tx.Bucket(metadataBucket).Get(formatKey))
		return nil
	}))
	require.NoError(t, q.db.Update(func(tx *bbolt.Tx) error { return tx.Bucket(metadataBucket).Delete(jobActivationVersionKey) }))
	require.ErrorContains(t, q.db.Update(initializeJobActivationState), "missing its format marker")
}

func TestJobActivationFingerprintSurvivesGobAndZoneNormalization(t *testing.T) {
	at := time.Date(2026, 11, 4, 12, 0, 0, 0, time.FixedZone("source", 3600))
	c := StateCommand{RequestID: "event", At: at, Job: &JobCommand{Activate: &ActivateJob{TenantID: "t", TriggerID: "trigger", Source: "source", EventID: "event", OccurredAt: at, Data: []byte{0, 255}}}}
	require.Equal(t, JobActivationCommandVersion, c.CommandVersion())
	hash, err := c.Fingerprint()
	require.NoError(t, err)
	var b bytes.Buffer
	require.NoError(t, gob.NewEncoder(&b).Encode(c))
	var decoded StateCommand
	require.NoError(t, gob.NewDecoder(&b).Decode(&decoded))
	other, err := decoded.Fingerprint()
	require.NoError(t, err)
	require.Equal(t, hash, other)
	decoded.Job.Activate.OccurredAt = decoded.Job.Activate.OccurredAt.UTC()
	other, err = decoded.Fingerprint()
	require.NoError(t, err)
	require.Equal(t, hash, other)
	decoded.Job.Activate.Source = string([]byte{255})
	require.Error(t, decoded.Validate())
}
