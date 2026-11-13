package db

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/commit"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func retireTrigger(tenant, id string, revision uint64) JobCommand {
	return JobCommand{RetireTrigger: &RetireJobTrigger{TenantID: tenant, ID: id, ExpectedRevision: revision, Actor: "tenant/writer", Reason: "remove obsolete activation configuration"}}
}

func TestJobTriggerRetirementStopsCalendarAndPreservesHalfDuplexWait(t *testing.T) {
	h := newChannelHarness(t)
	h.ok(StateCommand{Configure: &ConfigureChannel{ID: "bus", GatewayID: "bus", Protocol: "fake", Endpoint: "localhost:1"}})
	h.ok(jc(JobCommand{PutResource: &PutJobResource{Spec: ResourceSpec{ID: "bus", ChannelID: "bus", Duplex: "half", Capacity: 1}}}))
	tr := h.trigger("tenant", "poll", 2, "bus")
	first := h.ok(jc(triggerPlan(t, *tr, h.at))).Job.Trigger
	stalePlan := triggerPlan(t, *first, h.at.Add(time.Second))
	id := JobOccurrenceRunID("tenant", "poll", 1, h.at)
	owner := h.claimJob("tenant", id, "attempt")
	wait := h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, Kind: JobWait, WaitKey: "reply", State: []byte("session")}})).Job.Run
	resource, err := h.db.GetJobResource("bus")
	require.NoError(t, err)
	channel, err := h.db.GetChannel("bus")
	require.NoError(t, err)
	before := h.configurationUsage("tenant")

	h.reject(jc(retireTrigger("tenant", "poll", 2)), "revision_conflict")
	stale := jc(retireTrigger("tenant", "poll", 1))
	stale.At = h.at.Add(-time.Second)
	h.reject(stale, "stale_time")
	require.Equal(t, before, h.configurationUsage("tenant"))
	command := jc(retireTrigger("tenant", "poll", 1))
	command.RequestID, command.At = "retire-poll", h.at
	retired := h.ok(command)
	require.Equal(t, &JobTriggerRetirement{TenantID: "tenant", ID: "poll", Revision: 1, RetiredAt: h.at, Actor: "tenant/writer", Reason: command.Job.RetireTrigger.Reason}, retired.Job.TriggerRetirement)
	require.Equal(t, retired, h.ok(command), "an exact retry returns the original audit receipt")
	h.reject(jc(retireTrigger("tenant", "poll", 1)), "gone")
	h.reject(jc(stalePlan), "not_found")
	h.reject(jc(JobCommand{PutTrigger: &PutJobTrigger{Definition: tr.JobTriggerDefinition, NextAt: h.at}}), "gone")
	due, err := h.db.ListDueJobTriggers(h.at.Add(time.Hour), nil, 10)
	require.NoError(t, err)
	require.Empty(t, due.Triggers)
	_, err = h.db.GetJobTrigger("tenant", "poll")
	require.ErrorContains(t, err, "not_found")
	require.Equal(t, before.Triggers-1, h.configurationUsage("tenant").Triggers)
	current, err := h.db.GetJobRun("tenant", id)
	require.NoError(t, err)
	require.Equal(t, wait, current)
	currentResource, err := h.db.GetJobResource("bus")
	require.NoError(t, err)
	require.Equal(t, resource, currentResource)
	currentChannel, err := h.db.GetChannel("bus")
	require.NoError(t, err)
	require.Equal(t, channel, currentChannel)
	all, own, err := h.db.ScheduledJobPendingCounts("tenant", "poll")
	require.NoError(t, err)
	require.Equal(t, 1, all)
	require.Equal(t, 1, own)
	h.ok(jc(JobCommand{Signal: &SignalJob{TenantID: "tenant", RunID: id, EventID: "response", Key: "reply", Data: []byte("response")}}))
	h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, ExpectedStep: 1, EventID: "response", Kind: JobComplete, Status: JobSucceeded, Quiescent: true}}))
	all, own, err = h.db.ScheduledJobPendingCounts("tenant", "poll")
	require.NoError(t, err)
	require.Zero(t, all)
	require.Zero(t, own)
	currentChannel, err = h.db.GetChannel("bus")
	require.NoError(t, err)
	require.Equal(t, ChannelAvailable, currentChannel.State)
	changed := command
	changed.Job = jobCopy(command.Job)
	changed.Job.RetireTrigger = jobCopy(command.Job.RetireTrigger)
	changed.Job.RetireTrigger.Reason = "different reason"
	h.reject(changed, "request_conflict")
}

func TestJobTriggerRetirementReleasesPinnedPayloadUnderLoweredQuota(t *testing.T) {
	h := newChannelHarness(t)
	h.job("tenant", "group", "work", "driver")
	h.ok(jc(JobCommand{PutDefinition: &PutJobDefinition{ExpectedRevision: 1, Definition: JobDefinition{TenantID: "tenant", ID: "work", GroupID: "group", BindingID: "driver", Handler: "test", HandlerVersion: "1", Input: []byte("abc")}}}))
	h.ok(jc(JobCommand{PutBinding: &PutJobBinding{ExpectedRevision: 1, Binding: JobBinding{TenantID: "tenant", ID: "driver", Driver: "test", DriverVersion: "1", Config: []byte("xy")}}}))
	d := JobTriggerDefinition{TenantID: "tenant", ID: "ingest", DefinitionID: "work", DefinitionRevision: 2, MaxPending: 2, Event: &EventTriggerSpec{Source: "meters"}}
	h.ok(jc(JobCommand{PutTrigger: &PutJobTrigger{Definition: d}}))
	require.Equal(t, uint64(10), h.configurationUsage("tenant").PayloadBytes)
	h.configurationLimits(JobConfigurationLimits{TenantID: "tenant", MaxTriggers: 1, MaxPayloadBytes: 1})
	h.ok(jc(retireTrigger("tenant", "ingest", 1)))
	require.Equal(t, JobConfigurationUsage{TenantID: "tenant", Groups: 1, Definitions: 1, Bindings: 1, PayloadBytes: 5}, h.configurationUsage("tenant"))
	h.configurationLimits(JobConfigurationLimits{TenantID: "tenant", MaxTriggers: 1, MaxPayloadBytes: 10})
	d.ID = "replacement"
	h.ok(jc(JobCommand{PutTrigger: &PutJobTrigger{Definition: d}}))
	require.Equal(t, uint64(10), h.configurationUsage("tenant").PayloadBytes)
	require.Equal(t, uint64(1), h.configurationUsage("tenant").Triggers)
}

func TestJobTriggerRetirementKeepsEventDedupThroughPurgeAndReceiptCleanup(t *testing.T) {
	h := newChannelHarness(t)
	run, event := h.finishedEventRun()
	command := jc(retireTrigger("tenant", "ingest", 1))
	command.RequestID, command.At = "retire-ingest", h.at
	retired := h.ok(command)
	duplicate := h.ok(jc(*event.Job)).Job.Activation
	require.Equal(t, run.ID, duplicate.RunID)
	page, err := h.db.ListJobRecords(JobRecordQuery{Kind: JobActivationRecord, TenantID: "tenant", TriggerID: "ingest"}, nil, 1)
	require.NoError(t, err)
	require.Len(t, page.Entries, 1, "retiring a trigger must preserve inspection of its accepted events")
	h.reject(jc(activation("tenant", "ingest", "meters", "new", h.at)), "not_found")
	changed := *event.Job.Activate
	changed.Data = []byte("different")
	h.reject(jc(JobCommand{Activate: &changed}), "activation_conflict")
	h.ok(jc(purge("tenant", run.ID, run.Step, h.at, MaxJobPurgeBatch)))
	p := h.ok(jc(compactReceipts("tenant", run.ID, run.Step, MaxJobPurgeBatch))).Job.ReceiptCompaction
	require.True(t, p.Complete)
	finishConfigurationSweep(h, h.ok(jc(configurationSweep("tenant", "config", 0, 128))).Job.ConfigurationReceiptCompaction)
	require.Equal(t, retired, h.ok(command), "the payload-free retirement audit keeps its receipt")
	duplicate = h.ok(jc(*event.Job)).Job.Activation
	require.Equal(t, run.ID, duplicate.RunID)
	require.True(t, duplicate.DataDeleted)
	require.Empty(t, duplicate.Data)
	h.reject(jc(JobCommand{Activate: &changed}), "activation_conflict")
	h.reject(jc(retireTrigger("tenant", "ingest", 1)), "gone")
	cloned := retired.Clone()
	cloned.Job.TriggerRetirement.Reason = "changed"
	require.NotEqual(t, cloned.Job.TriggerRetirement.Reason, retired.Job.TriggerRetirement.Reason)
}

func TestJobTriggerRetirementSurvivesSnapshotReopenAndProtectsUnknownReservation(t *testing.T) {
	h := newChannelHarness(t)
	tr := h.trigger("tenant", "poll", 2, "bus")
	h.ok(jc(triggerPlan(t, *tr, h.at)))
	id := JobOccurrenceRunID("tenant", "poll", tr.Revision, h.at)
	owner := h.claimJob("tenant", id, "attempt")
	h.at = h.at.Add(time.Minute)
	h.ok(jc(JobCommand{Expire: &ExpireJob{JobOwner: owner, LeaseUntil: h.at}}))
	retired := h.ok(jc(retireTrigger("tenant", "poll", 1))).Job.TriggerRetirement
	var snapshot bytes.Buffer
	require.NoError(t, h.db.Snapshot(&snapshot))
	path := filepath.Join(t.TempDir(), "restored.db")
	q, err := NewDB(path)
	require.NoError(t, err)
	require.NoError(t, q.RestoreAt(&snapshot, h.index))
	require.NoError(t, q.Close())
	q, err = NewDB(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close() })
	h.db = q
	got, err := q.GetJobTriggerRetirement("tenant", "poll")
	require.NoError(t, err)
	require.Equal(t, retired, got)
	page, err := q.ListJobRecords(JobRecordQuery{Kind: JobTriggerRetirementRecord, TenantID: "tenant"}, nil, 1)
	require.NoError(t, err)
	require.Len(t, page.Entries, 1)
	require.Equal(t, retired, page.Entries[0].Record.TriggerRetirement)
	_, err = q.GetJobTriggerRetirement("other", "poll")
	require.Error(t, err)
	_, err = q.ListJobRecords(JobRecordQuery{Kind: JobTriggerRetirementRecord, TenantID: "other"}, page.Entries[0].Cursor, 1)
	require.ErrorContains(t, err, "another namespace")
	resource, err := q.GetJobResource("bus")
	require.NoError(t, err)
	require.Equal(t, ResourceBlocked, resource.State)
	all, own, err := q.ScheduledJobPendingCounts("tenant", "poll")
	require.NoError(t, err)
	require.Equal(t, 1, all)
	require.Equal(t, 1, own)
	h.ok(jc(JobCommand{Resolve: &ResolveJob{TenantID: "tenant", RunID: id, Status: JobFailed, Actor: "operator", Reason: "confirmed result"}}))
	all, own, err = q.ScheduledJobPendingCounts("tenant", "poll")
	require.NoError(t, err)
	require.Zero(t, all)
	require.Zero(t, own)
	resource, err = q.GetJobResource("bus")
	require.NoError(t, err)
	require.Equal(t, ResourceBlocked, resource.State)
	record, err := q.GetJobRecord(JobRecordQuery{Kind: JobTriggerRetirementRecord, TenantID: "tenant", ID: "poll"})
	require.NoError(t, err)
	require.Equal(t, retired, record.TriggerRetirement)
}

func TestJobTriggerRetirementValidatesAuditAndGuardsActivatedSchema(t *testing.T) {
	h := newChannelHarness(t)
	command := jc(retireTrigger("missing", "absent", 1))
	command.RequestID, command.At = "absent-retirement", h.at
	require.Equal(t, JobTriggerRetirementCommandVersion, command.CommandVersion())
	// A downgraded envelope must not advance the index or activate the format.
	result := h.db.ApplyEntries([]commit.Entry{logEntry(t, 1, Command{Version: JobConfigurationReceiptCommandVersion, Type: CmdState, State: &command})})
	require.ErrorContains(t, result.Error, "requires version 16")
	require.Zero(t, h.db.LastAppliedIndex())
	h.reject(command, "not_found")
	tr := h.trigger("tenant", "poll", 2)
	for _, mutate := range []func(*RetireJobTrigger){
		func(c *RetireJobTrigger) { c.ExpectedRevision = 0 },
		func(c *RetireJobTrigger) { c.Actor = "" },
		func(c *RetireJobTrigger) { c.Reason = "" },
	} {
		c := retireTrigger("tenant", "poll", tr.Revision)
		mutate(c.RetireTrigger)
		h.reject(jc(c), "invalid")
	}
	require.NoError(t, h.db.db.View(func(tx *bbolt.Tx) error {
		require.Equal(t, []byte{10}, tx.Bucket(metadataBucket).Get(formatKey), "even rejected v16 requests guard against older writers")
		_, _, err := validateCompactSchema(tx)
		return err
	}))
	for _, corruption := range []struct {
		name string
		edit func(*bbolt.Tx) error
		want string
	}{
		{"marker", func(tx *bbolt.Tx) error { return tx.Bucket(metadataBucket).Delete(jobTriggerRetirementVersionKey) }, "missing its format marker"},
		{"version", func(tx *bbolt.Tx) error {
			return tx.Bucket(metadataBucket).Put(jobTriggerRetirementVersionKey, []byte{2})
		}, "unsupported"},
		{"bucket", func(tx *bbolt.Tx) error { return tx.DeleteBucket(jobTriggerRetirementsBucket) }, "missing its bucket"},
	} {
		t.Run(corruption.name, func(t *testing.T) {
			// Roll back each corruption after checking both open and offline validation.
			err := h.db.db.Update(func(tx *bbolt.Tx) error {
				require.NoError(t, corruption.edit(tx))
				require.ErrorContains(t, initializeJobTriggerRetirementState(tx), corruption.want)
				_, _, err := validateCompactSchema(tx)
				require.Error(t, err)
				return bbolt.ErrTxNotWritable
			})
			require.ErrorIs(t, err, bbolt.ErrTxNotWritable)
		})
	}
}
