package db

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func (h *channelHarness) configurationLimits(v JobConfigurationLimits) *JobConfigurationLimits {
	h.t.Helper()
	old, err := h.db.GetJobConfigurationLimits(v.TenantID)
	require.NoError(h.t, err)
	return h.ok(jc(JobCommand{PutConfigurationLimits: &PutJobConfigurationLimits{Limits: v, ExpectedRevision: old.Revision}})).Job.ConfigurationLimits
}
func (h *channelHarness) configurationUsage(tenant string) JobConfigurationUsage {
	h.t.Helper()
	v, err := h.db.GetJobConfigurationUsage(tenant)
	require.NoError(h.t, err)
	return *v
}

func TestJobConfigurationLimitsCountsAreAtomicTenantScopedAndReplaceable(t *testing.T) {
	cases := []struct {
		name    string
		limit   JobConfigurationLimits
		command func(id string, expected uint64) JobCommand
		kind    JobRecordKind
	}{
		{"groups", JobConfigurationLimits{MaxGroups: 1}, func(id string, expected uint64) JobCommand {
			return JobCommand{PutGroup: &PutJobGroup{ExpectedRevision: expected, Group: JobGroup{TenantID: "tenant", ID: id, Runtime: StepRuntime}}}
		}, JobGroupRecord},
		{"definitions", JobConfigurationLimits{MaxDefinitions: 1}, func(id string, expected uint64) JobCommand {
			return JobCommand{PutDefinition: &PutJobDefinition{ExpectedRevision: expected, Definition: JobDefinition{TenantID: "tenant", ID: id, Runtime: StepRuntime, Handler: "test", HandlerVersion: "1"}}}
		}, JobDefinitionRecord},
		{"bindings", JobConfigurationLimits{MaxBindings: 1}, func(id string, expected uint64) JobCommand {
			return JobCommand{PutBinding: &PutJobBinding{ExpectedRevision: expected, Binding: JobBinding{TenantID: "tenant", ID: id, Driver: "test", DriverVersion: "1"}}}
		}, JobBindingRecord},
		{"triggers", JobConfigurationLimits{MaxTriggers: 1}, func(id string, expected uint64) JobCommand {
			return JobCommand{PutTrigger: &PutJobTrigger{ExpectedRevision: expected, Definition: JobTriggerDefinition{TenantID: "tenant", ID: id, DefinitionID: "work", DefinitionRevision: 1, MaxPending: 2, Event: &EventTriggerSpec{Source: "meters"}}}}
		}, JobTriggerRecord},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newChannelHarness(t)
			// All four primary records use the same ID in separate namespaces.
			h.job("tenant", "work", "work", "work")
			h.eventTrigger("tenant", "work", "meters", 2)
			tc.limit.TenantID = "tenant"
			h.configurationLimits(tc.limit)
			before := h.configurationUsage("tenant")
			require.Equal(t, JobConfigurationUsage{TenantID: "tenant", Groups: 1, Definitions: 1, Bindings: 1, Triggers: 1}, before)
			denied := jc(tc.command("extra", 0))
			denied.RequestID = "denied"
			denied.At = h.at
			h.reject(denied, "quota_exceeded")
			_, err := h.db.GetJobRecord(JobRecordQuery{Kind: tc.kind, TenantID: "tenant", ID: "extra"})
			require.Error(t, err)
			require.Equal(t, before, h.configurationUsage("tenant"))
			h.ok(jc(tc.command("work", 1)))
			require.Equal(t, before, h.configurationUsage("tenant"), "CAS replacement does not consume another slot")
			h.job("other", "work", "work", "work")
			h.eventTrigger("other", "work", "meters", 2)
			h.configurationLimits(JobConfigurationLimits{TenantID: "tenant"})
			h.reject(denied, "quota_exceeded")
			denied.RequestID = "fresh"
			h.ok(denied)
			require.Equal(t, JobConfigurationUsage{TenantID: "other", Groups: 1, Definitions: 1, Bindings: 1, Triggers: 1}, h.configurationUsage("other"))
		})
	}
}

func TestJobConfigurationPayloadIncludesIndependentPinnedTemplates(t *testing.T) {
	h := newChannelHarness(t)
	h.job("tenant", "group", "work", "driver")
	definition := JobDefinition{TenantID: "tenant", ID: "work", GroupID: "group", Handler: "test", HandlerVersion: "1", BindingID: "driver", Input: []byte{0, 255, 1, 2}}
	binding := JobBinding{TenantID: "tenant", ID: "driver", Driver: "test", DriverVersion: "1", Config: []byte{255, 0}}
	h.ok(jc(JobCommand{PutDefinition: &PutJobDefinition{ExpectedRevision: 1, Definition: definition}}))
	h.ok(jc(JobCommand{PutBinding: &PutJobBinding{ExpectedRevision: 1, Binding: binding}}))
	h.configurationLimits(JobConfigurationLimits{TenantID: "tenant", MaxPayloadBytes: 11})
	trigger := JobTriggerDefinition{TenantID: "tenant", ID: "ingest", DefinitionID: "work", DefinitionRevision: 2, MaxPending: 2, Event: &EventTriggerSpec{Source: "meters"}}
	c := jc(JobCommand{PutTrigger: &PutJobTrigger{Definition: trigger}})
	require.Equal(t, uint64(6), h.configurationUsage("tenant").PayloadBytes)
	h.reject(c, "quota_exceeded")
	require.Zero(t, h.configurationUsage("tenant").Triggers)
	h.configurationLimits(JobConfigurationLimits{TenantID: "tenant", MaxPayloadBytes: 12})
	h.ok(c)
	require.Equal(t, uint64(12), h.configurationUsage("tenant").PayloadBytes)
	definition.Input = []byte("a")
	binding.Config = nil
	h.ok(jc(JobCommand{PutDefinition: &PutJobDefinition{ExpectedRevision: 2, Definition: definition}}))
	h.ok(jc(JobCommand{PutBinding: &PutJobBinding{ExpectedRevision: 2, Binding: binding}}))
	require.Equal(t, uint64(7), h.configurationUsage("tenant").PayloadBytes)
	pinned, err := h.db.GetJobTrigger("tenant", "ingest")
	require.NoError(t, err)
	require.Equal(t, []byte{0, 255, 1, 2}, pinned.Template.Definition.Input)
	require.Equal(t, []byte{255, 0}, pinned.Template.Binding.Config)
	// Repinning replaces the old template and releases its logical payload usage.
	trigger.DefinitionRevision = 3
	h.ok(jc(JobCommand{PutTrigger: &PutJobTrigger{ExpectedRevision: 1, Definition: trigger}}))
	require.Equal(t, uint64(2), h.configurationUsage("tenant").PayloadBytes)
	h.configurationLimits(JobConfigurationLimits{TenantID: "tenant", MaxPayloadBytes: 1})
	// Equal or smaller writes remain available after a cap is lowered.
	h.ok(jc(JobCommand{PutDefinition: &PutJobDefinition{ExpectedRevision: 3, Definition: definition}}))
	definition.Input = []byte("ab")
	h.reject(jc(JobCommand{PutDefinition: &PutJobDefinition{ExpectedRevision: 4, Definition: definition}}), "quota_exceeded")
	require.Equal(t, uint64(2), h.configurationUsage("tenant").PayloadBytes)
}

func TestJobConfigurationLimitsAllowCalendarProgressCallbacksAndGatewayRelease(t *testing.T) {
	h := newChannelHarness(t)
	h.ok(StateCommand{Configure: &ConfigureChannel{ID: "bus", GatewayID: "bus", Protocol: "fake", Endpoint: "localhost:1"}})
	h.ok(jc(JobCommand{PutResource: &PutJobResource{Spec: ResourceSpec{ID: "bus", ChannelID: "bus", Duplex: "half", Capacity: 1}}}))
	tr := h.trigger("tenant", "poll", 2, "bus")
	h.ok(jc(JobCommand{PutDefinition: &PutJobDefinition{ExpectedRevision: 1, Definition: JobDefinition{TenantID: "tenant", ID: "work", GroupID: "group", Handler: "test", HandlerVersion: "1", BindingID: "driver", Input: []byte("payload")}}}))
	// Exceed the lowered byte cap before materializing a pinned calendar job.
	h.configurationLimits(JobConfigurationLimits{TenantID: "tenant", MaxPayloadBytes: 1, MaxGroups: 1, MaxDefinitions: 1, MaxBindings: 1, MaxTriggers: 1})
	before := h.configurationUsage("tenant")
	h.ok(jc(triggerPlan(t, *tr, h.at)))
	id := JobOccurrenceRunID("tenant", "poll", tr.Revision, tr.NextAt)
	owner := h.claimJob("tenant", id, "first")
	h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, Kind: JobWait, WaitKey: "reply"}}))
	h.ok(jc(JobCommand{Signal: &SignalJob{TenantID: "tenant", RunID: id, EventID: "reply", Key: "reply", Data: []byte("event")}}))
	h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, ExpectedStep: 1, EventID: "reply", Kind: JobComplete, Status: JobSucceeded, Quiescent: true}}))
	h.ok(jc(JobCommand{PauseTrigger: &PauseJobTrigger{TenantID: "tenant", ID: "poll", ExpectedRevision: 1, Paused: true}}))
	require.Equal(t, before, h.configurationUsage("tenant"))
	resource, err := h.db.GetJobResource("bus")
	require.NoError(t, err)
	require.Equal(t, ResourceAvailable, resource.State)
	channel, err := h.db.GetChannel("bus")
	require.NoError(t, err)
	require.Equal(t, ChannelAvailable, channel.State)
}

func TestJobConfigurationUsageRebuildsOldWritersAndSurvivesActivatedSnapshots(t *testing.T) {
	h := newChannelHarness(t)
	h.job("tenant", "group", "work", "driver")
	h.eventTrigger("tenant", "ingest", "meters", 2)
	expected := h.configurationUsage("tenant")
	require.NoError(t, h.db.db.Update(func(tx *bbolt.Tx) error {
		return writeRecord(tx, jobConfigurationUsageBucket, "tenant", &JobConfigurationUsage{TenantID: "tenant"})
	}))
	// Pre-v14 writers can leave the derived counters stale on a format-4 file.
	var legacy bytes.Buffer
	require.NoError(t, h.db.Snapshot(&legacy))
	q, err := NewDB(filepath.Join(t.TempDir(), "restored.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close() })
	require.NoError(t, q.RestoreAt(&legacy, h.index))
	h.db = q
	require.Equal(t, expected, h.configurationUsage("tenant"))
	h.configurationLimits(JobConfigurationLimits{TenantID: "tenant", MaxTriggers: 1})
	var activated bytes.Buffer
	require.NoError(t, q.Snapshot(&activated))
	restored, err := NewDB(filepath.Join(t.TempDir(), "guarded.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = restored.Close() })
	require.NoError(t, restored.RestoreAt(&activated, h.index))
	h.db = restored
	require.Equal(t, expected, h.configurationUsage("tenant"))
	limits, err := restored.GetJobConfigurationLimits("tenant")
	require.NoError(t, err)
	require.Equal(t, uint64(1), limits.MaxTriggers)
	require.NoError(t, restored.db.View(func(tx *bbolt.Tx) error {
		require.Equal(t, []byte{8}, tx.Bucket(metadataBucket).Get(formatKey))
		return nil
	}))
	require.NoError(t, restored.db.Update(func(tx *bbolt.Tx) error { return tx.DeleteBucket(jobConfigurationUsageBucket) }))
	require.ErrorContains(t, restored.db.Update(initializeJobConfigurationState), "missing bucket")
}

func TestJobConfigurationLimitsCASDefaultsRecordsAndRejectedFormatGuard(t *testing.T) {
	h := newChannelHarness(t)
	c := jc(JobCommand{PutConfigurationLimits: &PutJobConfigurationLimits{Limits: JobConfigurationLimits{TenantID: "absent"}}})
	require.Equal(t, JobConfigurationCommandVersion, c.CommandVersion())
	h.reject(c, "not_found")
	h.job("tenant", "", "work", "")
	defaults, err := h.db.GetJobRecord(JobRecordQuery{Kind: JobConfigurationLimitsRecord, TenantID: "tenant"})
	require.NoError(t, err)
	require.Equal(t, &JobConfigurationLimits{TenantID: "tenant"}, defaults.ConfigurationLimits)
	applied := h.configurationLimits(JobConfigurationLimits{TenantID: "tenant", MaxDefinitions: 1})
	clone := (StateResult{Job: &JobResult{ConfigurationLimits: applied}}).Clone()
	clone.Job.ConfigurationLimits.MaxDefinitions = 99
	require.Equal(t, uint64(1), applied.MaxDefinitions)
	h.reject(jc(JobCommand{PutConfigurationLimits: &PutJobConfigurationLimits{Limits: JobConfigurationLimits{TenantID: "tenant", MaxDefinitions: 2}}}), "revision_conflict")
	record, err := h.db.GetJobRecord(JobRecordQuery{Kind: JobConfigurationLimitsRecord, TenantID: "tenant"})
	require.NoError(t, err)
	require.Equal(t, applied, record.ConfigurationLimits)
	record, err = h.db.GetJobRecord(JobRecordQuery{Kind: JobConfigurationUsageRecord, TenantID: "tenant"})
	require.NoError(t, err)
	require.Equal(t, uint64(1), record.ConfigurationUsage.Definitions)
	for _, kind := range []JobRecordKind{JobConfigurationLimitsRecord, JobConfigurationUsageRecord} {
		_, err = h.db.ListJobRecords(JobRecordQuery{Kind: kind, TenantID: "tenant"}, nil, 1)
		require.Error(t, err)
		_, err = h.db.GetJobRecord(JobRecordQuery{Kind: kind, TenantID: "absent"})
		require.Error(t, err)
	}
	require.NoError(t, h.db.db.View(func(tx *bbolt.Tx) error {
		require.Equal(t, []byte{8}, tx.Bucket(metadataBucket).Get(formatKey))
		return nil
	}))
	// Rejected v14 requests also make older writers unsafe.
	require.NoError(t, h.db.db.Update(func(tx *bbolt.Tx) error { return tx.Bucket(metadataBucket).Delete(jobConfigurationVersionKey) }))
	require.ErrorContains(t, h.db.db.Update(initializeJobConfigurationState), "missing its format marker")
}
