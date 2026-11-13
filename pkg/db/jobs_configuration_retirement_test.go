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

func retireConfiguration(tenant string, kind JobRecordKind, id string, revision uint64) JobCommand {
	return JobCommand{RetireConfiguration: &RetireJobConfiguration{TenantID: tenant, Kind: kind, ID: id, ExpectedRevision: revision, Actor: "operator", Reason: "obsolete configuration"}}
}

func TestJobConfigurationRetirementReleasesQuotaWithoutDiscardingPinnedTemplates(t *testing.T) {
	h := newChannelHarness(t)
	h.job("tenant", "same", "same", "same")
	h.job("other", "same", "same", "same")
	d := JobDefinition{TenantID: "tenant", ID: "same", GroupID: "same", BindingID: "same", Handler: "test", HandlerVersion: "1", Input: []byte("abc")}
	b := JobBinding{TenantID: "tenant", ID: "same", Driver: "test", DriverVersion: "1", Config: []byte("xy")}
	h.ok(jc(JobCommand{PutDefinition: &PutJobDefinition{ExpectedRevision: 1, Definition: d}}))
	h.ok(jc(JobCommand{PutBinding: &PutJobBinding{ExpectedRevision: 1, Binding: b}}))
	h.ok(jc(JobCommand{PutTrigger: &PutJobTrigger{Definition: JobTriggerDefinition{TenantID: "tenant", ID: "ingest", DefinitionID: "same", DefinitionRevision: 2, MaxPending: 2, Event: &EventTriggerSpec{Source: "meters"}}}}))
	accepted := h.ok(jc(activation("tenant", "ingest", "meters", "first", h.at))).Job.Activation
	h.configurationLimits(JobConfigurationLimits{TenantID: "tenant", MaxGroups: 1, MaxDefinitions: 1, MaxBindings: 1, MaxTriggers: 1, MaxPayloadBytes: 1})
	before := h.configurationUsage("tenant")
	require.Equal(t, uint64(10), before.PayloadBytes)
	h.reject(jc(retireConfiguration("tenant", JobGroupRecord, "same", 1)), "configuration_in_use")
	h.reject(jc(retireConfiguration("tenant", JobBindingRecord, "same", 2)), "configuration_in_use")
	h.reject(jc(retireConfiguration("tenant", JobDefinitionRecord, "same", 1)), "revision_conflict")
	require.Equal(t, before, h.configurationUsage("tenant"))
	for _, tc := range []struct {
		kind     JobRecordKind
		audit    JobRecordKind
		revision uint64
	}{{JobDefinitionRecord, JobDefinitionRetirementRecord, 2}, {JobBindingRecord, JobBindingRetirementRecord, 2}, {JobGroupRecord, JobGroupRetirementRecord, 1}} {
		c := jc(retireConfiguration("tenant", tc.kind, "same", tc.revision))
		c.RequestID, c.At = "retire-"+string(tc.kind), h.at
		retired := h.ok(c)
		require.Equal(t, tc.kind, retired.Job.ConfigurationRetirement.Kind)
		require.Equal(t, tc.revision, retired.Job.ConfigurationRetirement.Revision)
		require.Equal(t, h.at, retired.Job.ConfigurationRetirement.RetiredAt)
		require.Equal(t, retired, h.ok(c))
		h.reject(jc(*c.Job), "gone")
		_, err := h.db.GetJobRecord(JobRecordQuery{Kind: tc.kind, TenantID: "tenant", ID: "same"})
		require.Error(t, err)
		audit, err := h.db.GetJobRecord(JobRecordQuery{Kind: tc.audit, TenantID: "tenant", ID: "same"})
		require.NoError(t, err)
		require.Equal(t, retired.Job.ConfigurationRetirement, audit.ConfigurationRetirement)
		page, err := h.db.ListJobRecords(JobRecordQuery{Kind: tc.audit, TenantID: "tenant"}, nil, 1)
		require.NoError(t, err)
		require.Len(t, page.Entries, 1)
		require.Equal(t, tc.kind, page.Entries[0].Record.ConfigurationRetirement.Kind)
		_, err = h.db.ListJobRecords(JobRecordQuery{Kind: tc.audit, TenantID: "other"}, page.Entries[0].Cursor, 1)
		require.ErrorContains(t, err, "another namespace")
	}
	require.Equal(t, JobConfigurationUsage{TenantID: "tenant", Triggers: 1, PayloadBytes: 5}, h.configurationUsage("tenant"))
	h.reject(jc(JobCommand{PutGroup: &PutJobGroup{Group: JobGroup{TenantID: "tenant", ID: "same", Runtime: StepRuntime}}}), "gone")
	h.reject(jc(JobCommand{PutDefinition: &PutJobDefinition{Definition: d}}), "gone")
	h.reject(jc(JobCommand{PutBinding: &PutJobBinding{Binding: b}}), "gone")
	h.reject(jc(JobCommand{Launch: &LaunchJob{TenantID: "tenant", RunID: "new", DefinitionID: "same", DefinitionRevision: 2}}), "not_found")
	run, err := h.db.GetJobRun("tenant", accepted.RunID)
	require.NoError(t, err)
	require.Equal(t, "abc", string(run.Definition.Input))
	require.Equal(t, "xy", string(run.Binding.Config))
	// The already pinned event trigger remains independently active.
	next := h.ok(jc(activation("tenant", "ingest", "meters", "second", h.at))).Job.Activation
	require.NotEqual(t, accepted.RunID, next.RunID)
	h.ok(jc(retireTrigger("tenant", "ingest", 1)))
	require.Equal(t, JobConfigurationUsage{TenantID: "tenant"}, h.configurationUsage("tenant"))
	h.job("tenant", "replacement", "replacement", "replacement")
	require.Equal(t, uint64(1), h.configurationUsage("tenant").Groups)
	require.Equal(t, uint64(1), h.configurationUsage("other").Groups)
}

func TestJobConfigurationRetirementKeepsGatewayWaitAndFrozenGroupQuotaAcrossRestore(t *testing.T) {
	h := newChannelHarness(t)
	h.ok(StateCommand{Configure: &ConfigureChannel{ID: "bus", GatewayID: "bus", Protocol: "fake", Endpoint: "localhost:1"}})
	h.ok(jc(JobCommand{PutResource: &PutJobResource{Spec: ResourceSpec{ID: "bus", ChannelID: "bus", Duplex: "half", Capacity: 1}}}))
	h.job("tenant", "group", "work", "driver", "bus")
	h.eventTrigger("tenant", "ingest", "meters", 4)
	first := h.ok(jc(activation("tenant", "ingest", "meters", "first", h.at))).Job.Activation
	second := h.ok(jc(activation("tenant", "ingest", "meters", "second", h.at))).Job.Activation
	owner := h.claimJob("tenant", first.RunID, "attempt")
	wait := h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, Kind: JobWait, WaitKey: "reply", State: []byte("session")}})).Job.Run
	// Pinned groups have an unlimited old snapshot. Admission must retain the
	// last live policy when retirement removes the current group record.
	h.ok(jc(JobCommand{PutGroup: &PutJobGroup{ExpectedRevision: 1, Group: JobGroup{TenantID: "tenant", ID: "group", Runtime: StepRuntime, MaxRunning: 1}}}))
	h.ok(jc(retireConfiguration("tenant", JobDefinitionRecord, "work", 1)))
	h.ok(jc(retireConfiguration("tenant", JobBindingRecord, "driver", 1)))
	retired := h.ok(jc(retireConfiguration("tenant", JobGroupRecord, "group", 2))).Job.ConfigurationRetirement
	require.Equal(t, uint64(1), retired.GroupMaxRunning)
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
	live, err := q.GetJobRun("tenant", first.RunID)
	require.NoError(t, err)
	require.Equal(t, wait, live)
	resource, err := q.GetJobResource("bus")
	require.NoError(t, err)
	require.Equal(t, ResourceOwned, resource.State)
	contender := JobOwner{TenantID: "tenant", RunID: second.RunID, AttemptID: "contender", OwnerID: "worker/contender"}
	h.reject(jc(JobCommand{Claim: &ClaimJob{JobOwner: contender, LeaseUntil: h.at.Add(time.Minute)}}), "quota_exceeded")
	h.ok(jc(JobCommand{Signal: &SignalJob{TenantID: "tenant", RunID: first.RunID, EventID: "reply", Key: "reply"}}))
	h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, ExpectedStep: 1, EventID: "reply", Kind: JobComplete, Status: JobSucceeded, Quiescent: true}}))
	owner = h.claimJob("tenant", second.RunID, "second")
	h.at = h.at.Add(time.Minute)
	h.ok(jc(JobCommand{Expire: &ExpireJob{JobOwner: owner, LeaseUntil: h.at}}))
	resource, err = q.GetJobResource("bus")
	require.NoError(t, err)
	require.Equal(t, ResourceBlocked, resource.State)
	h.ok(jc(JobCommand{Resolve: &ResolveJob{TenantID: "tenant", RunID: second.RunID, Status: JobFailed, Actor: "operator", Reason: "confirmed result"}}))
	resource, err = q.GetJobResource("bus")
	require.NoError(t, err)
	require.Equal(t, ResourceBlocked, resource.State)
	audit, err := q.GetJobConfigurationRetirement("tenant", JobGroupRecord, "group")
	require.NoError(t, err)
	require.Equal(t, retired, audit)
}

func TestJobConfigurationReferenceReplacementsAndRejectionsAreAtomic(t *testing.T) {
	h := newChannelHarness(t)
	h.job("tenant", "old-group", "work", "old-binding")
	h.ok(jc(JobCommand{PutGroup: &PutJobGroup{Group: JobGroup{TenantID: "tenant", ID: "new-group", Runtime: StepRuntime}}}))
	h.ok(jc(JobCommand{PutBinding: &PutJobBinding{Binding: JobBinding{TenantID: "tenant", ID: "new-binding", Driver: "test", DriverVersion: "1"}}}))
	d := JobDefinition{TenantID: "tenant", ID: "work", GroupID: "new-group", BindingID: "new-binding", Handler: "test", HandlerVersion: "1"}
	h.ok(jc(JobCommand{PutDefinition: &PutJobDefinition{ExpectedRevision: 1, Definition: d}}))
	h.ok(jc(retireConfiguration("tenant", JobGroupRecord, "old-group", 1)))
	h.ok(jc(retireConfiguration("tenant", JobBindingRecord, "old-binding", 1)))
	replacement := d
	replacement.GroupID, replacement.BindingID, replacement.Runtime = "", "", StepRuntime
	h.reject(jc(JobCommand{PutDefinition: &PutJobDefinition{ExpectedRevision: 1, Definition: replacement}}), "revision_conflict")
	h.reject(jc(retireConfiguration("tenant", JobGroupRecord, "new-group", 1)), "configuration_in_use")
	h.reject(jc(retireConfiguration("tenant", JobBindingRecord, "new-binding", 1)), "configuration_in_use")
	h.configurationLimits(JobConfigurationLimits{TenantID: "tenant", MaxPayloadBytes: 1})
	replacement.Input = []byte("too much")
	h.reject(jc(JobCommand{PutDefinition: &PutJobDefinition{ExpectedRevision: 2, Definition: replacement}}), "quota_exceeded")
	h.reject(jc(retireConfiguration("tenant", JobGroupRecord, "new-group", 1)), "configuration_in_use")
	replacement.Input = nil
	h.ok(jc(JobCommand{PutDefinition: &PutJobDefinition{ExpectedRevision: 2, Definition: replacement}}))
	h.ok(jc(retireConfiguration("tenant", JobGroupRecord, "new-group", 1)))
	h.ok(jc(retireConfiguration("tenant", JobBindingRecord, "new-binding", 1)))
	cloned := h.ok(jc(retireConfiguration("tenant", JobDefinitionRecord, "work", 3))).Clone()
	cloned.Job.ConfigurationRetirement.Kind = JobBindingRecord
	audit, err := h.db.GetJobConfigurationRetirement("tenant", JobDefinitionRecord, "work")
	require.NoError(t, err)
	require.Equal(t, JobDefinitionRecord, audit.Kind)
}

func TestJobConfigurationRetirementReferenceMigrationAndSchemaGuard(t *testing.T) {
	h := newChannelHarness(t)
	h.job("tenant", "group", "work", "driver")
	// Simulate a stale derived index left by a pre-v17 writer.
	require.NoError(t, h.db.db.Update(func(tx *bbolt.Tx) error {
		if err := tx.DeleteBucket(jobConfigurationReferencesBucket); err != nil {
			return err
		}
		_, err := tx.CreateBucket(jobConfigurationReferencesBucket)
		return err
	}))
	var old bytes.Buffer
	require.NoError(t, h.db.Snapshot(&old))
	q, err := NewDB(filepath.Join(t.TempDir(), "migrated.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close() })
	require.NoError(t, q.RestoreAt(&old, h.index))
	h.db = q
	require.NoError(t, q.db.View(func(tx *bbolt.Tx) error {
		require.Equal(t, []byte{2}, tx.Bucket(metadataBucket).Get(formatKey), "restoring old data alone does not activate the new format")
		return nil
	}))
	command := jc(retireConfiguration("tenant", JobGroupRecord, "group", 1))
	command.RequestID, command.At = "in-use", h.at
	bad := q.ApplyEntries([]commit.Entry{logEntry(t, h.index+1, Command{Version: JobTriggerRetirementCommandVersion, Type: CmdState, State: &command})})
	require.ErrorContains(t, bad.Error, "requires version 17")
	require.Equal(t, h.index, q.LastAppliedIndex())
	h.reject(command, "configuration_in_use")
	var activated bytes.Buffer
	require.NoError(t, q.Snapshot(&activated))
	q2, err := NewDB(filepath.Join(t.TempDir(), "guarded.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = q2.Close() })
	require.NoError(t, q2.RestoreAt(&activated, h.index))
	h.db = q2
	h.reject(jc(retireConfiguration("tenant", JobBindingRecord, "driver", 1)), "configuration_in_use")
	require.NoError(t, q2.db.View(func(tx *bbolt.Tx) error {
		require.Equal(t, []byte{11}, tx.Bucket(metadataBucket).Get(formatKey))
		_, _, err := validateCompactSchema(tx)
		return err
	}))
	for _, name := range jobConfigurationRetirementBuckets {
		err := q2.db.Update(func(tx *bbolt.Tx) error {
			require.NoError(t, tx.DeleteBucket(name))
			require.ErrorContains(t, initializeJobConfigurationRetirementState(tx), "missing bucket")
			_, _, err := validateCompactSchema(tx)
			require.Error(t, err)
			return bbolt.ErrTxNotWritable
		})
		require.ErrorIs(t, err, bbolt.ErrTxNotWritable)
	}
	require.NoError(t, q2.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(metadataBucket).Delete(jobConfigurationRetirementVersionKey)
	}))
	require.ErrorContains(t, q2.db.Update(initializeJobConfigurationRetirementState), "missing its format marker")
}

func TestJobConfigurationRetirementValidationAndReceiptLifetime(t *testing.T) {
	h := newChannelHarness(t)
	h.job("tenant", "group", "work", "driver")
	for _, kind := range []JobRecordKind{JobTenantRecord, JobTriggerRecord, JobResourceRecord, ""} {
		h.reject(jc(retireConfiguration("tenant", kind, "work", 1)), "invalid")
	}
	for _, edit := range []func(*RetireJobConfiguration){
		func(c *RetireJobConfiguration) { c.ExpectedRevision = 0 },
		func(c *RetireJobConfiguration) { c.Reason = "" },
		func(c *RetireJobConfiguration) { c.Actor = "" },
	} {
		c := retireConfiguration("tenant", JobDefinitionRecord, "work", 1)
		edit(c.RetireConfiguration)
		h.reject(jc(c), "invalid")
	}
	c := jc(retireConfiguration("tenant", JobDefinitionRecord, "work", 1))
	c.RequestID, c.At = "retire-definition", h.at
	retired := h.ok(c)
	require.Equal(t, JobConfigurationRetirementCommandVersion, c.CommandVersion())
	finishConfigurationSweep(h, h.ok(jc(configurationSweep("tenant", "audit", 0, 128))).Job.ConfigurationReceiptCompaction)
	require.Equal(t, retired, h.ok(c))
	changed := c
	changed.Job = &JobCommand{RetireConfiguration: jobCopy(c.Job.RetireConfiguration)}
	changed.Job.RetireConfiguration.Reason = "different reason"
	h.reject(changed, "request_conflict")
}
