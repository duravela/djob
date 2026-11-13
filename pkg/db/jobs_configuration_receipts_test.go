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

func configurationSweep(tenant, id string, step uint64, batch int) JobCommand {
	return JobCommand{CompactConfigurationReceipts: &CompactJobConfigurationReceipts{TenantID: tenant, ID: id, ExpectedStep: step, BatchSize: batch, Actor: "tenant/operator", Reason: "compact configuration history"}}
}
func finishConfigurationSweep(h *channelHarness, p *JobConfigurationReceiptCompaction) *JobConfigurationReceiptCompaction {
	h.t.Helper()
	for i := 0; !p.Complete && i < 128; i++ {
		p = h.ok(jc(configurationSweep(p.TenantID, p.ID, p.Step, 128))).Job.ConfigurationReceiptCompaction
	}
	require.True(h.t, p.Complete)
	return p
}

func TestConfigurationReceiptCompactionPreservesAcknowledgementsHorizonAndLiveSession(t *testing.T) {
	h := newChannelHarness(t)
	h.ok(StateCommand{Configure: &ConfigureChannel{ID: "bus", GatewayID: "bus", Protocol: "fake", Endpoint: "localhost:1"}})
	global := h.ok(jc(JobCommand{PutResource: &PutJobResource{Spec: ResourceSpec{ID: "bus", ChannelID: "bus", Duplex: "half", Capacity: 1}}}))
	commands := map[string]StateCommand{}
	results := map[string]StateResult{}
	record := func(id string, c JobCommand) StateResult {
		command := jc(c)
		command.RequestID = id
		command.At = h.at
		commands[id] = command
		r := h.ok(command)
		results[id] = r
		return r
	}
	record("tenant", JobCommand{PutTenant: &PutJobTenant{Tenant: JobTenant{ID: "tenant"}}})
	record("grant", JobCommand{Grant: &PutResourceGrant{Grant: ResourceGrant{TenantID: "tenant", ResourceID: "bus", Allowed: true}}})
	record("group", JobCommand{PutGroup: &PutJobGroup{Group: JobGroup{TenantID: "tenant", ID: "group", Runtime: StepRuntime, Resources: []string{"bus"}}}})
	record("binding", JobCommand{PutBinding: &PutJobBinding{Binding: JobBinding{TenantID: "tenant", ID: "driver", Driver: "test", DriverVersion: "1", Config: []byte("xy")}}})
	d := JobDefinition{TenantID: "tenant", ID: "work", GroupID: "group", BindingID: "driver", Handler: "test", HandlerVersion: "1", Input: []byte("abc")}
	record("definition", JobCommand{PutDefinition: &PutJobDefinition{Definition: d}})
	record("limits", JobCommand{PutLimits: &PutJobLimits{Limits: JobLimits{TenantID: "tenant"}}})
	record("configuration-limits", JobCommand{PutConfigurationLimits: &PutJobConfigurationLimits{Limits: JobConfigurationLimits{TenantID: "tenant"}}})
	record("retention", JobCommand{PutRetention: &PutJobRetention{Policy: JobRetentionPolicy{TenantID: "tenant"}}})
	record("event-trigger", JobCommand{PutTrigger: &PutJobTrigger{Definition: JobTriggerDefinition{TenantID: "tenant", ID: "ingest", DefinitionID: "work", DefinitionRevision: 1, MaxPending: 2, Event: &EventTriggerSpec{Source: "meters"}}}})
	record("pause", JobCommand{PauseTrigger: &PauseJobTrigger{TenantID: "tenant", ID: "ingest", ExpectedRevision: 1, Paused: true}})
	tr := record("calendar", JobCommand{PutTrigger: &PutJobTrigger{Definition: JobTriggerDefinition{TenantID: "tenant", ID: "poll", DefinitionID: "work", DefinitionRevision: 1, MaxPending: 2, Timing: ScheduleTiming{Spec: ScheduleSpec{Kind: ScheduleInterval, StartAt: h.at, Interval: time.Second}, MissedPolicy: MissedCoalesce}}, NextAt: h.at}}).Job.Trigger
	record("materialize", triggerPlan(t, *tr, h.at))
	denied := h.reject(jc(JobCommand{PutDefinition: &PutJobDefinition{Definition: d}}), "revision_conflict")
	runID := JobOccurrenceRunID("tenant", "poll", 1, h.at)
	owner := h.claimJob("tenant", runID, "attempt")
	wait := h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, Kind: JobWait, WaitKey: "reply", State: []byte("session")}}))
	first := h.ok(jc(configurationSweep("tenant", "audit", 0, 1))).Job.ConfigurationReceiptCompaction
	require.False(t, first.Complete)
	require.Equal(t, uint64(len(commands)), first.Horizon)
	// A later replacement is outside this committed sweep, even while it runs.
	d.Input = []byte("later")
	later := jc(JobCommand{PutDefinition: &PutJobDefinition{ExpectedRevision: 1, Definition: d}})
	later.RequestID = "later"
	later.At = h.at
	h.ok(later)
	before := h.configurationUsage("tenant")
	last := finishConfigurationSweep(h, first)
	require.Equal(t, uint64(len(commands)), last.ReceiptsCompacted)
	require.Equal(t, uint64(25), last.PayloadBytesRemoved) // 2+3+four pinned copies of 5.
	require.Equal(t, "tenant/operator", last.Actor)
	require.Equal(t, before, h.configurationUsage("tenant"))
	for id, c := range commands {
		replay := h.apply(c)
		require.True(t, replay.Compacted, id)
		require.ErrorContains(t, replay.Err(), ReceiptCompactedCode)
		require.Equal(t, results[id].Fingerprint, replay.Fingerprint)
		require.Nil(t, replay.Rejection)
		if replay.Job.Definition != nil {
			require.Equal(t, uint64(1), replay.Job.Definition.Revision)
			require.Empty(t, replay.Job.Definition.Input)
		}
		if replay.Job.Binding != nil {
			require.Empty(t, replay.Job.Binding.Config)
		}
		if replay.Job.Trigger != nil {
			require.Empty(t, replay.Job.Trigger.Template.Definition.Input)
			require.Nil(t, replay.Job.Trigger.Template.Binding)
			require.True(t, replay.Job.Trigger.NextAt.IsZero())
			require.Equal(t, results[id].Job.Created, replay.Job.Created)
		}
		require.NoError(t, h.db.db.View(func(tx *bbolt.Tx) error {
			require.Nil(t, tx.Bucket(requestsBucket).Get([]byte(id)))
			require.NotNil(t, tx.Bucket(jobCompactReceiptsBucket).Get([]byte(id)))
			return nil
		}))
	}
	require.False(t, h.ok(later).Compacted)
	require.Equal(t, last, h.ok(jc(configurationSweep("tenant", "audit", last.Step, 128))).Job.ConfigurationReceiptCompaction)
	conflict := commands["definition"]
	changed := d
	changed.Input = []byte("different")
	conflict.Job = &JobCommand{PutDefinition: &PutJobDefinition{Definition: changed}}
	h.reject(conflict, "request_conflict")
	require.NoError(t, h.db.db.View(func(tx *bbolt.Tx) error {
		for _, id := range []string{global.RequestID, denied.RequestID, wait.RequestID} {
			require.NotNil(t, tx.Bucket(requestsBucket).Get([]byte(id)), id)
		}
		return nil
	}))
	live, err := h.db.GetJobRun("tenant", runID)
	require.NoError(t, err)
	require.Equal(t, JobWaiting, live.Status)
	require.Equal(t, "session", string(live.State))
	require.Equal(t, owner.OwnerID, live.OwnerID)
	require.Equal(t, "abc", string(live.Definition.Input))
	h.ok(jc(JobCommand{Signal: &SignalJob{TenantID: "tenant", RunID: runID, EventID: "response", Key: "reply", Data: []byte("response")}}))
	h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, ExpectedStep: 1, EventID: "response", Kind: JobComplete, Status: JobSucceeded, Quiescent: true}}))
	channel, err := h.db.GetChannel("bus")
	require.NoError(t, err)
	require.Equal(t, ChannelAvailable, channel.State)
	pending, err := h.db.ListPendingJobConfigurationReceiptCompactions(nil, 1)
	require.NoError(t, err)
	require.Empty(t, pending.Compactions)
	recordResult, err := h.db.GetJobRecord(JobRecordQuery{Kind: JobConfigurationReceiptCompactionRecord, TenantID: "tenant", ID: "audit"})
	require.NoError(t, err)
	require.Equal(t, last, recordResult.ConfigurationReceiptCompaction)
}

func TestConfigurationReceiptCompactionEnforcesLogicalBatchBudget(t *testing.T) {
	h := newChannelHarness(t)
	h.ok(jc(JobCommand{PutTenant: &PutJobTenant{Tenant: JobTenant{ID: "tenant"}}}))
	for i := range 10 {
		h.ok(jc(JobCommand{PutDefinition: &PutJobDefinition{Definition: JobDefinition{TenantID: "tenant", ID: fmt.Sprintf("work-%d", i), Runtime: StepRuntime, Handler: "test", HandlerVersion: "1", Input: bytes.Repeat([]byte{byte(i)}, 1<<20)}}}))
	}
	first := h.ok(jc(configurationSweep("tenant", "budget", 0, 128))).Job.ConfigurationReceiptCompaction
	require.False(t, first.Complete)
	require.Equal(t, uint64(8<<20), first.PayloadBytesRemoved)
	require.Equal(t, uint64(9), first.ReceiptsCompacted)
	last := finishConfigurationSweep(h, first)
	require.Equal(t, uint64(10<<20), last.PayloadBytesRemoved)
	require.Equal(t, uint64(11), last.ReceiptsCompacted)
}

func TestConfigurationReceiptSweepsRemainFiniteConcurrentAndTenantScoped(t *testing.T) {
	h := newChannelHarness(t)
	h.job("tenant", "group", "work", "driver")
	h.job("tenant/other", "group", "work", "driver")
	first := h.ok(jc(configurationSweep("tenant", "one", 0, 1))).Job.ConfigurationReceiptCompaction
	newer := h.ok(jc(JobCommand{PutDefinition: &PutJobDefinition{ExpectedRevision: 1, Definition: JobDefinition{TenantID: "tenant", ID: "work", Runtime: StepRuntime, Handler: "test", HandlerVersion: "1", Input: []byte("new")}}}))
	second := h.ok(jc(configurationSweep("tenant", "two", 0, 128))).Job.ConfigurationReceiptCompaction
	require.True(t, second.Complete)
	require.Equal(t, uint64(4), second.ReceiptsCompacted)
	last := finishConfigurationSweep(h, first)
	require.Equal(t, uint64(1), last.ReceiptsCompacted)
	require.NoError(t, h.db.db.View(func(tx *bbolt.Tx) error {
		require.Equal(t, 4, tx.Bucket(jobConfigurationReceiptIndexBucket).Stats().KeyN, "only the other tenant remains indexed")
		require.Nil(t, tx.Bucket(requestsBucket).Get([]byte(newer.RequestID)))
		return nil
	}))
	h.reject(jc(configurationSweep("tenant", "one", 0, 1)), "step_conflict")
	page, err := h.db.ListJobRecords(JobRecordQuery{Kind: JobConfigurationReceiptCompactionRecord, TenantID: "tenant/other"}, nil, 10)
	require.NoError(t, err)
	require.Empty(t, page.Entries)
}

func TestConfigurationReceiptIndexRebuildAndSnapshotResume(t *testing.T) {
	h := newChannelHarness(t)
	h.job("tenant", "group", "work", "driver")
	h.configurationLimits(JobConfigurationLimits{TenantID: "tenant"})
	// Simulate v14 writes that left the derived index empty in a format-8 file.
	require.NoError(t, h.db.db.Update(func(tx *bbolt.Tx) error {
		for _, b := range [][]byte{jobConfigurationReceiptIndexBucket, jobConfigurationReceiptCountersBucket} {
			if err := tx.DeleteBucket(b); err != nil {
				return err
			}
			if _, err := tx.CreateBucket(b); err != nil {
				return err
			}
		}
		return nil
	}))
	var legacy bytes.Buffer
	require.NoError(t, h.db.Snapshot(&legacy))
	q, err := NewDB(filepath.Join(t.TempDir(), "rebuilt.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close() })
	require.NoError(t, q.RestoreAt(&legacy, h.index))
	h.db = q
	first := h.ok(jc(configurationSweep("tenant", "snapshot", 0, 1))).Job.ConfigurationReceiptCompaction
	require.Equal(t, uint64(5), first.Horizon)
	var activated bytes.Buffer
	require.NoError(t, q.Snapshot(&activated))
	restored, err := NewDB(filepath.Join(t.TempDir(), "restored.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = restored.Close() })
	require.NoError(t, restored.RestoreAt(&activated, h.index))
	h.db = restored
	pending, err := restored.ListPendingJobConfigurationReceiptCompactions(nil, 1)
	require.NoError(t, err)
	require.Len(t, pending.Compactions, 1)
	require.Equal(t, *first, pending.Compactions[0])
	last := finishConfigurationSweep(h, first)
	require.Equal(t, uint64(5), last.ReceiptsCompacted)
	require.NoError(t, restored.db.View(func(tx *bbolt.Tx) error {
		require.Equal(t, []byte{9}, tx.Bucket(metadataBucket).Get(formatKey))
		return nil
	}))
	require.NoError(t, restored.db.Update(func(tx *bbolt.Tx) error { return tx.DeleteBucket(jobConfigurationReceiptIndexBucket) }))
	require.ErrorContains(t, restored.db.Update(initializeJobConfigurationReceiptState), "missing bucket")
}

func TestConfigurationReceiptCompactionValidationAndRejectedActivation(t *testing.T) {
	h := newChannelHarness(t)
	command := jc(configurationSweep("missing", "audit", 0, 1))
	require.Equal(t, JobConfigurationReceiptCommandVersion, command.CommandVersion())
	h.reject(command, "not_found")
	require.NoError(t, h.db.db.View(func(tx *bbolt.Tx) error {
		require.Equal(t, []byte{9}, tx.Bucket(metadataBucket).Get(formatKey))
		return nil
	}))
	h.job("tenant", "", "work", "")
	for _, batch := range []int{-1, 129} {
		h.reject(jc(configurationSweep("tenant", "bad-batch", 0, batch)), "invalid")
	}
	h.reject(jc(configurationSweep("tenant", "", 0, 1)), "invalid")
	h.reject(jc(configurationSweep("tenant", "audit", 1, 1)), "step_conflict")
	p := h.ok(jc(configurationSweep("tenant", "audit", 0, 1))).Job.ConfigurationReceiptCompaction
	h.at = h.at.Add(-time.Second)
	h.reject(jc(configurationSweep("tenant", "audit", p.Step, 1)), "stale_time")
	require.NoError(t, h.db.db.Update(func(tx *bbolt.Tx) error { return tx.Bucket(metadataBucket).Delete(jobConfigurationReceiptVersionKey) }))
	require.ErrorContains(t, h.db.db.Update(initializeJobConfigurationReceiptState), "missing its format marker")
}

func TestConfigurationReceiptCompactionIgnoresHistoricalGobWireSize(t *testing.T) {
	h := newChannelHarness(t)
	h.job("tenant", "group", "work", "driver")
	var snapshot bytes.Buffer
	require.NoError(t, h.db.Snapshot(&snapshot))
	q, err := NewDB(filepath.Join(t.TempDir(), "other-encoding.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close() })
	require.NoError(t, q.RestoreAt(&snapshot, h.index))
	other := &channelHarness{t: t, db: q, at: h.at, index: h.index}
	type historicalResult struct {
		RequestID              string
		Fingerprint            [32]byte
		Job                    *JobResult
		UnknownHistoricalField []byte
	}
	require.NoError(t, q.db.Update(func(tx *bbolt.Tx) error {
		k, _ := tx.Bucket(jobConfigurationReceiptIndexBucket).Cursor().First()
		require.NotNil(t, k)
		id := tx.Bucket(jobConfigurationReceiptIndexBucket).Get(k)
		r, err := readRecord[StateResult](tx, requestsBucket, string(id))
		if err != nil {
			return err
		}
		var wire bytes.Buffer
		if err := gob.NewEncoder(&wire).Encode(historicalResult{RequestID: r.RequestID, Fingerprint: r.Fingerprint, Job: r.Job, UnknownHistoricalField: make([]byte, MaxJobReceiptBatchPayloadBytes+1)}); err != nil {
			return err
		}
		require.Greater(t, wire.Len(), MaxJobReceiptBatchPayloadBytes)
		return tx.Bucket(requestsBucket).Put(id, wire.Bytes())
	}))
	a := h.ok(jc(configurationSweep("tenant", "encoding", 0, 128))).Job.ConfigurationReceiptCompaction
	b := other.ok(jc(configurationSweep("tenant", "encoding", 0, 128))).Job.ConfigurationReceiptCompaction
	require.True(t, a.Complete)
	require.Equal(t, a, b, "replicated batch decisions and counters use logical fields")
}
