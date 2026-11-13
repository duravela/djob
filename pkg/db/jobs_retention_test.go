package db

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func purge(tenant, run string, step uint64, before time.Time, batch int) JobCommand {
	return JobCommand{Purge: &PurgeJob{TenantID: tenant, RunID: run, ExpectedStep: step, FinishedBefore: before, BatchSize: batch, Actor: "operator", Reason: "completed history retention"}}
}
func (h *channelHarness) finishedEventRun() (*JobRun, StateCommand) {
	h.t.Helper()
	h.job("tenant", "group", "work", "driver", "bus")
	h.eventTrigger("tenant", "ingest", "meters", 4)
	event := jc(activation("tenant", "ingest", "meters", "reading", h.at))
	event.RequestID = "original-event"
	accepted := h.ok(event).Job.Activation
	owner := h.claimJob("tenant", accepted.RunID, "attempt")
	for i := 0; i < 3; i++ {
		result := h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, ExpectedStep: uint64(i), Kind: JobDoAction, Operation: "read"}}))
		h.ok(jc(JobCommand{CompleteAction: &CompleteJobAction{JobOwner: owner, Sequence: result.Job.Action.Sequence, Status: EffectSucceeded, Result: []byte("result")}}))
	}
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		h.ok(jc(JobCommand{Signal: &SignalJob{TenantID: "tenant", RunID: accepted.RunID, EventID: id, Key: "reply", Data: []byte("payload")}}))
	}
	return h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, ExpectedStep: 3, Kind: JobComplete, Status: JobSucceeded, Quiescent: true}})).Job.Run, event
}

func TestJobPurgeIsBoundedAndRetainsExternalEventAndRunIdentity(t *testing.T) {
	h := newChannelHarness(t)
	run, event := h.finishedEventRun()
	h.limits(JobLimits{TenantID: "tenant", MaxStoredRuns: 1, MaxActivations: 1})
	c := purge("tenant", run.ID, run.Step, h.at, 1)
	first := h.ok(jc(c)).Job.Deletion
	require.False(t, first.CleanupComplete)
	require.Equal(t, uint64(3), first.RecordsDeleted)
	require.Zero(t, h.usage("tenant").StoredRuns)
	require.Zero(t, h.usage("tenant").Activations)
	require.Zero(t, h.usage("tenant").ActivationBytes)
	require.Equal(t, uint64(5), h.usage("tenant").InboxEvents)
	_, err := h.db.GetJobRecord(JobRecordQuery{Kind: JobRunRecord, TenantID: "tenant", ID: run.ID})
	require.ErrorContains(t, err, "purged")
	h.reject(jc(JobCommand{Launch: &LaunchJob{TenantID: "tenant", DefinitionID: "work", DefinitionRevision: 1, RunID: run.ID}}), "gone")
	duplicate := h.ok(jc(*event.Job)).Job.Activation
	require.Equal(t, run.ID, duplicate.RunID)
	require.True(t, duplicate.DataDeleted)
	require.Empty(t, duplicate.Data)
	digest := sha256.Sum256([]byte("measurement"))
	require.Equal(t, digest[:], duplicate.DataSHA256)
	changed := *event.Job.Activate
	changed.Data = []byte("changed")
	h.reject(jc(JobCommand{Activate: &changed}), "activation_conflict")
	require.Equal(t, []byte("measurement"), h.ok(event).Job.Activation.Data, "original request receipts retain their original acknowledgement")
	last := first
	for i := 0; !last.CleanupComplete && i < 30; i++ {
		next := h.ok(jc(c)).Job.Deletion
		require.LessOrEqual(t, next.RecordsDeleted-last.RecordsDeleted, uint64(1))
		last = next
	}
	require.True(t, last.CleanupComplete)
	require.Equal(t, uint64(16), last.RecordsDeleted)
	require.Equal(t, JobUsage{TenantID: "tenant"}, h.usage("tenant"))
	pending, err := h.db.ListPendingJobPurges(nil, 10)
	require.NoError(t, err)
	require.Empty(t, pending.Deletions)
	deletionRecord, err := h.db.GetJobRecord(JobRecordQuery{Kind: JobDeletionRecord, TenantID: "tenant", ID: run.ID})
	require.NoError(t, err)
	require.Equal(t, last, deletionRecord.Deletion)
	receiptRecord, err := h.db.GetJobRecord(JobRecordQuery{Kind: JobActivationRecord, TenantID: "tenant", TriggerID: "ingest", Source: "meters", ID: "reading"})
	require.NoError(t, err)
	require.True(t, receiptRecord.Activation.DataDeleted)
	h.ok(jc(activation("tenant", "ingest", "meters", "new-reading", h.at)))
	require.Equal(t, uint64(1), h.usage("tenant").StoredRuns)
	resource, err := h.db.GetJobResource("bus")
	require.NoError(t, err)
	require.Equal(t, ResourceAvailable, resource.State)
	clone := (StateResult{Job: &JobResult{Deletion: last, Activation: duplicate}}).Clone()
	clone.Job.Deletion.Resources[0] = "changed"
	clone.Job.Activation.DataSHA256[0] ^= 255
	require.Equal(t, "bus", last.Resources[0])
	require.Equal(t, digest[:], duplicate.DataSHA256)
}

func TestJobPurgeProtectsLiveWaitsUnknownEffectsAndRecoveryAudit(t *testing.T) {
	h := newChannelHarness(t)
	h.job("tenant", "group", "work", "driver", "bus")
	h.launch("tenant", "work", "run")
	h.reject(jc(purge("tenant", "run", 0, h.at, 128)), "job_protected")
	owner := h.claimJob("tenant", "run", "attempt")
	h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, Kind: JobDoAction, Operation: "write"}}))
	h.reject(jc(purge("tenant", "run", 1, h.at, 128)), "job_protected")
	h.at = h.at.Add(time.Minute)
	h.ok(jc(JobCommand{Expire: &ExpireJob{JobOwner: owner, LeaseUntil: h.at}}))
	h.reject(jc(purge("tenant", "run", 1, h.at, 128)), "job_protected")
	resolution := jc(JobCommand{Resolve: &ResolveJob{TenantID: "tenant", RunID: "run", ExpectedStep: 1, Status: JobFailed, Actor: "operator", Reason: "confirmed failure"}})
	resolution.RequestID = "run-resolution"
	h.ok(resolution)
	h.reject(jc(purge("tenant", "run", 2, h.at, 128)), "job_protected")
	resourceResolution := jc(JobCommand{ResolveResource: &ResolveJobResource{ResourceID: "bus", Epoch: 1, Actor: "operator", Reason: "confirmed stopped", Quiescent: true}})
	resourceResolution.RequestID = "resource-resolution"
	h.ok(resourceResolution)
	h.reject(jc(purge("tenant", "run", 2, h.at, 128)), "job_protected") // Unknown action history remains protected after outcome resolution.
	audit, err := h.db.GetJobResolution("run-resolution")
	require.NoError(t, err)
	require.Equal(t, "confirmed failure", audit.Reason)
	_, err = h.db.GetJobResolution("resource-resolution")
	require.NoError(t, err)
	h.job("waiting", "", "work", "")
	h.launch("waiting", "work", "wait")
	waiter := h.claimJob("waiting", "wait", "owner")
	h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: waiter, Kind: JobWait, WaitKey: "reply"}}))
	h.reject(jc(purge("waiting", "wait", 1, h.at, 128)), "job_protected")
}

func TestJobPurgeRetainsRecoveryAuditAfterQuiescenceAndKnownOutcome(t *testing.T) {
	h := newChannelHarness(t)
	h.job("tenant", "group", "work", "driver", "bus")
	h.launch("tenant", "work", "run")
	owner := h.claimJob("tenant", "run", "attempt")
	h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, Kind: JobDoAction, Operation: "read"}}))
	h.ok(jc(JobCommand{CompleteAction: &CompleteJobAction{JobOwner: owner, Sequence: 1, Status: EffectSucceeded}}))
	h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, ExpectedStep: 1, Kind: JobComplete, Status: JobSucceeded}})) // Quiescence was not established.
	resolve := jc(JobCommand{Resolve: &ResolveJob{TenantID: "tenant", RunID: "run", ExpectedStep: 2, Status: JobSucceeded, Actor: "operator", Reason: "confirmed result"}})
	resolve.RequestID = "outcome-audit"
	h.ok(resolve)
	h.reject(jc(purge("tenant", "run", 3, h.at, 128)), "job_protected")
	recover := jc(JobCommand{ResolveResource: &ResolveJobResource{ResourceID: "bus", Epoch: 1, Actor: "operator", Reason: "confirmed exclusion", Quiescent: true}})
	recover.RequestID = "resource-audit"
	h.ok(recover)
	require.True(t, h.ok(jc(purge("tenant", "run", 3, h.at, 128))).Job.Deletion.CleanupComplete)
	_, err := h.db.GetJobResolution("outcome-audit")
	require.NoError(t, err)
	_, err = h.db.GetJobResolution("resource-audit")
	require.NoError(t, err)
	resource, err := h.db.GetJobResource("bus")
	require.NoError(t, err)
	require.Equal(t, ResourceAvailable, resource.State)
}

func TestJobPurgeSnapshotResumesPartialCleanupAndDigestDeduplication(t *testing.T) {
	h := newChannelHarness(t)
	run, event := h.finishedEventRun()
	command := purge("tenant", run.ID, run.Step, h.at, 2)
	first := h.ok(jc(command)).Job.Deletion
	require.False(t, first.CleanupComplete)
	var snapshot bytes.Buffer
	require.NoError(t, h.db.Snapshot(&snapshot))
	q, err := NewDB(filepath.Join(t.TempDir(), "restored.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close() })
	require.NoError(t, q.RestoreAt(&snapshot, h.index))
	h2 := &channelHarness{t: t, db: q, at: h.at.Add(time.Second), index: h.index}
	pending, err := q.ListPendingJobPurges(nil, 1)
	require.NoError(t, err)
	require.Len(t, pending.Deletions, 1)
	for range 12 {
		d := h2.ok(jc(command)).Job.Deletion
		if d.CleanupComplete {
			break
		}
	}
	d, err := q.GetJobDeletion("tenant", run.ID)
	require.NoError(t, err)
	require.True(t, d.CleanupComplete)
	require.Equal(t, JobUsage{TenantID: "tenant"}, h2.usage("tenant"))
	repeated := h2.ok(jc(*event.Job)).Job.Activation
	require.True(t, repeated.DataDeleted)
	require.Equal(t, run.ID, repeated.RunID)
	require.NoError(t, q.db.Update(func(tx *bbolt.Tx) error { return tx.DeleteBucket(jobDeletionBucket) }))
	require.ErrorContains(t, q.db.Update(initializeJobRetentionState), "missing bucket")
}

func TestJobRetentionPolicyUsesLiveRevisionAndExplicitCutoff(t *testing.T) {
	h := newChannelHarness(t)
	h.job("tenant", "", "work", "")
	h.launch("tenant", "work", "run")
	h.ok(jc(JobCommand{Cancel: &CancelJob{TenantID: "tenant", RunID: "run"}}))
	policy, err := h.db.GetJobRetention("tenant")
	require.NoError(t, err)
	require.False(t, policy.Enabled)
	h.ok(jc(JobCommand{PutRetention: &PutJobRetention{Policy: JobRetentionPolicy{TenantID: "tenant", Enabled: true, KeepFor: time.Second}}}))
	at := h.at.Add(2 * time.Second)
	page, err := h.db.ListJobRetentionCandidates(at, nil, 1)
	require.NoError(t, err)
	require.Len(t, page.Candidates, 1)
	candidate := page.Candidates[0]
	command := purge("tenant", "run", candidate.Step, candidate.FinishedBefore, 1)
	command.Purge.PolicyRevision = candidate.PolicyRevision
	h.ok(jc(JobCommand{PutRetention: &PutJobRetention{ExpectedRevision: 1, Policy: JobRetentionPolicy{TenantID: "tenant"}}}))
	h.at = at
	h.reject(jc(command), "revision_conflict")
	h.ok(jc(JobCommand{PutRetention: &PutJobRetention{ExpectedRevision: 2, Policy: JobRetentionPolicy{TenantID: "tenant", Enabled: true, KeepFor: time.Second}}}))
	command.Purge.PolicyRevision = 3
	command.Purge.FinishedBefore = h.at
	h.reject(jc(command), "invalid")
	command.Purge.FinishedBefore = candidate.FinishedBefore
	command.Purge.ExpectedStep = 1
	h.reject(jc(command), "step_conflict")
	command.Purge.ExpectedStep = 0
	require.True(t, h.ok(jc(command)).Job.Deletion.CleanupComplete)
}

func TestJobRetentionRebuildsIndexBeforeActivationAndPreservesFingerprintEncoding(t *testing.T) {
	h := newChannelHarness(t)
	h.job("tenant", "", "work", "")
	h.launch("tenant", "work", "run")
	h.ok(jc(JobCommand{Cancel: &CancelJob{TenantID: "tenant", RunID: "run"}}))
	require.NoError(t, h.db.db.Update(func(tx *bbolt.Tx) error {
		c := tx.Bucket(jobRetentionIndexBucket).Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			if err := c.Delete(); err != nil {
				return err
			}
		}
		return nil
	}))
	var snapshot bytes.Buffer
	require.NoError(t, h.db.Snapshot(&snapshot))
	q, err := NewDB(filepath.Join(t.TempDir(), "restored.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close() })
	require.NoError(t, q.RestoreAt(&snapshot, h.index))
	h2 := &channelHarness{t: t, db: q, at: h.at, index: h.index}
	h2.ok(jc(JobCommand{PutRetention: &PutJobRetention{Policy: JobRetentionPolicy{TenantID: "tenant", Enabled: true, KeepFor: time.Second}}}))
	page, err := q.ListJobRetentionCandidates(h.at.Add(2*time.Second), nil, 1)
	require.NoError(t, err)
	require.Len(t, page.Candidates, 1)
	command := jc(purge("tenant", "run", 0, h.at.In(time.FixedZone("source", 3600)), 1))
	command.RequestID = "purge"
	command.At = h.at
	require.Equal(t, JobRetentionCommandVersion, command.CommandVersion())
	expected, err := command.Fingerprint()
	require.NoError(t, err)
	var encoded bytes.Buffer
	require.NoError(t, gob.NewEncoder(&encoded).Encode(command))
	var decoded StateCommand
	require.NoError(t, gob.NewDecoder(&encoded).Decode(&decoded))
	actual, err := decoded.Fingerprint()
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestJobRetentionRejectionsActivateGuardedFormat(t *testing.T) {
	for _, kind := range []string{"policy", "purge"} {
		t.Run(kind, func(t *testing.T) {
			h := newChannelHarness(t)
			require.NoError(t, h.db.db.View(func(tx *bbolt.Tx) error {
				require.Less(t, tx.Bucket(metadataBucket).Get(formatKey)[0], byte(6))
				return nil
			}))
			command := JobCommand{PutRetention: &PutJobRetention{Policy: JobRetentionPolicy{TenantID: "missing"}}}
			if kind == "purge" {
				command = purge("missing", "run", 0, h.at, 1)
			}
			h.reject(jc(command), "not_found")
			require.NoError(t, h.db.db.View(func(tx *bbolt.Tx) error {
				require.Equal(t, []byte{6}, tx.Bucket(metadataBucket).Get(formatKey))
				return nil
			}))
		})
	}
}
