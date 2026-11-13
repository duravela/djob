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

func compactReceipts(tenant, run string, step uint64, batch int) JobCommand {
	return JobCommand{CompactReceipts: &CompactJobReceipts{TenantID: tenant, RunID: run, ExpectedStep: step, BatchSize: batch, Actor: "operator", Reason: "remove historical payload copies"}}
}

func (h *channelHarness) fullRunReceipts(tenant, run string) map[string]StateResult {
	h.t.Helper()
	out := map[string]StateResult{}
	require.NoError(h.t, h.db.db.View(func(tx *bbolt.Tx) error {
		prefix := []byte(jobKey(tenant, run))
		c := tx.Bucket(jobReceiptRunIndexBucket).Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			r, err := readRecord[StateResult](tx, requestsBucket, string(v))
			if err != nil {
				return err
			}
			out[string(v)] = *r
		}
		return nil
	}))
	return out
}

func TestJobReceiptCompactionIsBoundedAndPreservesIdentityWithoutPayloads(t *testing.T) {
	h := newChannelHarness(t)
	run, original := h.finishedEventRun()
	h.ok(jc(purge("tenant", run.ID, run.Step, h.at, MaxJobPurgeBatch)))
	before := h.fullRunReceipts("tenant", run.ID)
	require.Greater(t, len(before), 10)
	other := h.ok(jc(JobCommand{Launch: &LaunchJob{TenantID: "tenant", DefinitionID: "work", DefinitionRevision: 1, RunID: "other"}}))
	command := jc(compactReceipts("tenant", run.ID, run.Step, 1))
	command.RequestID = "compact-first-batch"
	first := h.ok(command).Job.ReceiptCompaction
	require.Equal(t, uint64(1), first.ReceiptsCompacted)
	require.False(t, first.Complete)
	require.Len(t, h.fullRunReceipts("tenant", run.ID), len(before)-1)
	repeated := h.ok(command).Job.ReceiptCompaction
	require.Equal(t, first, repeated, "an exact retry must not compact a second batch")
	last := first
	for i := 0; !last.Complete && i < len(before); i++ {
		next := h.ok(jc(compactReceipts("tenant", run.ID, run.Step, 1))).Job.ReceiptCompaction
		require.Equal(t, last.ReceiptsCompacted+1, next.ReceiptsCompacted)
		last = next
	}
	require.True(t, last.Complete)
	require.Equal(t, uint64(len(before)), last.ReceiptsCompacted)
	require.Positive(t, last.PayloadBytesRemoved)
	require.Empty(t, h.fullRunReceipts("tenant", run.ID))
	pending, err := h.db.ListPendingJobReceiptCompactions(nil, 1)
	require.NoError(t, err)
	require.Empty(t, pending.Compactions)
	for id, previous := range before {
		require.NoError(t, h.db.db.View(func(tx *bbolt.Tx) error {
			require.Nil(t, tx.Bucket(requestsBucket).Get([]byte(id)))
			stored, err := readRecord[compactJobReceipt](tx, jobCompactReceiptsBucket, id)
			if err != nil {
				return err
			}
			receipt := stored.result()
			require.Equal(t, previous.Fingerprint, receipt.Fingerprint)
			require.Equal(t, id, receipt.RequestID)
			require.True(t, receipt.Compacted)
			require.ErrorContains(t, receipt.Err(), ReceiptCompactedCode)
			require.Nil(t, receipt.Rejection, "successful submission remains distinguishable from rejection")
			require.Nil(t, receipt.Job.Action)
			if r := receipt.Job.Run; r != nil {
				require.Equal(t, previous.Job.Run.Step, r.Step)
				require.Equal(t, previous.Job.Run.Status, r.Status)
				require.Empty(t, r.OwnerID)
				require.Empty(t, r.AttemptID)
				require.True(t, r.LeaseUntil.IsZero())
				require.Empty(t, r.State)
				require.Empty(t, r.Result)
				require.Empty(t, r.Definition.Input)
				require.Nil(t, r.Binding)
			}
			if receipt.Job.Event != nil {
				require.Empty(t, receipt.Job.Event.Data)
			}
			if receipt.Job.Activation != nil {
				require.Empty(t, receipt.Job.Activation.Data)
			}
			return nil
		}))
	}
	retried := h.apply(original)
	require.True(t, retried.Compacted)
	require.ErrorContains(t, retried.Err(), ReceiptCompactedCode)
	require.Equal(t, run.ID, retried.Job.Activation.RunID)
	changed := *original.Job.Activate
	changed.Data = []byte("different")
	conflict := original
	conflict.Job = &JobCommand{Activate: &changed}
	h.reject(conflict, "request_conflict")
	h.reject(jc(*conflict.Job), "activation_conflict")
	// Future redeliveries cannot grow the deleted run's full receipt history.
	late := jc(*original.Job)
	late.RequestID = "late-redelivery"
	require.True(t, h.ok(late).Job.Activation.DataDeleted)
	require.True(t, h.apply(late).Compacted)
	require.Empty(t, h.fullRunReceipts("tenant", run.ID))
	require.NoError(t, h.db.db.View(func(tx *bbolt.Tx) error {
		r, err := readRecord[StateResult](tx, requestsBucket, other.RequestID)
		if err == nil {
			require.Equal(t, other, *r, "another run's retry contract remains unchanged")
		}
		return err
	}))
	resource, err := h.db.GetJobResource("bus")
	require.NoError(t, err)
	require.Equal(t, ResourceAvailable, resource.State)
	require.Equal(t, "other", h.fullRunReceipts("tenant", "other")[other.RequestID].Job.Run.ID)
	status, err := h.db.GetJobRecord(JobRecordQuery{Kind: JobReceiptCompactionRecord, TenantID: "tenant", ID: run.ID})
	require.NoError(t, err)
	require.Equal(t, last, status.ReceiptCompaction)
	page, err := h.db.ListJobRecords(JobRecordQuery{Kind: JobReceiptCompactionRecord, TenantID: "tenant"}, nil, 1)
	require.NoError(t, err)
	require.Len(t, page.Entries, 1)
}

func TestJobReceiptCompactionRequiresCompletePrimaryDeletionAndExactStep(t *testing.T) {
	h := newChannelHarness(t)
	run, _ := h.finishedEventRun()
	c := compactReceipts("tenant", run.ID, run.Step, 1)
	h.reject(jc(c), "not_found")
	d := h.ok(jc(purge("tenant", run.ID, run.Step, h.at, 1))).Job.Deletion
	require.False(t, d.CleanupComplete)
	h.reject(jc(c), "job_protected")
	before := len(h.fullRunReceipts("tenant", run.ID))
	h.ok(jc(purge("tenant", run.ID, run.Step, h.at, MaxJobPurgeBatch)))
	c.CompactReceipts.ExpectedStep++
	h.reject(jc(c), "step_conflict")
	c.CompactReceipts.ExpectedStep--
	c.CompactReceipts.Actor = ""
	h.reject(jc(c), "invalid")
	c.CompactReceipts.Actor = "operator"
	c.CompactReceipts.BatchSize = MaxJobPurgeBatch + 1
	h.reject(jc(c), "invalid")
	require.Len(t, h.fullRunReceipts("tenant", run.ID), before)
	c.CompactReceipts.BatchSize = 1
	h.at = h.at.Add(time.Second)
	h.ok(jc(c))
	h.at = h.at.Add(-time.Second)
	h.reject(jc(c), "stale_time")
	// Feature activation includes durable rejections, without compacting data.
	require.NoError(t, h.db.db.View(func(tx *bbolt.Tx) error {
		require.Equal(t, []byte{7}, tx.Bucket(metadataBucket).Get(formatKey))
		return nil
	}))
}

func TestJobReceiptCompactionSnapshotRetainsProgressAndExactConflictGuard(t *testing.T) {
	h := newChannelHarness(t)
	run, event := h.finishedEventRun()
	h.ok(jc(purge("tenant", run.ID, run.Step, h.at, MaxJobPurgeBatch)))
	first := h.ok(jc(compactReceipts("tenant", run.ID, run.Step, 1))).Job.ReceiptCompaction
	var snapshot bytes.Buffer
	require.NoError(t, h.db.Snapshot(&snapshot))
	q, err := NewDB(filepath.Join(t.TempDir(), "restored.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close() })
	require.NoError(t, q.RestoreAt(&snapshot, h.index))
	h2 := &channelHarness{t: t, db: q, at: h.at, index: h.index}
	pending, err := q.ListPendingJobReceiptCompactions(nil, 1)
	require.NoError(t, err)
	require.Equal(t, []JobReceiptCompaction{*first}, pending.Compactions)
	last := h2.ok(jc(compactReceipts("tenant", run.ID, run.Step, MaxJobPurgeBatch))).Job.ReceiptCompaction
	require.True(t, last.Complete)
	require.True(t, h2.apply(event).Compacted)
	changed := *event.Job.Activate
	changed.Data = []byte("conflict")
	event.Job = &JobCommand{Activate: &changed}
	h2.reject(event, "request_conflict")
	require.NoError(t, q.db.Update(func(tx *bbolt.Tx) error { return tx.DeleteBucket(jobCompactReceiptsBucket) }))
	require.ErrorContains(t, q.db.Update(initializeJobReceiptState), "missing bucket")
}

func TestJobReceiptIndexRebuildsBeforeActivationAndEncodingIsStable(t *testing.T) {
	h := newChannelHarness(t)
	run, _ := h.finishedEventRun()
	want := h.fullRunReceipts("tenant", run.ID)
	require.NoError(t, h.db.db.Update(func(tx *bbolt.Tx) error {
		if err := tx.DeleteBucket(jobReceiptRunIndexBucket); err != nil {
			return err
		}
		_, err := tx.CreateBucket(jobReceiptRunIndexBucket)
		return err
	}))
	var snapshot bytes.Buffer
	require.NoError(t, h.db.Snapshot(&snapshot))
	q, err := NewDB(filepath.Join(t.TempDir(), "restored.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close() })
	require.NoError(t, q.RestoreAt(&snapshot, h.index))
	h2 := &channelHarness{t: t, db: q, at: h.at, index: h.index}
	require.Equal(t, want, h2.fullRunReceipts("tenant", run.ID))
	c := jc(compactReceipts("tenant", run.ID, run.Step, 1))
	c.RequestID, c.At = "compact", h.at
	require.Equal(t, JobReceiptCommandVersion, c.CommandVersion())
	fingerprint, err := c.Fingerprint()
	require.NoError(t, err)
	var wire bytes.Buffer
	require.NoError(t, gob.NewEncoder(&wire).Encode(c))
	var decoded StateCommand
	require.NoError(t, gob.NewDecoder(&wire).Decode(&decoded))
	roundtrip, err := decoded.Fingerprint()
	require.NoError(t, err)
	require.Equal(t, fingerprint, roundtrip)
}

func TestJobReceiptCompactionBoundsPayloadBytesPerBatch(t *testing.T) {
	h := newChannelHarness(t)
	h.job("tenant", "group", "work", "driver", "bus")
	definition, err := h.db.GetJobRecord(JobRecordQuery{Kind: JobDefinitionRecord, TenantID: "tenant", ID: "work"})
	require.NoError(t, err)
	definition.Definition.Input = bytes.Repeat([]byte("d"), 1<<20)
	h.ok(jc(JobCommand{PutDefinition: &PutJobDefinition{ExpectedRevision: 1, Definition: *definition.Definition}}))
	binding, err := h.db.GetJobRecord(JobRecordQuery{Kind: JobBindingRecord, TenantID: "tenant", ID: "driver"})
	require.NoError(t, err)
	binding.Binding.Config = bytes.Repeat([]byte("b"), 1<<20)
	h.ok(jc(JobCommand{PutBinding: &PutJobBinding{ExpectedRevision: 1, Binding: *binding.Binding}}))
	h.ok(jc(JobCommand{Launch: &LaunchJob{TenantID: "tenant", DefinitionID: "work", DefinitionRevision: 2, RunID: "large"}}))
	owner := h.claimJob("tenant", "large", "attempt")
	for range 3 {
		h.ok(jc(JobCommand{Renew: &RenewJob{JobOwner: owner, LeaseUntil: h.at.Add(2 * time.Minute)}}))
	}
	run := h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, Kind: JobComplete, Status: JobSucceeded, Quiescent: true}})).Job.Run
	h.ok(jc(purge("tenant", run.ID, run.Step, h.at, MaxJobPurgeBatch)))
	total := len(h.fullRunReceipts("tenant", run.ID))
	first := h.ok(jc(compactReceipts("tenant", run.ID, run.Step, MaxJobPurgeBatch))).Job.ReceiptCompaction
	require.False(t, first.Complete)
	require.Positive(t, first.ReceiptsCompacted)
	require.Less(t, first.ReceiptsCompacted, uint64(total))
	require.LessOrEqual(t, first.PayloadBytesRemoved, uint64(MaxJobReceiptBatchPayloadBytes))
	last := first
	for range 4 {
		next := h.ok(jc(compactReceipts("tenant", run.ID, run.Step, MaxJobPurgeBatch))).Job.ReceiptCompaction
		require.LessOrEqual(t, next.PayloadBytesRemoved-last.PayloadBytesRemoved, uint64(MaxJobReceiptBatchPayloadBytes))
		last = next
		if last.Complete {
			break
		}
	}
	require.True(t, last.Complete)
	require.Equal(t, uint64(total), last.ReceiptsCompacted)
}

func TestJobReceiptCompactionIsIndependentOfHistoricalGobEncoding(t *testing.T) {
	h := newChannelHarness(t)
	run, _ := h.finishedEventRun()
	h.ok(jc(purge("tenant", run.ID, run.Step, h.at, MaxJobPurgeBatch)))
	var snapshot bytes.Buffer
	require.NoError(t, h.db.Snapshot(&snapshot))
	q, err := NewDB(filepath.Join(t.TempDir(), "other-encoding.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close() })
	require.NoError(t, q.RestoreAt(&snapshot, h.index))
	other := &channelHarness{t: t, db: q, at: h.at, index: h.index}
	// Earlier writers used a different result schema. These receipts decode to
	// the same logical values but have different wire sizes and type descriptors.
	type historicalResult struct {
		RequestID   string
		Fingerprint [32]byte
		Job         *JobResult
	}
	changed := 0
	require.NoError(t, q.db.Update(func(tx *bbolt.Tx) error {
		prefix := []byte(jobKey("tenant", run.ID))
		cursor := tx.Bucket(jobReceiptRunIndexBucket).Cursor()
		for k, v := cursor.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = cursor.Next() {
			original, err := readRecord[StateResult](tx, requestsBucket, string(v))
			if err != nil {
				return err
			}
			var wire bytes.Buffer
			require.NoError(t, gob.NewEncoder(&wire).Encode(historicalResult{RequestID: original.RequestID, Fingerprint: original.Fingerprint, Job: original.Job}))
			var decoded StateResult
			require.NoError(t, gob.NewDecoder(bytes.NewReader(wire.Bytes())).Decode(&decoded))
			require.Equal(t, *original, decoded)
			if len(tx.Bucket(requestsBucket).Get(v)) != wire.Len() {
				changed++
			}
			if err := tx.Bucket(requestsBucket).Put(v, wire.Bytes()); err != nil {
				return err
			}
		}
		return nil
	}))
	require.Positive(t, changed)
	a := h.ok(jc(compactReceipts("tenant", run.ID, run.Step, MaxJobPurgeBatch))).Job.ReceiptCompaction
	b := other.ok(jc(compactReceipts("tenant", run.ID, run.Step, MaxJobPurgeBatch))).Job.ReceiptCompaction
	require.Equal(t, a, b, "replicated progress and batch decisions cannot depend on local serialization")
}

// Freeze the v13 payload accounting contract for deterministic future replay.
func TestJobReceiptV13PayloadAccounting(t *testing.T) {
	j := &JobResult{
		Run: &JobRun{
			Definition:    JobDefinition{Input: make([]byte, 3)},
			Binding:       &JobBinding{Config: make([]byte, 5)},
			State:         make([]byte, 7),
			Result:        make([]byte, 11),
			Error:         "error",
			CancelReason:  "stop",
			Activation:    &JobActivation{Data: make([]byte, 13)},
			PendingAction: &JobAction{Input: make([]byte, 17), Result: make([]byte, 19), Error: "lost"},
		},
		Action:     &JobAction{Input: make([]byte, 23), Result: make([]byte, 29), Error: "bad"},
		Event:      &JobEvent{Data: make([]byte, 31), Key: "reply"},
		Activation: &JobActivation{Data: make([]byte, 37)},
	}
	require.Equal(t, uint64(216), jobReceiptPayloadBytes(j))
}
