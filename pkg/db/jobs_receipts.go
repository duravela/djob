package db

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math"
	"time"

	"go.etcd.io/bbolt"
)

const (
	ReceiptCompactedCode           = "receipt_compacted"
	MaxJobReceiptBatchPayloadBytes = 8 << 20
)

// Receipt compaction is a separate, explicit operation after primary cleanup.
// Request identities never expire: a compact receipt can acknowledge public
// submission, but cannot be used as a worker's historical execution result.
type CompactJobReceipts struct {
	TenantID, RunID string
	ExpectedStep    uint64
	BatchSize       int
	Actor, Reason   string
}

type JobReceiptCompaction struct {
	TenantID, RunID                        string
	Step                                   uint64
	Actor, Reason                          string
	StartedAt, LastCleanupAt               time.Time
	ReceiptsCompacted, PayloadBytesRemoved uint64
	Complete                               bool
}

type JobReceiptCompactionPage struct {
	Compactions []JobReceiptCompaction
	NextCursor  []byte
}

// Only fields used by the public acknowledgement survive. In particular no
// ownership token, lease, handler configuration, action, or payload survives.
type compactJobReceipt struct {
	Configuration                   *compactJobConfigurationReceipt
	RequestID                       string
	Fingerprint                     [32]byte
	TenantID, RunID                 string
	HasRun                          bool
	Step                            uint64
	Status                          JobStatus
	EventID                         string
	TriggerID, Source, ActivationID string
	TriggerRevision                 uint64
}

var (
	jobReceiptVersionKey       = []byte("job_receipt_state_version")
	jobReceiptRunIndexBucket   = []byte("job_receipt_run_index")
	jobCompactReceiptsBucket   = []byte("job_compact_receipts")
	jobReceiptCompactionBucket = []byte("job_receipt_compactions")
	jobReceiptPendingBucket    = []byte("job_receipt_pending")
	jobReceiptBuckets          = [][]byte{jobReceiptRunIndexBucket, jobCompactReceiptsBucket, jobReceiptCompactionBucket, jobReceiptPendingBucket}
)

func compactReceiptFor(r StateResult) (*compactJobReceipt, error) {
	if r.Rejection != nil || r.Compacted || r.Job == nil {
		return nil, nil
	}
	j := r.Job
	// Configuration, materialization and cleanup receipts have their own lifetime.
	if j.ConfigurationRetirement != nil || j.TriggerRetirement != nil || j.Tenant != nil || j.Group != nil || j.Definition != nil || j.Binding != nil || j.Resource != nil || j.Grant != nil || j.Trigger != nil || j.Limits != nil || j.Retention != nil || j.Deletion != nil || j.ReceiptCompaction != nil || j.ConfigurationLimits != nil || j.ConfigurationReceiptCompaction != nil {
		return nil, nil
	}
	c := &compactJobReceipt{RequestID: r.RequestID, Fingerprint: r.Fingerprint}
	identity := func(tenant, run string) error {
		if !validIdentity(tenant) || !validIdentity(run) || (c.RunID != "" && (tenant != c.TenantID || run != c.RunID)) {
			return fmt.Errorf("inconsistent run identity in job receipt %q", r.RequestID)
		}
		c.TenantID, c.RunID = tenant, run
		return nil
	}
	if j.Run != nil {
		if err := identity(j.Run.TenantID, j.Run.ID); err != nil {
			return nil, err
		}
		c.HasRun, c.Step, c.Status = true, j.Run.Step, j.Run.Status
	}
	if j.Action != nil {
		if err := identity(j.Action.TenantID, j.Action.RunID); err != nil {
			return nil, err
		}
	}
	if j.Event != nil {
		if err := identity(j.Event.TenantID, j.Event.RunID); err != nil {
			return nil, err
		}
		c.EventID = j.Event.ID
	}
	if a := j.Activation; a != nil {
		if err := identity(a.TenantID, a.RunID); err != nil {
			return nil, err
		}
		c.TriggerID, c.Source, c.ActivationID, c.TriggerRevision = a.TriggerID, a.Source, a.EventID, a.TriggerRevision
	}
	if c.RunID == "" {
		return nil, nil
	}
	return c, nil
}

func (c *compactJobReceipt) result() StateResult {
	j := &JobResult{}
	if c.Configuration != nil {
		j = c.Configuration.result(c.TenantID)
	}
	if c.HasRun {
		j.Run = &JobRun{TenantID: c.TenantID, ID: c.RunID, Step: c.Step, Status: c.Status}
	}
	if c.EventID != "" {
		j.Event = &JobEvent{TenantID: c.TenantID, RunID: c.RunID, ID: c.EventID}
	}
	if c.ActivationID != "" {
		j.Activation = &JobActivation{TenantID: c.TenantID, RunID: c.RunID, TriggerID: c.TriggerID, Source: c.Source, EventID: c.ActivationID, TriggerRevision: c.TriggerRevision}
	}
	return StateResult{RequestID: c.RequestID, Fingerprint: c.Fingerprint, Compacted: true, Job: j}
}

// v13 accounting measures logical payload copies, never locally encoded bytes:
// older writers and gob type IDs can produce different encodings on replicas.
// Keep this field set stable for v13 replay. A repeated payload in Run and Action
// counts twice because both copies disappear from the historical receipt.
func jobReceiptPayloadBytes(j *JobResult) uint64 {
	var size uint64
	action := func(a *JobAction) {
		if a != nil {
			size += uint64(len(a.Input)) + uint64(len(a.Result)) + uint64(len(a.Error))
		}
	}
	if run := j.Run; run != nil {
		size += uint64(len(run.Definition.Input)) + uint64(len(run.State)) + uint64(len(run.Result)) + uint64(len(run.Error)) + uint64(len(run.CancelReason))
		if run.Binding != nil {
			size += uint64(len(run.Binding.Config))
		}
		if run.Activation != nil {
			size += uint64(len(run.Activation.Data))
		}
		action(run.PendingAction)
	}
	action(j.Action)
	if j.Event != nil {
		size += uint64(len(j.Event.Data)) + uint64(len(j.Event.Key))
	}
	if j.Activation != nil {
		size += uint64(len(j.Activation.Data))
	}
	return size
}

func initializeJobReceiptState(tx *bbolt.Tx) error {
	meta := tx.Bucket(metadataBucket)
	version := meta.Get(jobReceiptVersionKey)
	if version != nil && !bytes.Equal(version, []byte{1}) {
		return fmt.Errorf("unsupported job receipt state version")
	}
	activated := meta.Get(formatKey)[0] >= 7
	if activated && version == nil {
		return fmt.Errorf("job receipt state is missing its format marker")
	}
	for _, name := range jobReceiptBuckets {
		b := tx.Bucket(name)
		if version != nil && b == nil {
			return fmt.Errorf("job receipt state is missing bucket %q", name)
		}
		if !activated && !bytes.Equal(name, jobReceiptRunIndexBucket) && b != nil && b.Stats().KeyN != 0 {
			return fmt.Errorf("compacted job receipts without an activated database format")
		}
		if _, err := tx.CreateBucketIfNotExists(name); err != nil {
			return err
		}
	}
	if !activated {
		// v8-v12 writers leave this derived index stale. Until format 7 excludes
		// them, rebuild from full receipts on every open and snapshot restore.
		if err := tx.DeleteBucket(jobReceiptRunIndexBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucket(jobReceiptRunIndexBucket); err != nil {
			return err
		}
		if err := tx.Bucket(requestsBucket).ForEach(func(k, data []byte) error {
			var r StateResult
			if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&r); err != nil {
				return fmt.Errorf("decode receipt for index: %w", err)
			}
			c, err := compactReceiptFor(r)
			if err != nil || c == nil {
				return err
			}
			if c.RequestID != string(k) {
				return fmt.Errorf("inconsistent job receipt key")
			}
			return tx.Bucket(jobReceiptRunIndexBucket).Put([]byte(jobKey(c.TenantID, c.RunID, c.RequestID)), k)
		}); err != nil {
			return err
		}
	}
	return meta.Put(jobReceiptVersionKey, []byte{1})
}

func writeStateReceipt(tx *bbolt.Tx, r StateResult) error {
	c, err := compactReceiptFor(r)
	if err != nil {
		return err
	}
	if c != nil {
		key := jobKey(c.TenantID, c.RunID)
		if tx.Bucket(jobReceiptCompactionBucket).Get([]byte(key)) != nil {
			// Once compaction is committed, late activation redeliveries must not
			// accumulate full receipts again. Their public acknowledgement is stable.
			return writeRecord(tx, jobCompactReceiptsBucket, r.RequestID, storedCompactReceipt(c))
		}
		if err := tx.Bucket(jobReceiptRunIndexBucket).Put([]byte(jobKey(c.TenantID, c.RunID, r.RequestID)), []byte(r.RequestID)); err != nil {
			return err
		}
	}
	if err := indexConfigurationReceipt(tx, r); err != nil {
		return err
	}
	return writeRecord(tx, requestsBucket, r.RequestID, storedStateReceipt(r))
}

func compactJobReceipts(tx *bbolt.Tx, s *jobChanges, command StateCommand, c CompactJobReceipts, r *JobResult) error {
	if !validIdentity(c.TenantID) || !validIdentity(c.RunID) {
		return reject("invalid", "receipt compaction requires tenant and run")
	}
	if err := validResolution(c.Actor, c.Reason); err != nil {
		return err
	}
	if c.BatchSize == 0 {
		c.BatchSize = MaxJobPurgeBatch
	}
	if c.BatchSize < 1 || c.BatchSize > MaxJobPurgeBatch {
		return reject("invalid", "receipt compaction batch must be 1..128")
	}
	key := jobKey(c.TenantID, c.RunID)
	deletion, err := readRecord[JobDeletion](tx, jobDeletionBucket, key)
	if err != nil {
		return err
	}
	if deletion.Step != c.ExpectedStep {
		return reject("step_conflict", "deleted run checkpoint differs")
	}
	if !deletion.CleanupComplete {
		return reject("job_protected", "primary history cleanup must finish before receipt compaction")
	}
	if command.At.Before(deletion.LastCleanupAt) {
		return reject("stale_time", "receipt compaction predates primary cleanup")
	}
	progress := &JobReceiptCompaction{TenantID: c.TenantID, RunID: c.RunID, Step: deletion.Step, Actor: c.Actor, Reason: c.Reason, StartedAt: command.At}
	if tx.Bucket(jobReceiptCompactionBucket).Get([]byte(key)) != nil {
		progress, err = readRecord[JobReceiptCompaction](tx, jobReceiptCompactionBucket, key)
		if err != nil {
			return err
		}
		if progress.Complete {
			r.ReceiptCompaction = progress
			return nil
		}
		if command.At.Before(progress.LastCleanupAt) {
			return reject("stale_time", "receipt compaction predates the prior batch")
		}
	}
	prefix := []byte(key)
	cursor := tx.Bucket(jobReceiptRunIndexBucket).Cursor()
	removed := 0
	var batchBytes uint64
	remaining := false
	for k, v := cursor.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = cursor.Next() {
		if removed == c.BatchSize {
			remaining = true
			break
		}
		data := tx.Bucket(requestsBucket).Get(v)
		if data == nil {
			return fmt.Errorf("receipt index refers to missing full receipt")
		}
		var full StateResult
		if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&full); err != nil {
			return err
		}
		compact, err := compactReceiptFor(full)
		if err != nil {
			return err
		}
		if compact == nil || compact.TenantID != c.TenantID || compact.RunID != c.RunID || compact.RequestID != string(v) || string(k) != jobKey(c.TenantID, c.RunID, compact.RequestID) || tx.Bucket(jobCompactReceiptsBucket).Get(v) != nil {
			return fmt.Errorf("inconsistent job receipt index")
		}
		payloadBytes := jobReceiptPayloadBytes(full.Job)
		// Always allow one oversized receipt so existing history cannot stall.
		if removed > 0 && (payloadBytes > MaxJobReceiptBatchPayloadBytes || batchBytes > MaxJobReceiptBatchPayloadBytes-payloadBytes) {
			remaining = true
			break
		}
		batchBytes += payloadBytes
		var encoded bytes.Buffer
		if err := gob.NewEncoder(&encoded).Encode(storedCompactReceipt(compact)); err != nil {
			return err
		}
		if math.MaxUint64-progress.PayloadBytesRemoved < payloadBytes || progress.ReceiptsCompacted == math.MaxUint64 {
			return fmt.Errorf("receipt compaction counters overflow")
		}
		progress.PayloadBytesRemoved += payloadBytes
		progress.ReceiptsCompacted++
		s.index(jobCompactReceiptsBucket, string(v), encoded.Bytes())
		s.del(requestsBucket, string(v))
		s.del(jobReceiptRunIndexBucket, string(k))
		removed++
	}
	progress.LastCleanupAt, progress.Complete = command.At, !remaining
	s.put(jobReceiptCompactionBucket, key, progress)
	if remaining {
		s.index(jobReceiptPendingBucket, key, []byte(key))
	} else {
		s.del(jobReceiptPendingBucket, key)
	}
	r.ReceiptCompaction = progress
	return nil
}

func (q *DB) GetJobReceiptCompaction(tenant, run string) (*JobReceiptCompaction, error) {
	return getStateRecord[JobReceiptCompaction](q, jobReceiptCompactionBucket, jobKey(tenant, run))
}

func (q *DB) ListPendingJobReceiptCompactions(after []byte, limit int) (JobReceiptCompactionPage, error) {
	page := JobReceiptCompactionPage{}
	if err := queryLimit(limit); err != nil {
		return page, err
	}
	err := q.db.View(func(tx *bbolt.Tx) error {
		cursor := tx.Bucket(jobReceiptPendingBucket).Cursor()
		for k, v := pageStart(cursor, after); k != nil && len(page.Compactions) < limit; k, v = cursor.Next() {
			c, err := readRecord[JobReceiptCompaction](tx, jobReceiptCompactionBucket, string(v))
			if err != nil {
				return err
			}
			if c.Complete || string(k) != jobKey(c.TenantID, c.RunID) {
				return fmt.Errorf("inconsistent receipt compaction pending index")
			}
			page.Compactions = append(page.Compactions, *c)
			page.NextCursor = bytes.Clone(k)
		}
		return nil
	})
	return page, err
}
