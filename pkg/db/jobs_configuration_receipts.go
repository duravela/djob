package db

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"math"
	"time"

	"go.etcd.io/bbolt"
)

// An explicitly named sweep captures the current tenant receipt horizon. Later
// configuration writes belong to a future sweep. Primary configuration is kept.
type CompactJobConfigurationReceipts struct {
	TenantID, ID  string
	ExpectedStep  uint64
	BatchSize     int
	Actor, Reason string
}
type JobConfigurationReceiptCompaction struct {
	TenantID, ID                           string
	Step, Horizon                          uint64
	Actor, Reason                          string
	StartedAt, LastCleanupAt               time.Time
	ReceiptsCompacted, PayloadBytesRemoved uint64
	Complete                               bool
}
type JobConfigurationReceiptCompactionPage struct {
	Compactions []JobConfigurationReceiptCompaction
	NextCursor  []byte
}

// Only public acknowledgement fields survive; callers cannot treat a compact
// configuration/materialization receipt as an executable template or cursor.
type compactJobConfigurationReceipt struct {
	Kind     JobRecordKind
	ID       string
	Revision uint64
	Created  int
}

var (
	jobConfigurationReceiptVersionKey        = []byte("job_configuration_receipt_state_version")
	jobConfigurationReceiptIndexBucket       = []byte("job_configuration_receipt_index")
	jobConfigurationReceiptCountersBucket    = []byte("job_configuration_receipt_counters")
	jobConfigurationReceiptCompactionsBucket = []byte("job_configuration_receipt_compactions")
	jobConfigurationReceiptPendingBucket     = []byte("job_configuration_receipt_pending")
	jobConfigurationReceiptBuckets           = [][]byte{jobConfigurationReceiptIndexBucket, jobConfigurationReceiptCountersBucket, jobConfigurationReceiptCompactionsBucket, jobConfigurationReceiptPendingBucket}
)

func configurationReceiptFor(r StateResult) (*compactJobReceipt, error) {
	if r.Rejection != nil || r.Compacted || r.Job == nil {
		return nil, nil
	}
	j := r.Job
	if j.ConfigurationRetirement != nil || j.TriggerRetirement != nil || j.Run != nil || j.Action != nil || j.Event != nil || j.Activation != nil || j.Resource != nil || j.Deletion != nil || j.ReceiptCompaction != nil || j.ConfigurationReceiptCompaction != nil {
		return nil, nil
	}
	c := &compactJobReceipt{RequestID: r.RequestID, Fingerprint: r.Fingerprint}
	a := &compactJobConfigurationReceipt{Created: j.Created}
	count := 0
	set := func(kind JobRecordKind, tenant, id string, revision uint64) {
		count++
		c.TenantID = tenant
		a.Kind, a.ID, a.Revision = kind, id, revision
	}
	if v := j.Tenant; v != nil {
		set(JobTenantRecord, v.ID, v.ID, v.Revision)
	}
	if v := j.Group; v != nil {
		set(JobGroupRecord, v.TenantID, v.ID, v.Revision)
	}
	if v := j.Definition; v != nil {
		set(JobDefinitionRecord, v.TenantID, v.ID, v.Revision)
	}
	if v := j.Binding; v != nil {
		set(JobBindingRecord, v.TenantID, v.ID, v.Revision)
	}
	if v := j.Grant; v != nil {
		set(JobGrantRecord, v.TenantID, v.ResourceID, v.Revision)
	}
	if v := j.Trigger; v != nil {
		set(JobTriggerRecord, v.TenantID, v.ID, v.Revision)
	}
	if v := j.Limits; v != nil {
		set(JobLimitsRecord, v.TenantID, v.TenantID, v.Revision)
	}
	if v := j.ConfigurationLimits; v != nil {
		set(JobConfigurationLimitsRecord, v.TenantID, v.TenantID, v.Revision)
	}
	if v := j.Retention; v != nil {
		set(JobRetentionRecord, v.TenantID, v.TenantID, v.Revision)
	}
	if count == 0 {
		return nil, nil
	}
	if count != 1 || !validIdentity(c.TenantID) || !validIdentity(a.ID) || a.Revision == 0 || !validIdentity(c.RequestID) {
		return nil, fmt.Errorf("inconsistent configuration receipt identity")
	}
	c.Configuration = a
	return c, nil
}
func (a *compactJobConfigurationReceipt) result(tenant string) *JobResult {
	j := &JobResult{Created: a.Created}
	switch a.Kind {
	case JobTenantRecord:
		j.Tenant = &JobTenant{ID: tenant, Revision: a.Revision}
	case JobGroupRecord:
		j.Group = &JobGroup{TenantID: tenant, ID: a.ID, Revision: a.Revision}
	case JobDefinitionRecord:
		j.Definition = &JobDefinition{TenantID: tenant, ID: a.ID, Revision: a.Revision}
	case JobBindingRecord:
		j.Binding = &JobBinding{TenantID: tenant, ID: a.ID, Revision: a.Revision}
	case JobGrantRecord:
		j.Grant = &ResourceGrant{TenantID: tenant, ResourceID: a.ID, Revision: a.Revision}
	case JobTriggerRecord:
		j.Trigger = &JobTrigger{JobTriggerDefinition: JobTriggerDefinition{TenantID: tenant, ID: a.ID}, Revision: a.Revision}
	case JobLimitsRecord:
		j.Limits = &JobLimits{TenantID: tenant, Revision: a.Revision}
	case JobConfigurationLimitsRecord:
		j.ConfigurationLimits = &JobConfigurationLimits{TenantID: tenant, Revision: a.Revision}
	case JobRetentionRecord:
		j.Retention = &JobRetentionPolicy{TenantID: tenant, Revision: a.Revision}
	}
	return j
}

// Frozen v15 accounting: raw definition inputs, binding configs, and pinned
// trigger copies. Local Gob sizes are not deterministic across writer schemas.
func configurationReceiptPayloadBytes(j *JobResult) uint64 {
	var n uint64
	if v := j.Definition; v != nil {
		n += uint64(len(v.Input))
	}
	if v := j.Binding; v != nil {
		n += uint64(len(v.Config))
	}
	if v := j.Trigger; v != nil {
		n += uint64(len(v.Template.Definition.Input))
		if v.Template.Binding != nil {
			n += uint64(len(v.Template.Binding.Config))
		}
	}
	return n
}
func configurationReceiptIndexKey(tenant string, sequence uint64) []byte {
	return binary.BigEndian.AppendUint64([]byte(jobKey(tenant)), sequence)
}
func configurationReceiptSequence(tx *bbolt.Tx, tenant string) (uint64, error) {
	data := tx.Bucket(jobConfigurationReceiptCountersBucket).Get([]byte(tenant))
	if data == nil {
		return 0, nil
	}
	if len(data) != 8 {
		return 0, fmt.Errorf("invalid configuration receipt counter")
	}
	return binary.BigEndian.Uint64(data), nil
}
func indexConfigurationReceipt(tx *bbolt.Tx, r StateResult) error {
	c, err := configurationReceiptFor(r)
	if err != nil || c == nil {
		return err
	}
	sequence, err := configurationReceiptSequence(tx, c.TenantID)
	if err != nil {
		return err
	}
	if sequence == math.MaxUint64 {
		return fmt.Errorf("configuration receipt sequence exhausted")
	}
	sequence++
	if err := tx.Bucket(jobConfigurationReceiptCountersBucket).Put([]byte(c.TenantID), binary.BigEndian.AppendUint64(nil, sequence)); err != nil {
		return err
	}
	return tx.Bucket(jobConfigurationReceiptIndexBucket).Put(configurationReceiptIndexKey(c.TenantID, sequence), []byte(c.RequestID))
}
func initializeJobConfigurationReceiptState(tx *bbolt.Tx) error {
	meta := tx.Bucket(metadataBucket)
	version := meta.Get(jobConfigurationReceiptVersionKey)
	if version != nil && !bytes.Equal(version, []byte{1}) {
		return fmt.Errorf("unsupported configuration receipt state version")
	}
	activated := meta.Get(formatKey)[0] >= 9
	if activated && version == nil {
		return fmt.Errorf("configuration receipt state is missing its format marker")
	}
	for _, name := range jobConfigurationReceiptBuckets {
		b := tx.Bucket(name)
		if version != nil && b == nil {
			return fmt.Errorf("configuration receipt state is missing bucket %q", name)
		}
		if !activated && (bytes.Equal(name, jobConfigurationReceiptCompactionsBucket) || bytes.Equal(name, jobConfigurationReceiptPendingBucket)) && b != nil && b.Stats().KeyN != 0 {
			return fmt.Errorf("configuration receipt compaction without an activated database format")
		}
		if _, err := tx.CreateBucketIfNotExists(name); err != nil {
			return err
		}
	}
	if !activated {
		// v8-v14 writers ignore this derived index. Rebuild every open/restore until
		// activation excludes them. Lexical request order gives a stable rebuild.
		for _, name := range [][]byte{jobConfigurationReceiptIndexBucket, jobConfigurationReceiptCountersBucket} {
			if err := tx.DeleteBucket(name); err != nil {
				return err
			}
			if _, err := tx.CreateBucket(name); err != nil {
				return err
			}
		}
		if err := tx.Bucket(requestsBucket).ForEach(func(k, data []byte) error {
			var r StateResult
			if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&r); err != nil {
				return err
			}
			if r.RequestID != string(k) {
				return fmt.Errorf("inconsistent configuration receipt key")
			}
			return indexConfigurationReceipt(tx, r)
		}); err != nil {
			return err
		}
	}
	return meta.Put(jobConfigurationReceiptVersionKey, []byte{1})
}
func compactJobConfigurationReceipts(tx *bbolt.Tx, s *jobChanges, command StateCommand, c CompactJobConfigurationReceipts, r *JobResult) error {
	if !validIdentity(c.ID) {
		return reject("invalid", "configuration receipt compaction ID is required")
	}
	if _, err := requireJobTenant(tx, c.TenantID); err != nil {
		return err
	}
	if err := validResolution(c.Actor, c.Reason); err != nil {
		return err
	}
	if c.BatchSize == 0 {
		c.BatchSize = MaxJobPurgeBatch
	}
	if c.BatchSize < 1 || c.BatchSize > MaxJobPurgeBatch {
		return reject("invalid", "configuration receipt compaction batch must be 1..128")
	}
	key := jobKey(c.TenantID, c.ID)
	progress := &JobConfigurationReceiptCompaction{TenantID: c.TenantID, ID: c.ID, Actor: c.Actor, Reason: c.Reason, StartedAt: command.At}
	if tx.Bucket(jobConfigurationReceiptCompactionsBucket).Get([]byte(key)) != nil {
		var err error
		progress, err = readRecord[JobConfigurationReceiptCompaction](tx, jobConfigurationReceiptCompactionsBucket, key)
		if err != nil {
			return err
		}
		if progress.TenantID != c.TenantID || progress.ID != c.ID {
			return fmt.Errorf("inconsistent configuration compaction identity")
		}
	} else {
		var err error
		progress.Horizon, err = configurationReceiptSequence(tx, c.TenantID)
		if err != nil {
			return err
		}
	}
	if c.ExpectedStep != progress.Step {
		return reject("step_conflict", "configuration receipt compaction step changed")
	}
	if progress.Complete {
		r.ConfigurationReceiptCompaction = progress
		return nil
	}
	if command.At.Before(progress.LastCleanupAt) {
		return reject("stale_time", "configuration receipt compaction predates prior batch")
	}
	if progress.Step == math.MaxUint64 {
		return reject("exhausted", "configuration receipt compaction step exhausted")
	}
	prefix := []byte(jobKey(c.TenantID))
	cursor := tx.Bucket(jobConfigurationReceiptIndexBucket).Cursor()
	removed := 0
	var batchBytes uint64
	remaining := false
	for k, v := cursor.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = cursor.Next() {
		if len(k) != len(prefix)+8 {
			return fmt.Errorf("invalid configuration receipt index key")
		}
		sequence := binary.BigEndian.Uint64(k[len(prefix):])
		if sequence == 0 {
			return fmt.Errorf("invalid configuration receipt sequence")
		}
		if sequence > progress.Horizon {
			break
		}
		if removed == c.BatchSize {
			remaining = true
			break
		}
		data := tx.Bucket(requestsBucket).Get(v)
		if data == nil {
			return fmt.Errorf("configuration index refers to missing full receipt")
		}
		var full StateResult
		if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&full); err != nil {
			return err
		}
		compact, err := configurationReceiptFor(full)
		if err != nil {
			return err
		}
		if compact == nil || compact.TenantID != c.TenantID || compact.RequestID != string(v) || tx.Bucket(jobCompactReceiptsBucket).Get(v) != nil {
			return fmt.Errorf("inconsistent configuration receipt index")
		}
		size := configurationReceiptPayloadBytes(full.Job)
		if removed > 0 && (size > MaxJobReceiptBatchPayloadBytes || batchBytes > MaxJobReceiptBatchPayloadBytes-size) {
			remaining = true
			break
		}
		if math.MaxUint64-progress.PayloadBytesRemoved < size || progress.ReceiptsCompacted == math.MaxUint64 {
			return fmt.Errorf("configuration receipt compaction counters overflow")
		}
		batchBytes += size
		progress.PayloadBytesRemoved += size
		progress.ReceiptsCompacted++
		s.put(jobCompactReceiptsBucket, string(v), storedCompactReceipt(compact))
		s.del(requestsBucket, string(v))
		s.del(jobConfigurationReceiptIndexBucket, string(k))
		removed++
	}
	progress.Step++
	progress.LastCleanupAt, progress.Complete = command.At, !remaining
	s.put(jobConfigurationReceiptCompactionsBucket, key, progress)
	if remaining {
		s.index(jobConfigurationReceiptPendingBucket, key, []byte(key))
	} else {
		s.del(jobConfigurationReceiptPendingBucket, key)
	}
	r.ConfigurationReceiptCompaction = progress
	return nil
}
func (q *DB) GetJobConfigurationReceiptCompaction(tenant, id string) (*JobConfigurationReceiptCompaction, error) {
	return getStateRecord[JobConfigurationReceiptCompaction](q, jobConfigurationReceiptCompactionsBucket, jobKey(tenant, id))
}
func (q *DB) ListPendingJobConfigurationReceiptCompactions(after []byte, limit int) (JobConfigurationReceiptCompactionPage, error) {
	page := JobConfigurationReceiptCompactionPage{}
	if err := queryLimit(limit); err != nil {
		return page, err
	}
	err := q.db.View(func(tx *bbolt.Tx) error {
		cursor := tx.Bucket(jobConfigurationReceiptPendingBucket).Cursor()
		for k, v := pageStart(cursor, after); k != nil && len(page.Compactions) < limit; k, v = cursor.Next() {
			c, err := readRecord[JobConfigurationReceiptCompaction](tx, jobConfigurationReceiptCompactionsBucket, string(v))
			if err != nil {
				return err
			}
			if c.Complete || string(k) != jobKey(c.TenantID, c.ID) || !bytes.Equal(k, v) {
				return fmt.Errorf("inconsistent configuration receipt pending index")
			}
			page.Compactions = append(page.Compactions, *c)
			page.NextCursor = bytes.Clone(k)
		}
		return nil
	})
	return page, err
}
