package db

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"time"

	"go.etcd.io/bbolt"
)

const MaxJobPurgeBatch = 128

type JobRetentionPolicy struct {
	TenantID string
	Revision uint64
	Enabled  bool
	KeepFor  time.Duration
}
type PutJobRetention struct {
	Policy           JobRetentionPolicy
	ExpectedRevision uint64
}
type PurgeJob struct {
	TenantID, RunID string
	ExpectedStep    uint64
	FinishedBefore  time.Time
	BatchSize       int
	Actor, Reason   string
	PolicyRevision  uint64 `json:",omitempty"` // Nonzero only for a controller's new automatic deletion.
}

// JobDeletion is a permanent identity/audit record. Cleanup removes primary run
// history in bounded transactions; it never authorizes run ID reuse.
type JobDeletion struct {
	TenantID, RunID                                 string
	Step                                            uint64
	Status                                          JobStatus
	DefinitionID                                    string
	DefinitionRevision                              uint64
	TriggerID                                       string
	TriggerRevision                                 uint64
	Resources                                       []string
	CreatedAt, FinishedAt, DeletedAt, LastCleanupAt time.Time
	Actor, Reason                                   string
	PolicyRevision                                  uint64
	RecordsDeleted                                  uint64
	CleanupComplete                                 bool
}

type JobPurgeCandidate struct {
	TenantID, RunID      string
	Step, PolicyRevision uint64
	FinishedBefore       time.Time
}
type JobPurgePage struct {
	Candidates []JobPurgeCandidate
	NextCursor []byte
}
type JobDeletionPage struct {
	Deletions  []JobDeletion
	NextCursor []byte
}

var (
	jobRetentionVersionKey      = []byte("job_retention_state_version")
	jobRetentionBucket          = []byte("job_retention")
	jobDeletionBucket           = []byte("job_deletions")
	jobActivationReceiptsBucket = []byte("job_activation_receipts")
	jobRetentionIndexBucket     = []byte("job_retention_index")
	jobPurgePendingBucket       = []byte("job_purge_pending")
	jobRetentionBuckets         = [][]byte{jobRetentionBucket, jobDeletionBucket, jobActivationReceiptsBucket, jobRetentionIndexBucket, jobPurgePendingBucket}
)

func knownTerminalJob(run *JobRun) bool {
	return run.Status == JobSucceeded || run.Status == JobFailed || run.Status == JobCancelled
}
func retentionIndexKey(run *JobRun) string {
	return string(timedKey(run.LastTransition, jobKey(run.TenantID, run.ID)))
}

func initializeJobRetentionState(tx *bbolt.Tx) error {
	meta := tx.Bucket(metadataBucket)
	version := meta.Get(jobRetentionVersionKey)
	if version != nil && !bytes.Equal(version, []byte{1}) {
		return fmt.Errorf("unsupported job retention state version")
	}
	if version == nil && meta.Get(formatKey)[0] >= 6 {
		return fmt.Errorf("job retention state is missing its format marker")
	}
	for _, name := range jobRetentionBuckets {
		if version != nil && tx.Bucket(name) == nil {
			return fmt.Errorf("job retention state is missing bucket %q", name)
		}
		if meta.Get(formatKey)[0] < 6 && !bytes.Equal(name, jobRetentionIndexBucket) && tx.Bucket(name) != nil && tx.Bucket(name).Stats().KeyN != 0 {
			return fmt.Errorf("job retention data without an activated database format")
		}
		if _, err := tx.CreateBucketIfNotExists(name); err != nil {
			return err
		}
	}
	if meta.Get(formatKey)[0] < 6 {
		// Earlier writers do not maintain this derived index. Their snapshots can
		// contain a stale copy, so rebuild until format 6 excludes those writers.
		if err := tx.DeleteBucket(jobRetentionIndexBucket); err != nil {
			return err
		}
		if _, err := tx.CreateBucket(jobRetentionIndexBucket); err != nil {
			return err
		}
		if err := tx.Bucket(jobRunsBucket).ForEach(func(k, data []byte) error {
			run, err := quotaValue[JobRun](data, nil)
			if err != nil {
				return err
			}
			if knownTerminalJob(run) {
				return tx.Bucket(jobRetentionIndexBucket).Put([]byte(retentionIndexKey(run)), k)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return meta.Put(jobRetentionVersionKey, []byte{1})
}

func retentionPolicyInTx(tx *bbolt.Tx, tenant string) (*JobRetentionPolicy, error) {
	if tx.Bucket(jobRetentionBucket).Get([]byte(tenant)) == nil {
		return &JobRetentionPolicy{TenantID: tenant}, nil
	}
	v, err := readRecord[JobRetentionPolicy](tx, jobRetentionBucket, tenant)
	if err == nil && v.TenantID != tenant {
		return nil, fmt.Errorf("inconsistent job retention tenant")
	}
	return v, err
}
func putJobRetention(tx *bbolt.Tx, s *jobChanges, c PutJobRetention, r *JobResult) error {
	v := c.Policy
	if _, err := requireJobTenant(tx, v.TenantID); err != nil {
		return err
	}
	if v.KeepFor < 0 || (v.Enabled && v.KeepFor <= 0) {
		return reject("invalid", "enabled retention requires a positive keep duration")
	}
	old, err := retentionPolicyInTx(tx, v.TenantID)
	if err != nil {
		return err
	}
	if old.Revision != c.ExpectedRevision || old.Revision == math.MaxUint64 {
		return reject("revision_conflict", "retention policy revision changed")
	}
	v.Revision = old.Revision + 1
	s.put(jobRetentionBucket, v.TenantID, &v)
	r.Retention = &v
	return nil
}

func purgeableJob(tx *bbolt.Tx, run *JobRun) error {
	if !knownTerminalJob(run) || run.OwnerID != "" || run.AttemptID != "" || !run.LeaseUntil.IsZero() {
		return reject("job_protected", "only known terminal runs without ownership can be purged")
	}
	if run.PendingAction != nil && (run.PendingAction.Status == EffectAuthorized || run.PendingAction.Status == EffectUnknown) {
		return reject("job_protected", "uncertain action evidence must be retained")
	}
	if run.ActionCount > 0 {
		action, err := readRecord[JobAction](tx, jobActionsBucket, jobActionKey(run.TenantID, run.ID, run.ActionCount))
		if err != nil {
			return err
		}
		if action.Status != EffectSucceeded && action.Status != EffectFailed {
			return reject("job_protected", "uncertain action history must be retained")
		}
	}
	for _, id := range run.Resources {
		resource, err := readRecord[JobResource](tx, jobResourcesBucket, id)
		if err != nil {
			return err
		}
		if resource.TenantID == run.TenantID && resource.RunID == run.ID && (resource.State == ResourceOwned || resource.State == ResourceBlocked) {
			return reject("job_protected", "run still has a held or blocked resource")
		}
	}
	return nil
}

func activationReceipt(v *JobActivation) *JobActivation {
	out := cloneJobActivation(v)
	digest := sha256.Sum256(v.Data)
	out.Data = nil
	out.DataDeleted = true
	out.DataSHA256 = append([]byte(nil), digest[:]...)
	return out
}
func sameActivationData(v *JobActivation, data []byte) bool {
	if !v.DataDeleted {
		return bytes.Equal(v.Data, data)
	}
	digest := sha256.Sum256(data)
	return bytes.Equal(v.DataSHA256, digest[:])
}

func purgeJob(tx *bbolt.Tx, s *jobChanges, command StateCommand, c PurgeJob, r *JobResult) error {
	if !validIdentity(c.TenantID) || !validIdentity(c.RunID) || !validDeadline(c.FinishedBefore) {
		return reject("invalid", "purge requires tenant, run and a supported finished cutoff")
	}
	if c.FinishedBefore.After(command.At) {
		return reject("invalid", "purge cutoff cannot be after its command time")
	}
	if err := validResolution(c.Actor, c.Reason); err != nil {
		return err
	}
	if c.BatchSize == 0 {
		c.BatchSize = MaxJobPurgeBatch
	}
	if c.BatchSize < 1 || c.BatchSize > MaxJobPurgeBatch {
		return reject("invalid", "purge batch must be 1..128 records")
	}
	key := jobKey(c.TenantID, c.RunID)
	var deletion *JobDeletion
	if tx.Bucket(jobDeletionBucket).Get([]byte(key)) != nil {
		var err error
		deletion, err = readRecord[JobDeletion](tx, jobDeletionBucket, key)
		if err != nil {
			return err
		}
		if deletion.Step != c.ExpectedStep {
			return reject("step_conflict", "purged run checkpoint differs")
		}
		if deletion.CleanupComplete {
			r.Deletion = deletion
			return nil
		}
		if command.At.Before(deletion.LastCleanupAt) {
			return reject("stale_time", "cleanup predates the prior batch")
		}
	} else {
		run, err := readRecord[JobRun](tx, jobRunsBucket, key)
		if err != nil {
			return err
		}
		if run.Step != c.ExpectedStep {
			return reject("step_conflict", "run checkpoint changed")
		}
		if command.At.Before(run.LastTransition) || run.LastTransition.After(c.FinishedBefore) {
			return reject("not_due", "run is newer than the deletion cutoff")
		}
		if err := purgeableJob(tx, run); err != nil {
			return err
		}
		if c.PolicyRevision != 0 {
			policy, err := retentionPolicyInTx(tx, c.TenantID)
			if err != nil {
				return err
			}
			if !policy.Enabled || policy.Revision != c.PolicyRevision {
				return reject("revision_conflict", "retention policy changed")
			}
			if !c.FinishedBefore.Equal(command.At.Add(-policy.KeepFor)) {
				return reject("invalid", "automatic purge cutoff does not match retention policy")
			}
		}
		deletion = &JobDeletion{TenantID: run.TenantID, RunID: run.ID, Step: run.Step, Status: run.Status, DefinitionID: run.Definition.ID, DefinitionRevision: run.Definition.Revision, TriggerID: run.TriggerID, TriggerRevision: run.TriggerRevision, Resources: append([]string(nil), run.Resources...), CreatedAt: run.CreatedAt, FinishedAt: run.LastTransition, DeletedAt: command.At, Actor: c.Actor, Reason: c.Reason, PolicyRevision: c.PolicyRevision, RecordsDeleted: 1}
		s.del(jobRunsBucket, key)
		s.del(jobRetentionIndexBucket, retentionIndexKey(run))
		// Run-scoped inbox IDs cannot be reused once the permanent run tombstone
		// exists. Activation IDs are external and retain a compact payload digest.
		if run.Activation != nil {
			a := run.Activation
			activationID := activationKey(a.TenantID, a.TriggerID, a.Source, a.EventID)
			accepted, err := readRecord[JobActivation](tx, jobActivationsBucket, activationID)
			if err != nil {
				return err
			}
			if accepted.RunID != run.ID {
				return fmt.Errorf("activation does not belong to purged run")
			}
			s.put(jobActivationReceiptsBucket, activationID, activationReceipt(accepted))
			s.del(jobActivationsBucket, activationID)
			deletion.RecordsDeleted++
		}
	}
	prefix := []byte(jobKey(c.TenantID, c.RunID))
	removed := 0
	remaining := false
	for _, bucket := range [][]byte{jobActionsBucket, jobAttemptsBucket, jobEventKeysBucket, jobEventsBucket} {
		cursor := tx.Bucket(bucket).Cursor()
		for k, _ := cursor.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = cursor.Next() {
			if removed == c.BatchSize {
				remaining = true
				break
			}
			s.del(bucket, string(k))
			removed++
		}
		if remaining {
			break
		}
	}
	if math.MaxUint64-deletion.RecordsDeleted < uint64(removed) {
		return fmt.Errorf("purge count overflow")
	}
	deletion.RecordsDeleted += uint64(removed)
	deletion.LastCleanupAt = command.At
	deletion.CleanupComplete = !remaining
	s.put(jobDeletionBucket, key, deletion)
	if remaining {
		s.index(jobPurgePendingBucket, key, []byte(key))
	} else {
		s.del(jobPurgePendingBucket, key)
	}
	r.Deletion = deletion
	return nil
}

func cloneJobDeletion(v *JobDeletion) *JobDeletion {
	out := jobCopy(v)
	if out != nil {
		out.Resources = append([]string(nil), v.Resources...)
	}
	return out
}
func (q *DB) GetJobDeletion(tenant, run string) (*JobDeletion, error) {
	return getStateRecord[JobDeletion](q, jobDeletionBucket, jobKey(tenant, run))
}
func (q *DB) GetJobRetention(tenant string) (*JobRetentionPolicy, error) {
	var v *JobRetentionPolicy
	err := q.db.View(func(tx *bbolt.Tx) error {
		if _, err := requireJobTenant(tx, tenant); err != nil {
			return err
		}
		var err error
		v, err = retentionPolicyInTx(tx, tenant)
		return err
	})
	return v, err
}

func (q *DB) ListPendingJobPurges(after []byte, limit int) (JobDeletionPage, error) {
	page := JobDeletionPage{}
	if err := queryLimit(limit); err != nil {
		return page, err
	}
	err := q.db.View(func(tx *bbolt.Tx) error {
		cursor := tx.Bucket(jobPurgePendingBucket).Cursor()
		for k, v := pageStart(cursor, after); k != nil && len(page.Deletions) < limit; k, v = cursor.Next() {
			d, err := readRecord[JobDeletion](tx, jobDeletionBucket, string(v))
			if err != nil {
				return err
			}
			if d.CleanupComplete || string(k) != jobKey(d.TenantID, d.RunID) {
				return fmt.Errorf("inconsistent purge pending index")
			}
			page.Deletions = append(page.Deletions, *d)
			page.NextCursor = bytes.Clone(k)
		}
		return nil
	})
	return page, err
}

// The limit bounds scanned index rows, including disabled/protected candidates.
// Keeping that cursor lets eligible tenants make progress across sparse pages.
func (q *DB) ListJobRetentionCandidates(at time.Time, after []byte, limit int) (JobPurgePage, error) {
	page := JobPurgePage{}
	if err := queryLimit(limit); err != nil {
		return page, err
	}
	err := q.db.View(func(tx *bbolt.Tx) error {
		if key, _ := tx.Bucket(jobRetentionBucket).Cursor().First(); key == nil {
			return nil
		}
		cursor := tx.Bucket(jobRetentionIndexBucket).Cursor()
		scanned := 0
		for k, v := pageStart(cursor, after); k != nil && scanned < limit; k, v = cursor.Next() {
			if len(k) < 12 {
				return fmt.Errorf("invalid retention index key")
			}
			if bytes.Compare(k[:12], orderedTime(at)) > 0 {
				break
			}
			scanned++
			page.NextCursor = bytes.Clone(k)
			run, err := readRecord[JobRun](tx, jobRunsBucket, string(v))
			if err != nil {
				return err
			}
			if !knownTerminalJob(run) || string(k) != retentionIndexKey(run) {
				return fmt.Errorf("inconsistent retention index")
			}
			policy, err := retentionPolicyInTx(tx, run.TenantID)
			if err != nil {
				return err
			}
			if !policy.Enabled {
				continue
			}
			cutoff := at.Add(-policy.KeepFor).UTC()
			if !validDeadline(cutoff) || run.LastTransition.After(cutoff) {
				continue
			}
			if err := purgeableJob(tx, run); err != nil {
				var rejected *CommandRejection
				if errors.As(err, &rejected) && rejected.Code == "job_protected" {
					continue
				}
				return err
			}
			page.Candidates = append(page.Candidates, JobPurgeCandidate{TenantID: run.TenantID, RunID: run.ID, Step: run.Step, PolicyRevision: policy.Revision, FinishedBefore: cutoff})
		}
		return nil
	})
	return page, err
}
