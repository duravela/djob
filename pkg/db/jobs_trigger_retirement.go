package db

import (
	"bytes"
	"fmt"
	"time"

	"go.etcd.io/bbolt"
)

// RetireJobTrigger removes current activation configuration. Existing runs and
// their pinned templates, pending slots, effects and reservations are untouched.
type RetireJobTrigger struct {
	TenantID, ID     string
	ExpectedRevision uint64
	Actor, Reason    string
}

// JobTriggerRetirement permanently reserves a retired trigger identity. Reusing
// it could confuse delayed planners, event deduplication and historical receipts.
// No application input, binding or execution credentials are retained here.
type JobTriggerRetirement struct {
	TenantID, ID  string
	Revision      uint64
	RetiredAt     time.Time
	Actor, Reason string
}

var (
	jobTriggerRetirementVersionKey = []byte("job_trigger_retirement_state_version")
	jobTriggerRetirementsBucket    = []byte("job_trigger_retirements")
)

func initializeJobTriggerRetirementState(tx *bbolt.Tx) error {
	meta := tx.Bucket(metadataBucket)
	version := meta.Get(jobTriggerRetirementVersionKey)
	activated := meta.Get(formatKey)[0] >= 10
	if version != nil && !bytes.Equal(version, []byte{1}) {
		return fmt.Errorf("unsupported trigger retirement state version")
	}
	if version == nil && activated {
		return fmt.Errorf("trigger retirement state is missing its format marker")
	}
	b := tx.Bucket(jobTriggerRetirementsBucket)
	if version != nil && b == nil {
		return fmt.Errorf("trigger retirement state is missing its bucket")
	}
	if !activated && b != nil && b.Stats().KeyN != 0 {
		return fmt.Errorf("trigger retirement data without an activated database format")
	}
	if _, err := tx.CreateBucketIfNotExists(jobTriggerRetirementsBucket); err != nil {
		return err
	}
	return meta.Put(jobTriggerRetirementVersionKey, []byte{1})
}

func retireJobTrigger(tx *bbolt.Tx, changes *jobChanges, at time.Time, c RetireJobTrigger, result *JobResult) error {
	if !validIdentity(c.TenantID) || !validIdentity(c.ID) || c.ExpectedRevision == 0 {
		return reject("invalid", "trigger retirement requires tenant, ID and a positive expected revision")
	}
	if err := validResolution(c.Actor, c.Reason); err != nil {
		return err
	}
	key := jobKey(c.TenantID, c.ID)
	if tx.Bucket(jobTriggerRetirementsBucket).Get([]byte(key)) != nil {
		return reject("gone", "trigger identity has been permanently retired")
	}
	trigger, err := readRecord[JobTrigger](tx, jobTriggersBucket, key)
	if err != nil {
		return err
	}
	if trigger.Revision != c.ExpectedRevision {
		return reject("revision_conflict", "trigger revision changed")
	}
	if at.Before(trigger.LastTransition) {
		return reject("stale_time", "retirement predates the trigger's last transition")
	}
	if trigger.Event == nil && !trigger.Paused && !trigger.Completed {
		changes.del(jobTriggerDueBucket, string(timedKey(trigger.NextAt, key)))
	}
	retirement := &JobTriggerRetirement{TenantID: c.TenantID, ID: c.ID, Revision: trigger.Revision, RetiredAt: at, Actor: c.Actor, Reason: c.Reason}
	changes.put(jobTriggerRetirementsBucket, key, retirement)
	changes.del(jobTriggersBucket, key)
	result.TriggerRetirement = retirement
	return nil
}

func (q *DB) GetJobTriggerRetirement(tenant, id string) (*JobTriggerRetirement, error) {
	return getStateRecord[JobTriggerRetirement](q, jobTriggerRetirementsBucket, jobKey(tenant, id))
}
