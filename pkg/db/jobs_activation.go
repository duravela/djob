package db

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	djob "github.com/duravela/djob/sdk/go"
	"go.etcd.io/bbolt"
)

// EventTriggerSpec binds a trigger to one named producer source. Events address
// a trigger explicitly; they cannot select a handler, binding, or resource set.
type EventTriggerSpec struct{ Source string }

// JobActivation is the immutable public event input; persistence remains in db.
type JobActivation = djob.JobActivation

type ActivateJob struct {
	TenantID, TriggerID, Source, EventID string
	OccurredAt                           time.Time
	Data                                 []byte `json:",omitempty"`
}

var (
	jobActivationVersionKey = []byte("job_activation_state_version")
	jobActivationsBucket    = []byte("job_activations")
)

func initializeJobActivationState(tx *bbolt.Tx) error {
	meta := tx.Bucket(metadataBucket)
	version := meta.Get(jobActivationVersionKey)
	if version != nil && !bytes.Equal(version, []byte{1}) {
		return fmt.Errorf("unsupported job activation state version")
	}
	if version == nil && meta.Get(formatKey)[0] >= 4 {
		return fmt.Errorf("job activation state is missing its format marker")
	}
	b := tx.Bucket(jobActivationsBucket)
	if version != nil && b == nil {
		return fmt.Errorf("job activation state is missing its bucket")
	}
	if meta.Get(formatKey)[0] < 4 && b != nil && b.Stats().KeyN != 0 {
		return fmt.Errorf("job activation data without an activated database format")
	}
	if _, err := tx.CreateBucketIfNotExists(jobActivationsBucket); err != nil {
		return err
	}
	return meta.Put(jobActivationVersionKey, []byte{1})
}

func activationKey(tenant, trigger, source, event string) string {
	return jobKey(tenant, trigger, source, event)
}

func EventJobRunID(tenant, trigger, source, event string) string {
	sum := sha256.Sum256([]byte(jobKey("djob:event/v1", tenant, trigger, source, event)))
	return "activated/" + hex.EncodeToString(sum[:])
}

func activateJob(tx *bbolt.Tx, s *jobChanges, at time.Time, c ActivateJob, r *JobResult) error {
	if !validIdentity(c.TenantID) || !validIdentity(c.TriggerID) || !validIdentity(c.Source) || !validIdentity(c.EventID) || !validDeadline(c.OccurredAt) || len(c.Data) > 64<<10 {
		return reject("invalid", "activation requires tenant, trigger, source, event ID, supported occurrence time and at most 64 KiB of data")
	}
	key := activationKey(c.TenantID, c.TriggerID, c.Source, c.EventID)
	for _, bucket := range [][]byte{jobActivationsBucket, jobActivationReceiptsBucket} {
		if tx.Bucket(bucket).Get([]byte(key)) == nil {
			continue
		}
		accepted, err := readRecord[JobActivation](tx, bucket, key)
		if err != nil {
			return err
		}
		if !accepted.OccurredAt.Equal(c.OccurredAt) || !sameActivationData(accepted, c.Data) {
			return reject("activation_conflict", "event identity was already accepted with different data or occurrence time")
		}
		r.Activation = accepted
		return nil
	}
	trigger, err := readRecord[JobTrigger](tx, jobTriggersBucket, jobKey(c.TenantID, c.TriggerID))
	if err != nil {
		return err
	}
	if trigger.Event == nil || trigger.Event.Source != c.Source {
		return reject("source_mismatch", "event source does not match this trigger")
	}
	if trigger.Paused {
		return reject("trigger_inactive", "event trigger is paused")
	}
	if at.Before(trigger.LastTransition) {
		return reject("stale_time", "activation predates the trigger configuration")
	}
	if err := checkJobGrants(tx, c.TenantID, trigger.Template.Resources); err != nil {
		return err
	}
	all, own, err := scheduledPendingCounts(tx, c.TenantID, trigger.ID)
	if err != nil {
		return err
	}
	if all >= MaxTenantScheduledPending || own >= trigger.MaxPending {
		return reject("backpressure", "triggered job pending limit reached; event was not accepted")
	}
	id := EventJobRunID(c.TenantID, c.TriggerID, c.Source, c.EventID)
	if tx.Bucket(jobDeletionBucket).Get([]byte(jobKey(c.TenantID, id))) != nil {
		return reject("gone", "activation run identity was purged")
	}
	if tx.Bucket(jobRunsBucket).Get([]byte(jobKey(c.TenantID, id))) != nil {
		return reject("run_exists", "activation run identity already exists")
	}
	activation := &JobActivation{TenantID: c.TenantID, TriggerID: c.TriggerID, Source: c.Source, EventID: c.EventID, RunID: id, TriggerRevision: trigger.Revision, OccurredAt: c.OccurredAt.UTC(), AcceptedAt: at, Data: bytes.Clone(c.Data)}
	if err := checkWorkerRouting(tx, c.TenantID, trigger.Template.Definition); err != nil {
		return err
	}
	run := trigger.Template.newRun(c.TenantID, id, at)
	run.TriggerID, run.TriggerRevision, run.Activation = trigger.ID, trigger.Revision, cloneJobActivation(activation)
	saveJobRun(s, nil, run)
	s.put(jobActivationsBucket, key, activation)
	r.Activation = activation
	return nil
}

func cloneJobActivation(v *JobActivation) *JobActivation {
	out := jobCopy(v)
	if out != nil {
		out.Data = bytes.Clone(v.Data)
		out.DataSHA256 = bytes.Clone(v.DataSHA256)
	}
	return out
}

func (q *DB) GetJobActivation(tenant, trigger, source, event string) (*JobActivation, error) {
	var result *JobActivation
	err := q.db.View(func(tx *bbolt.Tx) error {
		key := activationKey(tenant, trigger, source, event)
		bucket := jobActivationsBucket
		if tx.Bucket(bucket).Get([]byte(key)) == nil {
			bucket = jobActivationReceiptsBucket
		}
		var err error
		result, err = readRecord[JobActivation](tx, bucket, key)
		return err
	})
	return result, err
}
