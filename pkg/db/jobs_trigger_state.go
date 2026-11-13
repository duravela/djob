package db

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"go.etcd.io/bbolt"
)

var (
	jobTriggerVersionKey    = []byte("job_trigger_state_version")
	jobTriggersBucket       = []byte("job_triggers")
	jobTriggerDueBucket     = []byte("job_trigger_due")
	jobOccurrencesBucket    = []byte("job_occurrences")
	jobTriggerPendingBucket = []byte("job_trigger_pending")
	jobTriggerBuckets       = [][]byte{jobTriggersBucket, jobTriggerDueBucket, jobOccurrencesBucket, jobTriggerPendingBucket}
)

func initializeJobTriggerState(tx *bbolt.Tx) error {
	meta := tx.Bucket(metadataBucket)
	version := meta.Get(jobTriggerVersionKey)
	if version != nil && !bytes.Equal(version, []byte{1}) {
		return fmt.Errorf("unsupported generic trigger state version")
	}
	if version == nil && meta.Get(formatKey)[0] >= 3 {
		return fmt.Errorf("generic trigger state is missing its format marker")
	}
	for _, name := range jobTriggerBuckets {
		b := tx.Bucket(name)
		if version != nil && b == nil {
			return fmt.Errorf("generic trigger state is missing bucket %q", name)
		}
		if meta.Get(formatKey)[0] < 3 && b != nil && b.Stats().KeyN != 0 {
			return fmt.Errorf("generic trigger data without an activated database format")
		}
		if _, err := tx.CreateBucketIfNotExists(name); err != nil {
			return err
		}
	}
	return meta.Put(jobTriggerVersionKey, []byte{1})
}

func saveJobTrigger(s *jobChanges, before, after *JobTrigger) {
	key := jobKey(after.TenantID, after.ID)
	if before != nil && before.Event == nil && !before.Paused && !before.Completed {
		s.del(jobTriggerDueBucket, string(timedKey(before.NextAt, key)))
	}
	s.put(jobTriggersBucket, key, after)
	if after.Event == nil && !after.Paused && !after.Completed {
		s.index(jobTriggerDueBucket, string(timedKey(after.NextAt, key)), []byte(key))
	}
}

// Unknown effects remain pending until an operator resolves them. Repeated
// time-based activation cannot escape a blocked device or uncertain result.
func scheduledJobPending(run *JobRun) bool {
	return run.Status != JobSucceeded && run.Status != JobFailed && run.Status != JobCancelled
}

func putJobTrigger(tx *bbolt.Tx, s *jobChanges, at time.Time, c PutJobTrigger, r *JobResult) error {
	d := c.Definition
	if err := ValidateJobTrigger(d); err != nil {
		return err
	}
	if _, err := requireJobTenant(tx, d.TenantID); err != nil {
		return err
	}
	if d.Event != nil && !c.NextAt.IsZero() {
		return reject("invalid", "event trigger has no occurrence cursor")
	}
	if d.Event == nil && (c.NextAt.IsZero() || c.NextAt.Before(d.Timing.Spec.StartAt) || c.NextAt.Year() > 9999) {
		return reject("invalid", "initial occurrence must be at or after the trigger start")
	}
	if d.Timing.Spec.Kind == ScheduleOnce && !c.NextAt.Equal(d.Timing.Spec.StartAt) {
		return reject("invalid", "one-time occurrence must equal its start time")
	}
	if d.Timing.Spec.Kind == ScheduleInterval && !intervalOccurrence(d.Timing.Spec.StartAt, c.NextAt, d.Timing.Spec.Interval) {
		return reject("invalid", "interval occurrence must be anchored to the start")
	}
	key := jobKey(d.TenantID, d.ID)
	if tx.Bucket(jobTriggerRetirementsBucket).Get([]byte(key)) != nil {
		return reject("gone", "trigger identity has been permanently retired")
	}
	var before *JobTrigger
	if tx.Bucket(jobTriggersBucket).Get([]byte(key)) != nil {
		var err error
		before, err = readRecord[JobTrigger](tx, jobTriggersBucket, key)
		if err != nil {
			return err
		}
		if at.Before(before.LastTransition) {
			return reject("stale_time", "trigger update predates its last transition")
		}
		if d.Event == nil && (c.NextAt.Before(at) || (d.Timing.Spec.Kind != ScheduleOnce && !c.NextAt.After(at))) {
			return reject("invalid", "updated trigger must begin in the future")
		}
	}
	if err := jobRevision(tx, jobTriggersBucket, key, c.ExpectedRevision, func() (uint64, error) { return before.Revision, nil }); err != nil {
		return err
	}
	template, err := resolveJobTemplate(tx, d.TenantID, d.DefinitionID, d.DefinitionRevision)
	if err != nil {
		return err
	}
	d.Timing.Spec.StartAt = d.Timing.Spec.StartAt.UTC()
	trigger := &JobTrigger{JobTriggerDefinition: d, Revision: c.ExpectedRevision + 1, Template: *template, NextAt: c.NextAt.UTC(), LastTransition: at}
	saveJobTrigger(s, before, trigger)
	r.Trigger = trigger
	return nil
}

func pauseJobTrigger(tx *bbolt.Tx, s *jobChanges, at time.Time, c PauseJobTrigger, r *JobResult) error {
	trigger, err := readRecord[JobTrigger](tx, jobTriggersBucket, jobKey(c.TenantID, c.ID))
	if err != nil {
		return err
	}
	if trigger.Revision != c.ExpectedRevision {
		return reject("revision_conflict", "trigger revision changed")
	}
	if at.Before(trigger.LastTransition) {
		return reject("stale_time", "pause predates the trigger's last transition")
	}
	if trigger.Paused != c.Paused {
		if trigger.Revision == ^uint64(0) {
			return reject("exhausted", "trigger revision exhausted")
		}
		before := *trigger
		trigger.Revision++
		trigger.Paused, trigger.LastTransition = c.Paused, at
		saveJobTrigger(s, &before, trigger)
	}
	r.Trigger = trigger
	return nil
}

func scheduledPendingCounts(tx *bbolt.Tx, tenant, trigger string) (tenantCount, triggerCount int, err error) {
	prefix := []byte(jobKey(tenant))
	triggerPrefix := []byte(jobKey(tenant, trigger))
	c := tx.Bucket(jobTriggerPendingBucket).Cursor()
	for k, _ := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, _ = c.Next() {
		tenantCount++
		if bytes.HasPrefix(k, triggerPrefix) {
			triggerCount++
		}
		if tenantCount > MaxTenantScheduledPending {
			return 0, 0, fmt.Errorf("scheduled job pending index exceeds its tenant bound")
		}
	}
	return
}

func materializeJobs(tx *bbolt.Tx, s *jobChanges, at time.Time, c MaterializeJobs, r *JobResult) error {
	plan := c.Plan
	trigger, err := readRecord[JobTrigger](tx, jobTriggersBucket, jobKey(c.TenantID, plan.ScheduleID))
	if err != nil {
		return err
	}
	if trigger.Event != nil {
		return reject("invalid", "event trigger requires an event activation")
	}
	if plan.DeadlineAware || plan.RetryAware {
		return reject("invalid", "generic triggers do not use gateway execution profiles")
	}
	if err := validateTimingMaterialization(trigger.TimingCursor(), at, plan); err != nil {
		return err
	}
	if len(plan.Occurrences) != 0 {
		if err := checkWorkerRouting(tx, c.TenantID, trigger.Template.Definition); err != nil {
			return err
		}
		if err := checkJobGrants(tx, c.TenantID, trigger.Template.Resources); err != nil {
			return err
		}
		all, own, err := scheduledPendingCounts(tx, c.TenantID, trigger.ID)
		if err != nil {
			return err
		}
		if all+len(plan.Occurrences) > MaxTenantScheduledPending || own+len(plan.Occurrences) > trigger.MaxPending {
			return reject("backpressure", "scheduled job pending limit reached")
		}
	}
	for _, occurrence := range plan.Occurrences {
		id := JobOccurrenceRunID(c.TenantID, trigger.ID, trigger.Revision, occurrence)
		key := jobKey(c.TenantID, id)
		if tx.Bucket(jobRunsBucket).Get([]byte(key)) != nil || tx.Bucket(jobOccurrencesBucket).Get([]byte(key)) != nil || tx.Bucket(jobDeletionBucket).Get([]byte(key)) != nil {
			return reject("occurrence_conflict", "trigger occurrence already exists")
		}
		run := trigger.Template.newRun(c.TenantID, id, at)
		occurrence = occurrence.UTC()
		run.TriggerID, run.TriggerRevision, run.ScheduledAt = trigger.ID, trigger.Revision, &occurrence
		saveJobRun(s, nil, run)
		s.index(jobOccurrencesBucket, key, []byte(key))
	}
	before := *trigger
	trigger.NextAt, trigger.LastTransition = plan.NextAt.UTC(), at
	trigger.Completed = plan.NextAt.IsZero()
	if len(plan.Occurrences) > 0 {
		trigger.LastOccurrence = plan.Occurrences[len(plan.Occurrences)-1].UTC()
	}
	if !plan.SkippedThrough.IsZero() {
		trigger.SkippedThrough = plan.SkippedThrough.UTC()
	}
	saveJobTrigger(s, &before, trigger)
	r.Trigger, r.Created = trigger, len(plan.Occurrences)
	return nil
}

func (q *DB) GetJobTrigger(tenant, id string) (*JobTrigger, error) {
	return getStateRecord[JobTrigger](q, jobTriggersBucket, jobKey(tenant, id))
}
func (q *DB) ScheduledJobPendingCounts(tenant, trigger string) (all, own int, err error) {
	err = q.db.View(func(tx *bbolt.Tx) error {
		var e error
		all, own, e = scheduledPendingCounts(tx, tenant, trigger)
		return e
	})
	return
}

// ListDueJobTriggers is a trusted control-plane scan, not a tenant API.
func (q *DB) ListDueJobTriggers(at time.Time, after []byte, limit int) (JobTriggerPage, error) {
	page := JobTriggerPage{}
	if err := queryLimit(limit); err != nil {
		return page, err
	}
	err := q.db.View(func(tx *bbolt.Tx) error {
		c := tx.Bucket(jobTriggerDueBucket).Cursor()
		for k, v := pageStart(c, after); k != nil && len(page.Triggers) < limit; k, v = c.Next() {
			if len(k) < 12 {
				return fmt.Errorf("invalid generic trigger due key")
			}
			if bytes.Compare(k[:12], orderedTime(at)) > 0 {
				break
			}
			trigger, err := readRecord[JobTrigger](tx, jobTriggersBucket, string(v))
			if err != nil {
				return err
			}
			if trigger.Event != nil || trigger.Paused || trigger.Completed || !bytes.Equal(k, timedKey(trigger.NextAt, jobKey(trigger.TenantID, trigger.ID))) {
				return fmt.Errorf("inconsistent generic trigger due index")
			}
			page.Triggers = append(page.Triggers, *trigger)
			page.NextCursor = bytes.Clone(k)
		}
		return nil
	})
	return page, err
}

// JobTriggerCapacity is a preflight optimization. The FSM repeats these checks
// in the transaction that creates runs; stale queries confer no authority.
func (q *DB) JobTriggerCapacity(tenant, id string) (capacity int, err error) {
	err = q.db.View(func(tx *bbolt.Tx) error {
		trigger, err := readRecord[JobTrigger](tx, jobTriggersBucket, jobKey(tenant, id))
		if err != nil {
			return err
		}
		if err := checkWorkerRouting(tx, tenant, trigger.Template.Definition); err != nil {
			var rejection *CommandRejection
			if errors.As(err, &rejection) && rejection.Code == "release_inactive" {
				return nil
			}
			return err
		}
		if err := checkJobGrants(tx, tenant, trigger.Template.Resources); err != nil {
			return err
		}
		all, own, err := scheduledPendingCounts(tx, tenant, id)
		if err != nil {
			return err
		}
		capacity = max(0, min(MaxTenantScheduledPending-all, trigger.MaxPending-own))
		capacity, err = tenantJobCapacity(tx, tenant, capacity)
		return err
	})
	return
}
