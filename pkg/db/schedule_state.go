package db

import (
	"bytes"
	"fmt"
	"time"

	"go.etcd.io/bbolt"
)

func transitionSchedule(tx *bbolt.Tx, command StateCommand, result *StateResult) error {
	switch {
	case command.PutSchedule != nil:
		put := command.PutSchedule
		d := put.Definition
		if err := ValidateScheduleDefinition(d); err != nil {
			return err
		}
		if _, err := readRecord[CommunicationChannel](tx, channelsBucket, d.ChannelID); err != nil {
			return err
		}
		if put.NextAt.IsZero() || put.NextAt.Before(d.Spec.StartAt) || put.NextAt.Year() > 9999 {
			return reject("invalid", "initial occurrence must be at or after the schedule start")
		}
		if d.Spec.Kind == ScheduleOnce && !put.NextAt.Equal(d.Spec.StartAt) {
			return reject("invalid", "one-time occurrence must equal its start time")
		}
		if d.Spec.Kind == ScheduleInterval && !intervalOccurrence(d.Spec.StartAt, put.NextAt, d.Spec.Interval) {
			return reject("invalid", "interval occurrence is not anchored to the start")
		}
		revision := uint64(1)
		if tx.Bucket(schedulesBucket).Get([]byte(d.ID)) != nil {
			old, err := readRecord[Schedule](tx, schedulesBucket, d.ID)
			if err != nil {
				return err
			}
			if old.Revision != put.ExpectedRevision {
				return reject("revision_conflict", "schedule revision does not match")
			}
			if command.At.Before(old.LastTransition) {
				return reject("stale_time", "schedule update precedes its last transition")
			}
			if old.Revision == ^uint64(0) {
				return reject("exhausted", "schedule revision exhausted")
			}
			if put.NextAt.Before(command.At) || (d.Spec.Kind != ScheduleOnce && !put.NextAt.After(command.At)) {
				return reject("invalid", "updated schedule must begin in the future")
			}
			revision = old.Revision + 1
		} else if put.ExpectedRevision != 0 {
			return reject("revision_conflict", "new schedule requires revision zero")
		}
		d.Spec.StartAt = d.Spec.StartAt.UTC()
		d.Payload = bytes.Clone(d.Payload)
		d.RetryPolicy = d.RetryPolicy.Clone()
		schedule := Schedule{ScheduleDefinition: d, Revision: revision, NextAt: put.NextAt.UTC(), LastTransition: command.At}
		if err := writeScheduleRecord(tx, schedule); err != nil {
			return err
		}
		result.Schedule = &schedule

	case command.PauseSchedule != nil:
		pause := command.PauseSchedule
		schedule, err := readRecord[Schedule](tx, schedulesBucket, pause.ID)
		if err != nil {
			return err
		}
		if schedule.Revision != pause.ExpectedRevision {
			return reject("revision_conflict", "schedule revision does not match")
		}
		if command.At.Before(schedule.LastTransition) {
			return reject("stale_time", "pause precedes the schedule's last transition")
		}
		if schedule.Paused != pause.Paused {
			if schedule.Revision == ^uint64(0) {
				return reject("exhausted", "schedule revision exhausted")
			}
			schedule.Revision++
			schedule.Paused = pause.Paused
			schedule.LastTransition = command.At
			if err := writeScheduleRecord(tx, *schedule); err != nil {
				return err
			}
		}
		result.Schedule = schedule

	case command.Materialize != nil:
		plan := command.Materialize
		schedule, err := readRecord[Schedule](tx, schedulesBucket, plan.ScheduleID)
		if err != nil {
			return err
		}
		if err := validateMaterialization(*schedule, command.At, *plan); err != nil {
			return err
		}
		if schedule.StartWindow > 0 && !plan.DeadlineAware {
			return reject("invalid", "schedule requires deadline-aware materialization")
		}
		if schedule.RetryPolicy != nil && !plan.RetryAware {
			return reject("invalid", "schedule requires retry-aware materialization")
		}
		runs := make([]Run, 0, len(plan.Occurrences))
		ready := 0
		for _, at := range plan.Occurrences {
			id := OccurrenceRunID(schedule.ID, schedule.Revision, at)
			if tx.Bucket(occurrencesBucket).Get([]byte(id)) != nil || tx.Bucket(runsBucket).Get([]byte(id)) != nil {
				return reject("occurrence_exists", "schedule occurrence already exists")
			}
			run := Run{ID: id, ChannelID: schedule.ChannelID, Executor: schedule.Executor, Payload: bytes.Clone(schedule.Payload), State: RunReady,
				CreatedAt: command.At, ReadySince: command.At, LastTransition: command.At, ScheduleID: schedule.ID, ScheduleRevision: schedule.Revision, ScheduledAt: at.UTC()}
			run.RetryPolicy = schedule.RetryPolicy.Clone()
			if schedule.StartWindow > 0 {
				run.StartDeadline = at.Add(schedule.StartWindow).UTC()
				if !validDeadline(run.StartDeadline) {
					return reject("invalid", "occurrence start deadline is outside the supported time range")
				}
				if run.StartDeadlinePassed(command.At) {
					run.State = RunExpired
				}
			}
			if run.State == RunReady {
				ready++
			}
			runs = append(runs, run)
		}
		// All business checks precede writes. Storage failure rolls back this entire
		// batch, including the cursor, occurrence ledger, and request receipt.
		count, err := pendingCount(tx, schedule.ChannelID)
		if err != nil {
			return err
		}
		if ready != 0 && count+uint64(ready) > MaxReadyRunsPerChannel {
			return reject("backpressure", "gateway ready-run capacity reached")
		}
		for _, run := range runs {
			if err := writeRunRecord(tx, run); err != nil {
				return err
			}
			if err := tx.Bucket(occurrencesBucket).Put([]byte(run.ID), []byte(run.ID)); err != nil {
				return err
			}
		}
		schedule.NextAt, schedule.Completed, schedule.LastTransition = plan.NextAt.UTC(), plan.NextAt.IsZero(), command.At
		if len(plan.Occurrences) != 0 {
			schedule.LastOccurrence = plan.Occurrences[len(plan.Occurrences)-1].UTC()
		} else {
			schedule.LastOccurrence = plan.SkippedThrough.UTC()
		}
		if !plan.SkippedThrough.IsZero() {
			schedule.SkippedThrough = plan.SkippedThrough.UTC()
		}
		if err := writeScheduleRecord(tx, *schedule); err != nil {
			return err
		}
		result.Schedule, result.Runs = schedule, runs
	}
	return nil
}

func intervalOccurrence(anchor, at time.Time, interval time.Duration) bool {
	if at.Before(anchor) || interval <= 0 {
		return false
	}
	delta := at.Sub(anchor)
	return anchor.Add(delta).Equal(at) && delta%interval == 0
}

// IntervalPlan is shared by the planner and state validation. It uses no clocks
// or zone data and retains only the most recent bounded replay window.
func IntervalPlan(schedule Schedule, at time.Time) (MaterializeRuns, error) {
	plan, err := IntervalTimingPlan(schedule.TimingCursor(), at)
	plan.DeadlineAware, plan.RetryAware = schedule.StartWindow > 0, schedule.RetryPolicy != nil
	return plan, err
}

func IntervalTimingPlan(schedule TimingCursor, at time.Time) (MaterializeRuns, error) {
	plan := MaterializeRuns{ScheduleID: schedule.ID, Revision: schedule.Revision, ExpectedNext: schedule.NextAt}
	latest, err := LatestIntervalDue(schedule.NextAt, at, schedule.Spec.Interval)
	if err != nil {
		return plan, err
	}
	plan.NextAt = latest.Add(schedule.Spec.Interval)
	if !plan.NextAt.After(at) || plan.NextAt.Year() > 9999 {
		return plan, fmt.Errorf("next interval occurrence is outside the supported time range")
	}
	count := 1
	if schedule.MissedPolicy == MissedReplay {
		total := uint64(latest.Sub(schedule.NextAt)/schedule.Spec.Interval) + 1
		count = schedule.CatchUpLimit
		if uint64(count) > total {
			count = int(total)
		}
	}
	if schedule.MissedPolicy == MissedSkip && at.Sub(latest) > schedule.MisfireGrace {
		plan.SkippedThrough = latest
		return plan, nil
	}
	first := latest.Add(-time.Duration(count-1) * schedule.Spec.Interval)
	if first.After(schedule.NextAt) {
		plan.SkippedThrough = first.Add(-schedule.Spec.Interval)
	}
	for i := 0; i < count; i++ {
		plan.Occurrences = append(plan.Occurrences, first.Add(time.Duration(i)*schedule.Spec.Interval))
	}
	return plan, nil
}

func validateMaterialization(schedule Schedule, at time.Time, plan MaterializeRuns) error {
	return validateTimingMaterialization(schedule.TimingCursor(), at, plan)
}

func validateTimingMaterialization(schedule TimingCursor, at time.Time, plan MaterializeRuns) error {
	if schedule.Revision != plan.Revision || !schedule.NextAt.Equal(plan.ExpectedNext) {
		return reject("schedule_conflict", "schedule revision or next occurrence changed")
	}
	if schedule.Paused || schedule.Completed {
		return reject("schedule_inactive", "schedule is paused or complete")
	}
	if at.Before(schedule.LastTransition) || at.Before(schedule.NextAt) {
		return reject("not_due", "schedule is not due at the proposed time")
	}
	if len(plan.Occurrences) > MaxMaterializedRuns {
		return reject("invalid", "materialization batch is too large")
	}
	if schedule.Spec.Kind == ScheduleInterval {
		expected, err := IntervalTimingPlan(schedule, at)
		if err != nil {
			return reject("invalid", err.Error())
		}
		if !samePlan(expected, plan) {
			return reject("invalid", "interval plan does not match its anchored recurrence and missed-run policy")
		}
		return nil
	}
	if schedule.Spec.Kind == ScheduleOnce {
		expected := MaterializeRuns{ScheduleID: schedule.ID, Revision: schedule.Revision, ExpectedNext: schedule.NextAt}
		if schedule.MissedPolicy == MissedSkip && at.Sub(schedule.NextAt) > schedule.MisfireGrace {
			expected.SkippedThrough = schedule.NextAt
		} else {
			expected.Occurrences = []time.Time{schedule.NextAt}
		}
		if !samePlan(expected, plan) {
			return reject("invalid", "one-time plan does not match its missed-run policy")
		}
		return nil
	}
	// Cron boundaries are calculated before proposal using an explicit zone.
	// Applying replicas validate ordered instants, not host-specific zone rules.
	if !plan.NextAt.After(at) || plan.NextAt.Year() > 9999 {
		return reject("invalid", "cron cursor must advance beyond the cutoff")
	}
	limit := 1
	if schedule.MissedPolicy == MissedReplay {
		limit = schedule.CatchUpLimit
	}
	if len(plan.Occurrences) > limit || (len(plan.Occurrences) == 0 && schedule.MissedPolicy != MissedSkip) {
		return reject("invalid", "invalid cron catch-up batch")
	}
	previous := time.Time{}
	for _, occurrence := range plan.Occurrences {
		if occurrence.Before(schedule.NextAt) || occurrence.After(at) || (!previous.IsZero() && !occurrence.After(previous)) {
			return reject("invalid", "cron occurrences must be ordered within the due range")
		}
		previous = occurrence
	}
	if schedule.MissedPolicy == MissedSkip && len(plan.Occurrences) != 0 && at.Sub(plan.Occurrences[0]) > schedule.MisfireGrace {
		return reject("invalid", "skipped cron occurrence exceeds its grace")
	}
	first := plan.NextAt
	if len(plan.Occurrences) != 0 {
		first = plan.Occurrences[0]
	}
	if first.Equal(schedule.NextAt) {
		if !plan.SkippedThrough.IsZero() {
			return reject("invalid", "no prefix was skipped")
		}
	} else if plan.SkippedThrough.Before(schedule.NextAt) || plan.SkippedThrough.After(at) || !plan.SkippedThrough.Before(first) {
		return reject("invalid", "missing or invalid skipped-occurrence watermark")
	}
	return nil
}

func samePlan(a, b MaterializeRuns) bool {
	if !a.NextAt.Equal(b.NextAt) || !a.SkippedThrough.Equal(b.SkippedThrough) || len(a.Occurrences) != len(b.Occurrences) {
		return false
	}
	for i := range a.Occurrences {
		if !a.Occurrences[i].Equal(b.Occurrences[i]) {
			return false
		}
	}
	return true
}
