// Package scheduler materializes durable occurrences and dispatches ready runs.
package scheduler

import (
	"fmt"
	"slices"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/robfig/cron/v3"
	"github.com/duravela/djob/pkg/db"
)

func calendar(spec db.ScheduleSpec) (*cron.SpecSchedule, error) {
	if spec.TimeZone == "Local" {
		return nil, fmt.Errorf("use an explicit time zone instead of Local")
	}
	location, err := time.LoadLocation(spec.TimeZone)
	if err != nil {
		return nil, fmt.Errorf("schedule time zone: %w", err)
	}
	parsed, err := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow).Parse(spec.Cron)
	if err != nil {
		return nil, fmt.Errorf("cron expression: %w", err)
	}
	schedule := parsed.(*cron.SpecSchedule)
	schedule.Location = location
	return schedule, nil
}

func nextCron(schedule *cron.SpecSchedule, after time.Time) (time.Time, error) {
	probe := after
	for i := 0; i < 3; i++ {
		next := schedule.Next(probe)
		if !next.IsZero() {
			if next.After(after) && next.Year() <= 9999 {
				return next.UTC(), nil
			}
			return time.Time{}, fmt.Errorf("cron occurrence is outside the supported time range")
		}
		// The library searches five years. Advance four only after that range has
		// no match, allowing the eight-year leap-day gap across a non-leap century.
		probe = probe.AddDate(4, 0, 0)
	}
	return time.Time{}, fmt.Errorf("cron has no occurrence within the supported twelve-year search window")
}

// PreparePut validates calendar input before proposal. Updates begin strictly
// after at; existing runs keep their previous immutable revision and payload.
func PreparePut(definition db.ScheduleDefinition, expectedRevision uint64, at time.Time) (db.PutSchedule, error) {
	if definition.MissedPolicy == db.MissedSkip && definition.MisfireGrace == 0 {
		definition.MisfireGrace = time.Second
	}
	if definition.MissedPolicy == db.MissedReplay && definition.CatchUpLimit == 0 {
		definition.CatchUpLimit = 32
	}
	definition.Spec.StartAt = definition.Spec.StartAt.UTC()
	if err := db.ValidateScheduleDefinition(definition); err != nil {
		return db.PutSchedule{}, err
	}
	timing, next, err := prepareTiming(definition.Timing(), expectedRevision, at)
	definition.Spec = timing.Spec
	return db.PutSchedule{Definition: definition, ExpectedRevision: expectedRevision, NextAt: next}, err
}

func prepareTiming(definition db.ScheduleTiming, expectedRevision uint64, at time.Time) (db.ScheduleTiming, time.Time, error) {
	next := definition.Spec.StartAt
	switch definition.Spec.Kind {
	case db.ScheduleOnce:
		if expectedRevision != 0 && definition.Spec.StartAt.Before(at) {
			return definition, next, fmt.Errorf("updated one-time schedule must be in the future")
		}
	case db.ScheduleInterval:
		if expectedRevision != 0 && !definition.Spec.StartAt.After(at) {
			latest, err := db.LatestIntervalDue(definition.Spec.StartAt, at, definition.Spec.Interval)
			if err != nil {
				return definition, next, err
			}
			next = latest.Add(definition.Spec.Interval)
		}
	case db.ScheduleCron:
		definition.Spec.Cron = strings.Join(strings.Fields(definition.Spec.Cron), " ")
		compiled, err := calendar(definition.Spec)
		if err != nil {
			return definition, next, err
		}
		after := definition.Spec.StartAt.Add(-time.Nanosecond)
		if expectedRevision != 0 && !after.After(at) {
			after = at
		}
		next, err = nextCron(compiled, after)
		if err != nil {
			return definition, next, err
		}
	}
	if next.Year() > 9999 {
		return definition, next, fmt.Errorf("initial occurrence is outside the supported time range")
	}
	return definition, next, nil
}

// latestCron uses the monotonic next-occurrence function as a binary-search
// predicate. It does not enumerate years of missed minute-by-minute work.
func latestCron(compiled *cron.SpecSchedule, earliest, at time.Time) (time.Time, error) {
	if earliest.After(at) {
		return time.Time{}, fmt.Errorf("no cron occurrence is due")
	}
	low, high := earliest.Unix()-1, at.Unix()+1
	for high-low > 1 {
		middle := low + (high-low)/2
		next, err := nextCron(compiled, time.Unix(middle, 0).UTC())
		if err == nil && !next.After(at) {
			low = middle
		} else {
			high = middle
		}
	}
	latest, err := nextCron(compiled, time.Unix(low, 0).UTC())
	if err != nil || latest.Before(earliest) || latest.After(at) {
		return time.Time{}, fmt.Errorf("invalid cron occurrence cursor")
	}
	return latest, nil
}

// Plan keeps the newest CatchUpLimit due occurrences for replay. Older backlog
// is explicitly skipped, keeping downtime from creating an unbounded work burst.
func Plan(schedule db.Schedule, at time.Time) (db.MaterializeRuns, error) {
	if err := db.ValidateScheduleDefinition(schedule.ScheduleDefinition); err != nil {
		return db.MaterializeRuns{}, err
	}
	plan, err := planTiming(schedule.TimingCursor(), at)
	plan.DeadlineAware, plan.RetryAware = schedule.StartWindow > 0, schedule.RetryPolicy != nil
	return plan, err
}

func planTiming(schedule db.TimingCursor, at time.Time) (db.MaterializeRuns, error) {
	plan := db.MaterializeRuns{ScheduleID: schedule.ID, Revision: schedule.Revision, ExpectedNext: schedule.NextAt}
	if err := db.ValidateScheduleTiming(schedule.ScheduleTiming); err != nil {
		return plan, err
	}
	if schedule.Paused || schedule.Completed || schedule.NextAt.IsZero() || schedule.NextAt.After(at) {
		return plan, fmt.Errorf("schedule is not due")
	}
	switch schedule.Spec.Kind {
	case db.ScheduleOnce:
		if schedule.MissedPolicy == db.MissedSkip && at.Sub(schedule.NextAt) > schedule.MisfireGrace {
			plan.SkippedThrough = schedule.NextAt
		} else {
			plan.Occurrences = []time.Time{schedule.NextAt}
		}
		return plan, nil
	case db.ScheduleInterval:
		return db.IntervalTimingPlan(schedule, at)
	}
	compiled, err := calendar(schedule.Spec)
	if err != nil {
		return plan, err
	}
	first, err := nextCron(compiled, schedule.NextAt.Add(-time.Nanosecond))
	if err != nil || !first.Equal(schedule.NextAt) {
		return plan, fmt.Errorf("cron cursor is not an occurrence")
	}
	latest, err := latestCron(compiled, schedule.NextAt, at)
	if err != nil {
		return plan, err
	}
	plan.NextAt, err = nextCron(compiled, latest)
	if err != nil {
		return plan, err
	}
	if schedule.MissedPolicy == db.MissedSkip && at.Sub(latest) > schedule.MisfireGrace {
		plan.SkippedThrough = latest
		return plan, nil
	}
	limit := 1
	if schedule.MissedPolicy == db.MissedReplay {
		limit = schedule.CatchUpLimit
	}
	for occurrence := latest; len(plan.Occurrences) < limit; {
		plan.Occurrences = append(plan.Occurrences, occurrence)
		if occurrence.Equal(schedule.NextAt) {
			break
		}
		occurrence, err = latestCron(compiled, schedule.NextAt, occurrence.Add(-time.Nanosecond))
		if err != nil {
			return plan, err
		}
	}
	slices.Reverse(plan.Occurrences)
	if plan.Occurrences[0].After(schedule.NextAt) {
		plan.SkippedThrough, err = latestCron(compiled, schedule.NextAt, plan.Occurrences[0].Add(-time.Nanosecond))
		if err != nil {
			return plan, err
		}
	}
	return plan, nil
}
