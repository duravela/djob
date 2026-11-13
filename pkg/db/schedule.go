package db

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

type ScheduleKind string

const (
	ScheduleOnce     ScheduleKind = "once"
	ScheduleInterval ScheduleKind = "interval"
	ScheduleCron     ScheduleKind = "cron"
)

type MissedRunPolicy string

const (
	MissedSkip             MissedRunPolicy = "skip"
	MissedCoalesce         MissedRunPolicy = "coalesce"
	MissedReplay           MissedRunPolicy = "replay"
	MaxMaterializedRuns                    = 128
	MaxQueryPage                           = 256
	MaxReadyRunsPerChannel                 = 256
)

type ScheduleSpec struct {
	Kind     ScheduleKind
	StartAt  time.Time
	Interval time.Duration
	Cron     string
	TimeZone string
}

type ScheduleDefinition struct {
	ID           string
	ChannelID    string
	Executor     string
	Payload      []byte
	Spec         ScheduleSpec
	MissedPolicy MissedRunPolicy
	MisfireGrace time.Duration
	CatchUpLimit int
	Paused       bool
	StartWindow  time.Duration `json:",omitempty"` // Relative to each occurrence, not materialization time.
	RetryPolicy  *RetryPolicy  `json:",omitempty"`
}

type Schedule struct {
	ScheduleDefinition
	Revision       uint64
	NextAt         time.Time
	Completed      bool
	LastTransition time.Time
	LastOccurrence time.Time
	SkippedThrough time.Time
}

type PutSchedule struct {
	Definition       ScheduleDefinition
	ExpectedRevision uint64
	NextAt           time.Time // Calculated before proposal; replicas never load zone rules.
}

type PauseSchedule struct {
	ID               string
	ExpectedRevision uint64
	Paused           bool
}

type MaterializeRuns struct {
	ScheduleID     string
	Revision       uint64
	ExpectedNext   time.Time
	Occurrences    []time.Time
	NextAt         time.Time
	SkippedThrough time.Time
	DeadlineAware  bool `json:",omitempty"`
	RetryAware     bool `json:",omitempty"`
}

type RunPage struct {
	Runs       []Run
	NextCursor []byte
}
type SchedulePage struct {
	Schedules  []Schedule
	NextCursor []byte
}

// ValidateScheduleDefinition performs deterministic structural checks. The
// scheduler also parses cron and loads the zone before proposing configuration.
func ValidateScheduleDefinition(d ScheduleDefinition) error {
	if err := d.RetryPolicy.Validate(); err != nil {
		return err
	}
	if len(d.Payload) > 64<<10 {
		return reject("invalid", "schedule payload exceeds 64 KiB")
	}
	if !validIdentity(d.ID) || !validIdentity(d.ChannelID) || !validIdentity(d.Executor) || d.Spec.StartAt.IsZero() {
		return reject("invalid", "schedule ID, channel, executor, and start time are required")
	}
	if d.MisfireGrace < 0 || d.CatchUpLimit < 0 || d.CatchUpLimit > MaxMaterializedRuns {
		return reject("invalid", "invalid misfire grace or catch-up limit")
	}
	if d.StartWindow < 0 || (d.StartWindow > 0 && !validDeadline(d.Spec.StartAt.Add(d.StartWindow))) {
		return reject("invalid", "start window must be nonnegative and produce a supported deadline")
	}
	return ValidateScheduleTiming(d.Timing())
}

func ValidateScheduleTiming(d ScheduleTiming) error {
	if d.Spec.StartAt.IsZero() {
		return reject("invalid", "a supported start time is required")
	}
	if d.MisfireGrace < 0 || d.CatchUpLimit < 0 || d.CatchUpLimit > MaxMaterializedRuns {
		return reject("invalid", "invalid misfire grace or catch-up limit")
	}
	switch d.MissedPolicy {
	case MissedSkip:
		if d.MisfireGrace <= 0 {
			return reject("invalid", "skip policy requires a positive misfire grace")
		}
	case MissedCoalesce:
	case MissedReplay:
		if d.CatchUpLimit == 0 {
			return reject("invalid", "replay policy requires a bounded catch-up limit")
		}
	default:
		return reject("invalid", "choose skip, coalesce, or replay for missed runs")
	}
	switch d.Spec.Kind {
	case ScheduleOnce:
		if d.Spec.Interval != 0 || d.Spec.Cron != "" || d.Spec.TimeZone != "" {
			return reject("invalid", "one-time schedule has incompatible recurrence fields")
		}
	case ScheduleInterval:
		if d.Spec.Interval <= 0 || d.Spec.Cron != "" || d.Spec.TimeZone != "" {
			return reject("invalid", "interval schedule requires a positive interval and no cron fields")
		}
	case ScheduleCron:
		if d.Spec.Interval != 0 || len(strings.Fields(d.Spec.Cron)) != 5 || len(d.Spec.Cron) > 256 || !validIdentity(d.Spec.TimeZone) {
			return reject("invalid", "cron requires five fields and an explicit time zone")
		}
	default:
		return reject("invalid", "unknown schedule kind")
	}
	return nil
}

// OccurrenceRunID is stable across duplicate proposals and leader changes.
func OccurrenceRunID(scheduleID string, revision uint64, at time.Time) string {
	h := sha256.New()
	var number [8]byte
	binary.BigEndian.PutUint64(number[:], uint64(len(scheduleID)))
	h.Write(number[:])
	h.Write([]byte(scheduleID))
	binary.BigEndian.PutUint64(number[:], revision)
	h.Write(number[:])
	h.Write([]byte(at.UTC().Format(time.RFC3339Nano)))
	return "scheduled/" + hex.EncodeToString(h.Sum(nil))
}

// LatestIntervalDue avoids iterating through a large missed-run backlog.
func LatestIntervalDue(next, at time.Time, interval time.Duration) (time.Time, error) {
	if interval <= 0 || next.After(at) {
		return time.Time{}, fmt.Errorf("no interval occurrence is due")
	}
	delta := at.Sub(next)
	if next.Add(delta).Before(at) {
		return time.Time{}, fmt.Errorf("interval backlog exceeds supported duration range")
	}
	return next.Add(delta - delta%interval).UTC(), nil
}
