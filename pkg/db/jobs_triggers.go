package db

import (
	"crypto/sha256"
	"encoding/hex"
	"time"
)

const (
	DefaultTriggerMaxPending = 32
	MaxTriggerPending        = 128
	// Automatic activation has a tenant-wide bound even when no worker is enabled.
	MaxTenantScheduledPending = 256
)

// JobTemplate pins all execution configuration at trigger creation/update.
// Resource grants and admission limits remain live policy at execution time.
type JobTemplate struct {
	Definition JobDefinition
	Group      *JobGroup
	Binding    *JobBinding
	Runtime    string
	Resources  []string `json:",omitempty"`
}

type JobTriggerDefinition struct {
	Event              *EventTriggerSpec `json:",omitempty"`
	TenantID           string
	ID                 string
	DefinitionID       string
	DefinitionRevision uint64
	Timing             ScheduleTiming
	Paused             bool
	MaxPending         int
}

type JobTrigger struct {
	JobTriggerDefinition
	Revision       uint64
	Template       JobTemplate
	NextAt         time.Time
	Completed      bool
	LastTransition time.Time
	LastOccurrence time.Time
	SkippedThrough time.Time
}

func (t JobTrigger) TimingCursor() TimingCursor {
	return TimingCursor{ScheduleTiming: t.Timing, ID: t.ID, Revision: t.Revision, NextAt: t.NextAt, Paused: t.Paused, Completed: t.Completed, LastTransition: t.LastTransition}
}

type PutJobTrigger struct {
	Definition       JobTriggerDefinition
	ExpectedRevision uint64
	NextAt           time.Time
}
type PauseJobTrigger struct {
	TenantID, ID     string
	ExpectedRevision uint64
	Paused           bool
}
type MaterializeJobs struct {
	TenantID string
	Plan     MaterializeRuns // Calendar instants only; deadline/retry profile flags must be false.
}
type JobTriggerPage struct {
	Triggers   []JobTrigger
	NextCursor []byte
}

func ValidateJobTrigger(d JobTriggerDefinition) error {
	if !validIdentity(d.TenantID) || !validIdentity(d.ID) || !validIdentity(d.DefinitionID) || d.DefinitionRevision == 0 {
		return reject("invalid", "trigger requires tenant, ID, and a versioned job definition")
	}
	if d.MaxPending < 1 || d.MaxPending > MaxTriggerPending || (d.Timing.MissedPolicy == MissedReplay && d.Timing.CatchUpLimit > d.MaxPending) {
		return reject("invalid", "trigger pending limit must be 1..128 and accommodate its catch-up batch")
	}
	if d.Event != nil {
		if !validIdentity(d.Event.Source) || d.Timing != (ScheduleTiming{}) {
			return reject("invalid", "event trigger requires a source and no calendar timing")
		}
		return nil
	}
	if d.Timing.Spec.StartAt.Year() < 1 || d.Timing.Spec.StartAt.Year() > 9999 {
		return reject("invalid", "unsupported trigger start time")
	}
	return ValidateScheduleTiming(d.Timing)
}

// JobOccurrenceRunID includes the tenant namespace and trigger revision. Its
// occurrence and cursor are committed with the run in a single Raft transaction.
func JobOccurrenceRunID(tenant, trigger string, revision uint64, at time.Time) string {
	sum := sha256.Sum256([]byte(jobKey(tenant, OccurrenceRunID(trigger, revision, at))))
	return "triggered/" + hex.EncodeToString(sum[:])
}

func cloneJobTemplate(t JobTemplate) JobTemplate {
	t.Definition = *cloneJobDefinition(&t.Definition)
	t.Group = cloneJobGroup(t.Group)
	t.Binding = cloneJobBinding(t.Binding)
	t.Resources = append([]string(nil), t.Resources...)
	return t
}
func cloneJobTrigger(t *JobTrigger) *JobTrigger {
	out := jobCopy(t)
	if out != nil {
		out.Template = cloneJobTemplate(t.Template)
		out.Event = jobCopy(t.Event)
	}
	return out
}
