// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

package djob

import "time"

const StepRuntime = "steps/v1"

type JobGroup struct {
	TenantID   string
	ID         string
	Revision   uint64
	Runtime    string
	Resources  []string `json:",omitempty"` // Mandatory defaults; jobs may add constraints.
	MaxRunning uint64
}

// JobDefinition names application behavior. Neither its input nor its handler
// contract depends on a wire protocol. Each launch snapshots the definition,
// group policy, and binding, so configuration edits affect future runs only.
type JobDefinition struct {
	TenantID             string
	ID                   string
	Revision             uint64
	GroupID              string
	Runtime              string
	Handler              string
	HandlerVersion       string
	WorkerReleaseWeights map[string]uint32 `json:",omitempty"` // Basis points summing to 10000; at most eight published releases.
	WorkerRelease        string            `json:",omitempty"` // Published managed deployment ID, pinned for this run.
	Input                []byte            `json:",omitempty"`
	BindingID            string
	Resources            []string `json:",omitempty"`
}

// JobBinding maps semantic operations to a driver and target configuration.
// Config must contain secret references rather than embedded credentials.
type JobBinding struct {
	TenantID      string
	ID            string
	Revision      uint64
	Driver        string
	DriverVersion string
	Config        []byte   `json:",omitempty"`
	Resources     []string `json:",omitempty"`
}

type JobStatus string

const (
	JobReady     JobStatus = "ready"
	JobRunning   JobStatus = "running"
	JobWaiting   JobStatus = "waiting"
	JobSucceeded JobStatus = "succeeded"
	JobFailed    JobStatus = "failed"
	JobCancelled JobStatus = "cancelled"
	JobUnknown   JobStatus = "unknown"
)

// Terminal includes unknown outcomes. It does not establish resource quiescence.
func (s JobStatus) Terminal() bool {
	return s == JobSucceeded || s == JobFailed || s == JobCancelled || s == JobUnknown
}

type EffectStatus string

const (
	EffectAuthorized EffectStatus = "authorized" // Delivery is possible, not established.
	EffectSucceeded  EffectStatus = "succeeded"
	EffectFailed     EffectStatus = "failed"
	EffectUnknown    EffectStatus = "unknown"
)

type JobAction struct {
	TenantID     string
	RunID        string
	Sequence     uint64
	AttemptID    string
	Operation    string
	Input        []byte `json:",omitempty"`
	Status       EffectStatus
	Result       []byte `json:",omitempty"`
	Error        string
	AuthorizedAt time.Time
	FinishedAt   time.Time
}

// State is logical application state. Sockets and driver sessions are local to
// an attempt; persisting State never proves a remote device session survived.
type JobRun struct {
	Activation        *JobActivation `json:",omitempty"`
	TriggerID         string         `json:",omitempty"`
	TriggerRevision   uint64         `json:",omitempty"`
	ScheduledAt       *time.Time     `json:",omitempty"`
	TenantID          string
	ID                string
	Definition        JobDefinition
	Group             *JobGroup
	Binding           *JobBinding
	Runtime           string
	Resources         []string `json:",omitempty"`
	Status            JobStatus
	Step              uint64
	State             []byte `json:",omitempty"`
	Result            []byte `json:",omitempty"`
	Error             string
	ActionCount       uint64
	PendingAction     *JobAction
	WaitKey           string
	WakeAt            time.Time
	OwnerID           string
	AttemptID         string
	LastAttemptID     string
	AttemptCount      uint64
	LeaseUntil        time.Time
	CancelRequested   bool
	CancelRequestedAt time.Time
	CancelReason      string
	CreatedAt         time.Time
	// ReadySince is diagnostic metadata for the most recent ready transition.
	// Zero means unknown (for example, records rewritten by an older controller).
	ReadySince     time.Time `json:",omitzero"`
	LastTransition time.Time
}

// Events belong to a run and tenant, and may arrive before the wait checkpoint.
// A stable producer ID deduplicates redelivery; a conflicting payload is refused.
type JobEvent struct {
	TenantID   string
	RunID      string
	ID         string
	Key        string
	Data       []byte `json:",omitempty"`
	At         time.Time
	ConsumedAt time.Time
}

// JobActivation is immutable input for one event-created run. Definition.Input
// remains the pinned application configuration. Event identity excludes trigger
// revision, so redelivery after configuration changes returns the original run.
type JobActivation struct {
	TenantID, TriggerID, Source, EventID, RunID string
	TriggerRevision                             uint64
	OccurredAt, AcceptedAt                      time.Time
	Data                                        []byte `json:",omitempty"`
	DataDeleted                                 bool   `json:",omitempty"`
	DataSHA256                                  []byte `json:",omitempty"`
}

type JobDecisionKind string

const (
	JobDoAction   JobDecisionKind = "action"
	JobCheckpoint JobDecisionKind = "checkpoint"
	JobWait       JobDecisionKind = "wait"
	JobComplete   JobDecisionKind = "complete"
)
