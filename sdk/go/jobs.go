// Copyright (c) 2026 Sebastian Kaiser
// SPDX-License-Identifier: Apache-2.0

package djob

import (
	"context"
	"time"
)

// Version identifies an exact handler or driver contract retained by durable runs.
type Version struct{ Name, Version string }

// View supplies a handler's committed run, optional event and explicit time.
type View struct {
	Run   JobRun
	Event *JobEvent
	Now   time.Time
}

// Decision requests one validated transition. It grants no authority to do I/O.
type Decision struct {
	Kind      JobDecisionKind
	State     []byte
	Operation string
	Input     []byte
	WaitKey   string
	WakeAt    time.Time
	WaitFor   time.Duration // Relative to the committed wait command's explicit timestamp; excludes WakeAt.
	Status    JobStatus
	Result    []byte
	Error     string
}

// Handler is a bounded, pure decision function. It may run again after a crash,
// so external effects belong exclusively in Driver.Execute. A supplied event or
// previous action result is consumed atomically with the returned decision.
type Handler func(View) (Decision, error)

// Action is one engine-authorized external operation with a stable identity.
type Action struct {
	TenantID, RunID, AttemptID string
	Sequence                   uint64
	Operation                  string
	Input                      []byte
	IdempotencyKey             string // Stable for this action; useful only if the target honors it.
}

// Effect reports the observed outcome; the engine must still commit that report.
type Effect struct {
	Status      EffectStatus
	Data        []byte
	Error       string
	SessionLost bool // Stop the conversation while retaining the known action result.
}

// A driver belongs to one attempt and can keep a temporary live session across
// related actions. Execute must return only after its I/O stops. Quiesce includes
// recovery and protocol quiet periods; Close releases all local sessions. The
// runtime never detaches an uncooperative driver or treats a checkpoint as proof
// that a physical session can be reconstructed.
type Driver interface {
	Execute(context.Context, Action) Effect
	Quiesce(context.Context) error
	Close(context.Context) error
}

// Factories validate/allocate without I/O. They run only after a committed action
// authorization. A new attempt always gets a new driver, never an old session.
type DriverFactory func(JobRun) (Driver, error)

// Registry explicitly selects the versions an execution process can run.
// Its contents advertise capabilities, not execution authority.
type Registry struct {
	Handlers map[Version]Handler
	Drivers  map[Version]DriverFactory
}
