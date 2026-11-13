// Package jobs executes protocol-agnostic handlers using durable checkpoints,
// correlated events, and versioned drivers. Scheduling policy is external: any
// planner can launch runs and any dispatcher can contend for the same claims.
// Company handlers and integrations are registered through Registry; see
// sdk/go/README.md and examples/company-node for the extension layout.
package jobs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/duravela/djob/pkg/db"
	djob "github.com/duravela/djob/sdk/go"
)

var ErrOwnershipLost = errors.New("job execution authority is uncertain")

// ErrHandlerUnavailable means transport failed before a pure handler decision
// was received. It interrupts execution without treating it as an application
// failure. Normal handler errors must not wrap this sentinel.
var ErrHandlerUnavailable = errors.New("job handler temporarily unavailable")

type Backend interface {
	ProposeStateCommand(context.Context, db.StateCommand) (db.StateResult, error)
	GetJobView(tenant, run string) (db.JobView, error)
}

// Extension contracts are defined in the storage-independent SDK. These aliases
// keep the embedded runtime API source-compatible for existing applications.
type Version = djob.Version
type View = djob.View
type Decision = djob.Decision
type Handler = djob.Handler
type Action = djob.Action
type Effect = djob.Effect
type Driver = djob.Driver
type DriverFactory = djob.DriverFactory
type Registry = djob.Registry

type Config struct {
	WorkerAdmission  *db.WorkerAdmission // Optional controller-side precondition on new claims.
	OwnerPrefix      string              // Deployment/session identity; a fresh random incarnation is always appended.
	LeaseDuration    time.Duration
	SafetyMargin     time.Duration
	RenewInterval    time.Duration
	CommitTimeout    time.Duration
	OperationTimeout time.Duration
	RecoveryTimeout  time.Duration
	PollInterval     time.Duration
	MaxDecisions     int
	// OnExecution runs synchronously at the start and return of Execute, after
	// driver cleanup and monitor shutdown on return. It must be quick and safe
	// for concurrent executions. Observations contain no tenant or job data.
	OnExecution func(ExecutionObservation)
	// OnAdmission observes only a run's first verified claim, including the
	// acknowledgement delay. Resumes do not emit another observation. The
	// callback must be quick and concurrency-safe.
	OnAdmission func(AdmissionObservation)
	// OnReadyAdmission observes every verified claim from its latest ready
	// transition, including resumes. Zero timestamps are reported as unknown.
	// The callback must be quick and safe for concurrent calls.
	OnReadyAdmission func(time.Duration, bool)
	// OnAction runs after an authorized action's completion-record attempt,
	// including failed acknowledgements. It contains no application data and
	// must return promptly and be safe for concurrent executions.
	OnAction func(ActionObservation)
	// OnEventConsumed runs after a matching decision acknowledgement consumes an
	// inbox event, including events queued before the wait or a process restart.
	// It contains only timing metadata and must be quick and concurrency-safe.
	OnEventConsumed func(EventConsumptionObservation)
}

func (c *Config) defaults() error {
	if c.OwnerPrefix == "" {
		c.OwnerPrefix = "job-worker"
	}
	if len(c.OwnerPrefix) > 192 || strings.TrimSpace(c.OwnerPrefix) == "" || strings.ContainsAny(c.OwnerPrefix, "\r\n\x00") {
		return fmt.Errorf("invalid job owner prefix")
	}
	if c.LeaseDuration == 0 {
		c.LeaseDuration = 30 * time.Second
	}
	if c.SafetyMargin == 0 {
		c.SafetyMargin = c.LeaseDuration / 5
	}
	if c.RenewInterval == 0 {
		c.RenewInterval = c.LeaseDuration / 3
	}
	if c.CommitTimeout == 0 {
		c.CommitTimeout = 2 * time.Second
	}
	if c.OperationTimeout == 0 {
		c.OperationTimeout = 10 * time.Second
	}
	if c.RecoveryTimeout == 0 {
		c.RecoveryTimeout = 3 * time.Second
	}
	if c.PollInterval == 0 {
		c.PollInterval = 25 * time.Millisecond
	}
	if c.MaxDecisions == 0 {
		c.MaxDecisions = 10000
	}
	if c.LeaseDuration <= 0 || c.LeaseDuration > time.Hour || c.SafetyMargin <= 0 || c.SafetyMargin >= c.LeaseDuration || c.RenewInterval <= 0 || c.CommitTimeout <= 0 || c.OperationTimeout <= 0 || c.RecoveryTimeout <= 0 || c.PollInterval <= 0 || c.MaxDecisions < 1 || c.RenewInterval+2*c.CommitTimeout >= c.LeaseDuration-c.SafetyMargin {
		return fmt.Errorf("invalid job worker capacities or lease timing")
	}
	return nil
}

type Result struct {
	Run     *db.JobRun
	Yielded bool // A resource-free wait/checkpoint no longer occupies this worker.
}
