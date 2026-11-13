package jobs

import (
	"context"
	"errors"
	"time"

	"github.com/duravela/djob/pkg/db"
)

type ExecutionPhase uint8

const (
	ExecutionStarted ExecutionPhase = iota + 1
	ExecutionReturned
)

// ExecutionObservation describes one local Execute invocation, not a unique
// durable job completion. Duration includes driver I/O, held-resource waits,
// cleanup, result acknowledgement and joining execution monitors. A resource-free
// wait returns this invocation; its later resume is observed separately.
// CleanupFailed reports a cleanup error, not whether physical quiescence exists.
// Result, Duration and CleanupFailed are populated only for ExecutionReturned.
type ExecutionObservation struct {
	Phase ExecutionPhase
	// Result is succeeded, failed, unknown, cancelled, waiting, yielded,
	// not_admitted, interrupted, ownership_lost or error. A proposal timeout
	// without an acknowledged completion is interrupted, not a durable outcome.
	Result        string
	Duration      time.Duration
	CleanupFailed bool
}

// ActionObservation describes one local attempt to record an authorized
// action's outcome. DriverDuration is populated only when Driver.Execute was
// invoked, and includes everything that method does before returning. RecordDuration
// includes local serialization, proposal retries and receipt validation. Both
// durations use the local monotonic clock and exclude this callback.
//
// Status is the normalized effect prepared for recording, not evidence of a
// durable result. RecordConfirmed requires the completion-record path to return
// a matching successful acknowledgement while authority remains valid. A false
// value does not prove the command was uncommitted. Observations are not replayed
// after a crash and confer no execution or resource-release authority.
type ActionObservation struct {
	Status          db.EffectStatus
	DriverInvoked   bool
	DriverDuration  time.Duration
	RecordDuration  time.Duration
	RecordConfirmed bool
}

func executionResult(result Result, err error, claimConfirmed bool) string {
	if errors.Is(err, ErrOwnershipLost) {
		return "ownership_lost"
	}
	if result.Yielded && result.Run != nil {
		switch result.Run.Status {
		case db.JobWaiting:
			return "waiting"
		case db.JobReady:
			return "yielded"
		}
	}
	if err == nil && result.Run != nil {
		switch result.Run.Status {
		case db.JobSucceeded:
			return "succeeded"
		case db.JobFailed:
			return "failed"
		case db.JobUnknown:
			return "unknown"
		case db.JobCancelled:
			return "cancelled"
		}
	}
	if !claimConfirmed && expectedAdmission(err) {
		return "not_admitted"
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return "interrupted"
	}
	return "error"
}
