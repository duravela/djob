package executor

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

// ExecutionObservation describes one local worker invocation, not a unique job
// completion. It contains no job, channel, tenant, payload or error identifiers.
// Duration includes recovery, transport close and result acknowledgement, but
// excludes queueing and the executor's eventual channel ownership release.
// CleanupFailed reports a cleanup error, not whether physical quiescence exists.
// Result, Duration and CleanupFailed are populated only for ExecutionReturned.
type ExecutionObservation struct {
	Phase ExecutionPhase
	// Result is succeeded, failed, unknown, cancelled, expired, retry_wait,
	// not_admitted, interrupted, result_uncertain, ownership_lost or error.
	Result        string
	Duration      time.Duration
	CleanupFailed bool
}

func executionResult(result Result, err error, claimConfirmed bool) string {
	if errors.Is(err, ErrResultUncertain) {
		return "result_uncertain"
	}
	if result.Committed {
		if !result.RetryAt.IsZero() {
			return "retry_wait"
		}
		switch result.Outcome {
		case db.AttemptSucceeded:
			return "succeeded"
		case db.AttemptFailed:
			return "failed"
		case db.AttemptUnknown:
			return "unknown"
		case db.AttemptCancelled:
			return "cancelled"
		case db.AttemptExpired:
			return "expired"
		}
	}
	if errors.Is(err, ErrOwnershipLost) || errors.Is(err, ErrLeaseExpiring) {
		return "ownership_lost"
	}
	if !claimConfirmed {
		if errors.Is(err, db.ErrAdmissionHeld) {
			return "not_admitted"
		}
		if errors.Is(err, ErrRunCancelled) || errors.Is(err, ErrRunExpired) {
			return "not_admitted"
		}
		var rejection *db.CommandRejection
		if errors.As(err, &rejection) && (rejection.Code == "run_unavailable" || rejection.Code == "not_found") {
			return "not_admitted"
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return "interrupted"
	}
	return "error"
}
