package executor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/duravela/djob/pkg/db"
)

func (e *Executor) execute(t task) (result Result, err error) {
	workerBegan := time.Now()
	claimConfirmed := false
	if observe := e.cfg.OnExecution; observe != nil {
		observe(ExecutionObservation{Phase: ExecutionStarted})
		// Register first so per-run monitor cleanup finishes before observation.
		defer func() {
			observe(ExecutionObservation{
				Phase: ExecutionReturned, Result: executionResult(result, err, claimConfirmed),
				Duration: time.Since(workerBegan), CleanupFailed: errors.Is(err, ErrRecovery),
			})
		}()
	}
	result = Result{RunID: t.runID}
	run, err := e.backend.GetRun(t.runID)
	if err != nil {
		return result, err
	}
	if run.ChannelID != e.owner.ChannelID {
		return result, fmt.Errorf("run %q belongs to another channel", t.runID)
	}
	if run.State == db.RunCancelled {
		result.Outcome = db.AttemptCancelled
		return result, ErrRunCancelled
	}
	if run.State == db.RunExpired {
		result.Outcome = db.AttemptExpired
		return result, ErrRunExpired
	}
	if run.State != db.RunReady {
		return result, &db.CommandRejection{Code: "run_unavailable", Message: "run is not ready"}
	}
	if run.StartDeadlinePassed(time.Now()) {
		return e.expireQueued(run)
	}
	id, err := randomID()
	if err != nil {
		return result, err
	}
	token, err := randomID()
	if err != nil {
		return result, err
	}
	result.AttemptID = e.owner.OwnerID + "/attempt/" + id
	claimBegan := time.Now()
	claimed, err := e.propose(context.Background(), func(c *db.StateCommand) {
		c.Claim = &db.ClaimRun{ChannelOwner: e.owner, RunID: run.ID, AttemptID: result.AttemptID, ClaimToken: token}
	})
	if err != nil {
		if errors.Is(err, db.ErrAdmissionHeld) {
			return result, err
		}
		var rejection *db.CommandRejection
		if errors.As(err, &rejection) && rejection.Code == "start_deadline_expired" {
			return e.expireQueued(run)
		}
		if !errors.As(err, &rejection) || (rejection.Code != "run_unavailable" && rejection.Code != "not_found") {
			e.fail(errors.Join(ErrOwnershipLost, err))
		}
		return result, err
	}
	if claimed.Channel == nil || claimed.Run == nil || claimed.Attempt == nil || claimed.Run.ID != run.ID ||
		claimed.Run.State != db.RunRunning || claimed.Run.ChannelID != e.owner.ChannelID || claimed.Run.ActiveAttemptID != result.AttemptID ||
		claimed.Channel.ActiveAttemptID != result.AttemptID || claimed.Attempt.ID != result.AttemptID || claimed.Attempt.RunID != run.ID ||
		claimed.Attempt.ClaimToken != token || claimed.Attempt.OwnerID != e.owner.OwnerID || claimed.Attempt.Epoch != e.owner.Epoch {
		err := fmt.Errorf("%w: invalid claim receipt", ErrOwnershipLost)
		e.fail(err)
		return result, err
	}
	claimConfirmed = true
	e.observeAdmission(claimed.Run)
	// Anchor the immutable deadline to the claim proposal's start. A delayed ack
	// or a backward wall-clock adjustment must not grant extra local start time.
	localDeadline := claimBegan.Add(claimed.Run.StartDeadline.Sub(claimBegan))
	deadlinePassed := func() bool {
		if claimed.Run.StartDeadline.IsZero() {
			return false
		}
		now := time.Now()
		return claimed.Run.StartDeadlinePassed(now) || !now.Before(localDeadline)
	}
	if deadlinePassed() {
		return e.complete(result, token, Outcome{Status: db.AttemptFailed}, ErrRunExpired, true, true)
	}
	runCtx, stopCancellation := e.watchCancellation(t.ctx, run.ID)
	defer stopCancellation()
	// A fresh renewal checks committed authority immediately before opening any
	// connection. A delayed or replayed claim receipt is not an execution permit.
	if runCtx.Err() != nil {
		return e.complete(result, token, Outcome{Status: db.AttemptFailed}, context.Cause(runCtx), true, true)
	}
	fresh, err := e.renew()
	if err != nil || fresh.Channel.State != db.ChannelOwned || fresh.Channel.ActiveAttemptID != result.AttemptID {
		err = errors.Join(ErrOwnershipLost, err)
		if e.life.Err() == nil {
			e.fail(err)
		}
		return e.complete(result, token, Outcome{Status: db.AttemptFailed}, err, true, true)
	}
	if runCtx.Err() != nil {
		return e.complete(result, token, Outcome{Status: db.AttemptFailed}, context.Cause(runCtx), true, true)
	}
	started, err := e.propose(runCtx, func(c *db.StateCommand) {
		c.StartAttempt = &db.StartAttempt{ChannelOwner: e.owner, RunID: run.ID, AttemptID: result.AttemptID, ClaimToken: token, DeadlineAware: true, RetryAware: true}
	})
	if err != nil {
		var rejection *db.CommandRejection
		if errors.As(err, &rejection) && rejection.Code == "cancel_requested" {
			err = ErrCancellationRequested
		} else if errors.As(err, &rejection) && rejection.Code == "start_deadline_expired" {
			err = ErrRunExpired
		} else if runCtx.Err() != nil {
			err = context.Cause(runCtx)
		} else {
			err = errors.Join(ErrOwnershipLost, err)
			e.fail(err)
		}
		return e.complete(result, token, Outcome{Status: db.AttemptFailed}, err, true, true)
	}
	if started.Channel == nil || started.Channel.State != db.ChannelOwned || started.Channel.OwnerID != e.owner.OwnerID || started.Channel.Epoch != e.owner.Epoch ||
		started.Channel.ActiveAttemptID != result.AttemptID || started.Run == nil || started.Run.ID != run.ID || started.Run.State != db.RunRunning || started.Run.CancellationRequested() ||
		!started.Run.StartDeadline.Equal(claimed.Run.StartDeadline) || started.Attempt == nil || started.Attempt.ID != result.AttemptID || started.Attempt.ClaimToken != token || started.Attempt.AuthorizedAt.IsZero() {
		err = fmt.Errorf("%w: invalid start authorization receipt", ErrOwnershipLost)
		e.fail(err)
		return e.complete(result, token, Outcome{Status: db.AttemptFailed}, err, true, true)
	}
	if runCtx.Err() != nil {
		return e.complete(result, token, Outcome{Status: db.AttemptFailed}, context.Cause(runCtx), true, true)
	}
	if deadlinePassed() {
		return e.complete(result, token, Outcome{Status: db.AttemptFailed}, ErrRunExpired, true, true)
	}
	transport, err := e.factory(*started.Channel)
	if err != nil {
		return e.complete(result, token, Outcome{Status: db.AttemptFailed}, err, true, true)
	}
	if transport == nil {
		return e.complete(result, token, Outcome{Status: db.AttemptFailed}, fmt.Errorf("transport factory returned nil"), true, true)
	}
	opCtx, cancel := context.WithTimeout(runCtx, e.cfg.OperationTimeout)
	e.mu.Lock()
	e.releaseSafe = false
	e.mu.Unlock()
	outcome := Outcome{Status: db.AttemptFailed}
	noDelivery := true
	if err = e.authorized(); err == nil {
		err = opCtx.Err()
	} else if e.life.Err() == nil {
		e.fail(err)
	}
	if err == nil && deadlinePassed() {
		err = ErrRunExpired
	}
	if err == nil {
		noDelivery = false
		outcome, err = transport.Exchange(opCtx, Request{Channel: *started.Channel, RunID: claimed.Run.ID, AttemptID: result.AttemptID,
			Executor: claimed.Run.Executor, Payload: bytes.Clone(claimed.Run.Payload)})
	}
	if outcome.Status != db.AttemptSucceeded && errors.Is(context.Cause(runCtx), ErrCancellationRequested) {
		err = errors.Join(err, ErrCancellationRequested)
	}
	cancel()
	cleanupErr := e.recover(transport)
	quiescent := cleanupErr == nil
	e.mu.Lock()
	e.releaseSafe = quiescent
	e.mu.Unlock()
	if !quiescent {
		cleanupErr = errors.Join(ErrRecovery, cleanupErr)
		e.fail(cleanupErr)
	}
	return e.complete(result, token, outcome, errors.Join(err, cleanupErr), quiescent, noDelivery)
}

func (e *Executor) recover(transport Transport) error {
	// Neither context inherits operation cancellation or executor shutdown.
	ctx, cancel := context.WithTimeout(context.Background(), e.cfg.RecoveryTimeout)
	recoverErr := transport.Recover(ctx)
	recoverErr = errors.Join(recoverErr, ctx.Err())
	cancel()
	ctx, cancel = context.WithTimeout(context.Background(), e.cfg.CloseTimeout)
	closeErr := transport.Close(ctx)
	closeErr = errors.Join(closeErr, ctx.Err())
	cancel()
	return errors.Join(recoverErr, closeErr)
}

func (e *Executor) complete(result Result, token string, outcome Outcome, executionErr error, quiescent, noDelivery bool) (Result, error) {
	if outcome.RetryClass != "" && (outcome.RetryClass != db.RetryNotSent || outcome.Status != db.AttemptFailed || len(outcome.Data) != 0) {
		outcome.Status, outcome.RetryClass = db.AttemptUnknown, ""
		executionErr = errors.Join(executionErr, fmt.Errorf("adapter returned invalid retry evidence"))
	}
	switch outcome.Status {
	case db.AttemptSucceeded:
	case db.AttemptFailed:
		if executionErr == nil {
			executionErr = ErrExecutionFailed
		}
	case db.AttemptUnknown:
		executionErr = errors.Join(ErrUnknownOutcome, executionErr)
	default:
		outcome.Status = db.AttemptUnknown
		executionErr = errors.Join(ErrUnknownOutcome, executionErr)
	}
	result.Outcome, result.Data, result.Quiescent = outcome.Status, bytes.Clone(outcome.Data), quiescent
	message := ""
	if executionErr != nil {
		message = strings.ToValidUTF8(executionErr.Error(), "\uFFFD")
	}
	completed, err := e.propose(context.Background(), func(c *db.StateCommand) {
		c.Finish = &db.FinishAttempt{ChannelOwner: e.owner, RunID: result.RunID, AttemptID: result.AttemptID, ClaimToken: token,
			Outcome: outcome.Status, Result: outcome.Data, Error: message, Quiescent: quiescent, CancellationAware: true, NoDelivery: noDelivery, DeadlineAware: true,
			RetryAware: true, RetryClass: outcome.RetryClass}
	})
	expected := outcome.Status
	if noDelivery && completed.Run.CancellationRequested() {
		expected = db.AttemptCancelled
	} else if noDelivery && completed.Attempt != nil && completed.Run.StartDeadlinePassed(completed.Attempt.FinishedAt) {
		expected = db.AttemptExpired
	}
	if err == nil && (completed.Attempt == nil || completed.Attempt.ID != result.AttemptID || completed.Attempt.OwnerID != e.owner.OwnerID ||
		completed.Attempt.Epoch != e.owner.Epoch || completed.Attempt.ClaimToken != token || completed.Attempt.Outcome != expected || completed.Attempt.Quiescent != quiescent || completed.Attempt.NoDelivery != noDelivery || completed.Attempt.RetryClass != outcome.RetryClass ||
		completed.Run == nil || completed.Run.ID != result.RunID || completed.Run.LastAttemptID != result.AttemptID) {
		err = fmt.Errorf("invalid completion receipt")
	}
	if err != nil {
		err = errors.Join(ErrResultUncertain, err)
		e.fail(err)
		return result, errors.Join(executionErr, err)
	}
	result.Committed = true
	result.Outcome = completed.Attempt.Outcome
	if completed.Run.State == db.RunRetryWait {
		if completed.Run.RetryAt.IsZero() || !completed.Run.RetryAt.Equal(completed.Attempt.RetryAt) || !quiescent || outcome.RetryClass != db.RetryNotSent || result.Outcome != db.AttemptFailed {
			err := fmt.Errorf("%w: invalid retry completion receipt", ErrResultUncertain)
			e.fail(err)
			return result, errors.Join(executionErr, err)
		}
		result.RetryAt = completed.Run.RetryAt
		executionErr = errors.Join(ErrRetryScheduled, executionErr)
	}
	if result.Outcome == db.AttemptCancelled {
		executionErr = errors.Join(ErrRunCancelled, executionErr)
	} else if result.Outcome == db.AttemptExpired {
		executionErr = errors.Join(ErrRunExpired, executionErr)
	}
	return result, executionErr
}

func (e *Executor) expireQueued(run *db.Run) (Result, error) {
	result := Result{RunID: run.ID}
	receipt, err := e.propose(context.Background(), func(c *db.StateCommand) {
		c.ExpireRun = &db.ExpireRun{RunID: run.ID, StartDeadline: run.StartDeadline, RetryAware: true}
	})
	if err != nil {
		return result, err
	}
	if receipt.Run == nil || receipt.Run.ID != run.ID {
		return result, fmt.Errorf("invalid run expiry receipt")
	}
	if receipt.Run.State == db.RunCancelled {
		result.Outcome = db.AttemptCancelled
		return result, ErrRunCancelled
	}
	if receipt.Run.State != db.RunExpired {
		return result, &db.CommandRejection{Code: "run_unavailable", Message: "run is no longer queued"}
	}
	result.Outcome, result.Committed = db.AttemptExpired, true
	return result, ErrRunExpired
}
