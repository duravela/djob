package db

import (
	"fmt"
	"time"

	"go.etcd.io/bbolt"
)

func cancelRun(tx *bbolt.Tx, command StateCommand, result *StateResult) error {
	c := command.CancelRun
	if !validIdentity(c.RunID) || len(c.Reason) > 4096 {
		return reject("invalid", "cancellation requires a run ID and a reason of at most 4096 bytes")
	}
	run, err := readRecord[Run](tx, runsBucket, c.RunID)
	if err != nil {
		return err
	}
	// A successful/failed/cancelled run is already terminal. Keep its confirmed
	// outcome. Unknown runs retain cancellation intent to forbid an unsafe retry.
	if !run.CancellationRequested() && (unstartedRun(*run) || run.State == RunRunning || run.State == RunUnknown) {
		run.CancelRequestedAt, run.CancelRequestID, run.CancelReason = command.At, command.RequestID, c.Reason
		if unstartedRun(*run) {
			if run.ActiveAttemptID != "" {
				return fmt.Errorf("ready run has an active attempt")
			}
			run.State = RunCancelled
			run.RetryAt = time.Time{}
		}
		// Cancellation is ordered by commitment; a delayed client timestamp must
		// not race the worker's clock or move the run's transition time backward.
		run.LastTransition = later(run.LastTransition, command.At)
		if err := writeRunRecord(tx, *run); err != nil {
			return err
		}
	}
	result.Run = run
	return nil
}

func startAttempt(tx *bbolt.Tx, command StateCommand, result *StateResult) error {
	c := command.StartAttempt
	if c.RetryAware && !c.DeadlineAware {
		return reject("invalid", "retry-aware start requires deadline-aware semantics")
	}
	channel, err := ownedChannel(tx, c.ChannelOwner, command.At)
	if err != nil {
		return err
	}
	run, err := readRecord[Run](tx, runsBucket, c.RunID)
	if err != nil {
		return err
	}
	attempt, err := readRecord[Attempt](tx, attemptsBucket, c.AttemptID)
	if err != nil {
		return err
	}
	if channel.State != ChannelOwned || channel.ActiveAttemptID != c.AttemptID || run.ChannelID != channel.ID || run.State != RunRunning || run.ActiveAttemptID != c.AttemptID ||
		attempt.RunID != run.ID || attempt.OwnerID != c.OwnerID || attempt.Epoch != c.Epoch || attempt.ClaimToken != c.ClaimToken || attempt.Outcome != AttemptRunning {
		return reject("stale_attempt", "start does not match the current channel and run claim")
	}
	if run.CancellationRequested() {
		return reject("cancel_requested", "cancellation committed before execution authorization")
	}
	if run.RetryPolicy != nil && !c.RetryAware {
		return reject("invalid", "run requires retry-aware start authorization")
	}
	if !run.StartDeadline.IsZero() && !c.DeadlineAware {
		return reject("invalid", "run requires deadline-aware start authorization")
	}
	if run.StartDeadlinePassed(command.At) {
		return reject("start_deadline_expired", "run start deadline passed before execution authorization")
	}
	if !attempt.AuthorizedAt.IsZero() {
		return reject("already_started", "an attempt can be authorized only once")
	}
	if !validIdentity(c.ClaimToken) {
		return reject("invalid", "claim token is required")
	}
	attempt.AuthorizedAt, channel.LastTransition = command.At, command.At
	if err := writeRecord(tx, attemptsBucket, attempt.ID, attempt); err != nil {
		return err
	}
	if err := writeRecord(tx, channelsBucket, channel.ID, channel); err != nil {
		return err
	}
	result.Channel, result.Run, result.Attempt = channel, run, attempt
	return nil
}

func later(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
