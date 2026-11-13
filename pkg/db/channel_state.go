package db

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.etcd.io/bbolt"
)

var (
	channelsBucket         = []byte("channels")
	runsBucket             = []byte("runs")
	attemptsBucket         = []byte("attempts")
	requestsBucket         = []byte("state_requests")
	resolutionsBucket      = []byte("state_resolutions")
	channelStateVersionKey = []byte("channel_state_version")
	channelBuckets         = [][]byte{channelsBucket, runsBucket, attemptsBucket, requestsBucket, resolutionsBucket}
)

// Older indexed databases/snapshots can gain empty channel buckets. Once the
// feature marker is present, a missing bucket is corruption, not an empty queue.
func initializeChannelState(tx *bbolt.Tx) error {
	metadata := tx.Bucket(metadataBucket)
	version := metadata.Get(channelStateVersionKey)
	if version != nil {
		if !bytes.Equal(version, []byte{1}) && !bytes.Equal(version, []byte{2}) && !bytes.Equal(version, []byte{3}) && !bytes.Equal(version, []byte{4}) {
			return fmt.Errorf("unsupported channel state version")
		}
		for _, name := range channelBuckets {
			if tx.Bucket(name) == nil {
				return fmt.Errorf("channel state is missing bucket %q", name)
			}
		}
		return nil
	}
	for _, name := range channelBuckets {
		if b := tx.Bucket(name); b != nil && b.Stats().KeyN != 0 {
			return fmt.Errorf("channel bucket %q has data without a format marker", name)
		}
		if _, err := tx.CreateBucketIfNotExists(name); err != nil {
			return err
		}
	}
	return metadata.Put(channelStateVersionKey, []byte{1})
}

func reject(code, message string) error { return &CommandRejection{Code: code, Message: message} }

func readRecord[T any](tx *bbolt.Tx, bucket []byte, id string) (*T, error) {
	data := tx.Bucket(bucket).Get([]byte(id))
	if data == nil {
		return nil, reject("not_found", fmt.Sprintf("%s %q does not exist", bucket, id))
	}
	var record T
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&record); err != nil {
		return nil, fmt.Errorf("decode %s %q: %w", bucket, id, err)
	}
	return &record, nil
}

func writeRecord(tx *bbolt.Tx, bucket []byte, id string, record any) error {
	var data bytes.Buffer
	if err := gob.NewEncoder(&data).Encode(record); err != nil {
		return err
	}
	return tx.Bucket(bucket).Put([]byte(id), data.Bytes())
}

func applyStateCommand(tx *bbolt.Tx, command StateCommand) (StateResult, error) {
	if err := command.Validate(); err != nil {
		return StateResult{}, fmt.Errorf("invalid state envelope: %w", err)
	}
	fingerprint, err := command.Fingerprint()
	if err != nil {
		return StateResult{}, fmt.Errorf("fingerprint state command: %w", err)
	}
	// Raise the marker monotonically. Older binaries must reject new fields and
	// states rather than silently dropping them while rewriting records.
	marker := byte(1)
	if command.CommandVersion() >= RetryCommandVersion {
		marker = 4
	} else if command.CommandVersion() >= DeadlineCommandVersion {
		marker = 3
	} else if command.CommandVersion() >= CancellationCommandVersion {
		marker = 2
	}
	if tx.Bucket(metadataBucket).Get(channelStateVersionKey)[0] < marker {
		if err := tx.Bucket(metadataBucket).Put(channelStateVersionKey, []byte{marker}); err != nil {
			return StateResult{}, err
		}
	}
	result := StateResult{RequestID: command.RequestID, Fingerprint: fingerprint}
	if command.CommandVersion() >= WorkerLifecycleCommandVersion && tx.Bucket(metadataBucket).Get(formatKey)[0] < 18 {
		if err := tx.Bucket(metadataBucket).Put(formatKey, []byte{18}); err != nil {
			return StateResult{}, err
		}
	}
	if command.CommandVersion() >= WorkerReleaseCommandVersion && tx.Bucket(metadataBucket).Get(formatKey)[0] < 17 {
		if err := tx.Bucket(metadataBucket).Put(formatKey, []byte{17}); err != nil {
			return StateResult{}, err
		}
	}
	if (command.WorkerEnrollment != nil || command.WorkerAdmission != nil) && tx.Bucket(metadataBucket).Get(formatKey)[0] < 13 {
		if err := tx.Bucket(metadataBucket).Put(formatKey, []byte{13}); err != nil {
			return StateResult{}, err
		}
	}
	if (command.WorkerDeployment != nil || command.WorkerAdmission != nil && command.WorkerAdmission.DeploymentID != "") && tx.Bucket(metadataBucket).Get(formatKey)[0] < 14 {
		if err := tx.Bucket(metadataBucket).Put(formatKey, []byte{14}); err != nil {
			return StateResult{}, err
		}
	}
	if command.Job != nil {
		// Feature activation is monotonic, including durable rejected requests.
		required := byte(2)
		if command.CommandVersion() >= JobTriggerCommandVersion {
			required = 3
		}
		if command.CommandVersion() >= JobActivationCommandVersion {
			required = 4
		}
		if command.CommandVersion() >= JobQuotaCommandVersion {
			required = 5
		}
		if command.CommandVersion() >= JobRetentionCommandVersion {
			required = 6
		}
		if command.CommandVersion() >= JobReceiptCommandVersion {
			required = 7
		}
		if command.CommandVersion() >= JobConfigurationCommandVersion {
			required = 8
		}
		if command.CommandVersion() >= JobConfigurationReceiptCommandVersion {
			required = 9
		}
		if command.CommandVersion() >= JobTriggerRetirementCommandVersion {
			required = 10
		}
		if command.CommandVersion() >= JobConfigurationRetirementCommandVersion {
			required = 11
		}
		if tx.Bucket(metadataBucket).Get(formatKey)[0] < required {
			if err := tx.Bucket(metadataBucket).Put(formatKey, []byte{required}); err != nil {
				return StateResult{}, err
			}
		}
	}

	if tx.Bucket(requestsBucket).Get([]byte(command.RequestID)) != nil {
		previous, err := readRecord[StateResult](tx, requestsBucket, command.RequestID)
		if err != nil {
			return StateResult{}, err
		}
		if previous.Fingerprint == fingerprint {
			return *previous, nil
		}
		result.Rejection = &CommandRejection{Code: "request_conflict", Message: "request ID was already used with a different command"}
		return result, nil
	}
	if tx.Bucket(jobCompactReceiptsBucket).Get([]byte(command.RequestID)) != nil {
		previous, err := readRecord[compactJobReceipt](tx, jobCompactReceiptsBucket, command.RequestID)
		if err != nil {
			return StateResult{}, err
		}
		if previous.RequestID != command.RequestID {
			return StateResult{}, fmt.Errorf("inconsistent compact receipt identity")
		}
		if previous.Fingerprint == fingerprint {
			return previous.result(), nil
		}
		result.Rejection = &CommandRejection{Code: "request_conflict", Message: "request ID was already used with a different command"}
		return result, nil
	}
	command.At = command.At.UTC()
	var transitionErr error
	if command.WorkerDeployment != nil {
		transitionErr = transitionWorkerDeployment(tx, command, &result)
	} else if command.WorkerEnrollment != nil {
		transitionErr = transitionWorkerEnrollment(tx, command, &result)
	} else if command.Job != nil {
		if command.Job.Claim != nil {
			transitionErr = checkWorkerClaim(tx, command)
		}
		if transitionErr == nil {
			transitionErr = transitionJob(tx, command, &result)
		}
	} else if command.PutSchedule != nil || command.PauseSchedule != nil || command.Materialize != nil {
		transitionErr = transitionSchedule(tx, command, &result)
	} else if command.CancelRun != nil {
		transitionErr = cancelRun(tx, command, &result)
	} else if command.StartAttempt != nil {
		transitionErr = startAttempt(tx, command, &result)
	} else if command.ExpireRun != nil {
		transitionErr = expireRun(tx, command, &result)
	} else if command.ReleaseRetry != nil {
		transitionErr = releaseRetry(tx, command, &result)
	} else {
		transitionErr = transitionState(tx, command, &result)
	}
	if err := transitionErr; err != nil {
		var rejection *CommandRejection
		if !errors.As(err, &rejection) {
			return StateResult{}, err
		}
		result.Rejection = rejection
		result.Channel, result.Run, result.Attempt = nil, nil, nil
		result.Schedule, result.Runs = nil, nil
		result.Job = nil
		result.WorkerEnrollment = nil
		result.WorkerDeployment = nil
	}
	if err := writeStateReceipt(tx, result); err != nil {
		return StateResult{}, err
	}
	return result, nil
}

func checkTime(channel *CommunicationChannel, at time.Time) error {
	if at.Before(channel.LastTransition) {
		return reject("stale_time", "command time precedes the channel's last transition")
	}
	return nil
}

func ownedChannel(tx *bbolt.Tx, owner ChannelOwner, at time.Time) (*CommunicationChannel, error) {
	c, err := readRecord[CommunicationChannel](tx, channelsBucket, owner.ChannelID)
	if err != nil {
		return nil, err
	}
	if err := checkTime(c, at); err != nil {
		return nil, err
	}
	if c.State != ChannelOwned && c.State != ChannelDraining {
		return nil, reject("not_owned", "channel is not owned")
	}
	if c.Epoch != owner.Epoch || c.OwnerID != owner.OwnerID || !validIdentity(owner.OwnerID) {
		return nil, reject("stale_owner", "owner incarnation or epoch does not match")
	}
	if !at.Before(c.LeaseUntil) {
		return nil, reject("lease_expired", "ownership expired; physical access requires resolution")
	}
	return c, nil
}

func validResolution(actor, reason string) error {
	if !validIdentity(actor) || strings.TrimSpace(reason) == "" || len(reason) > 4096 {
		return reject("invalid", "resolution requires an actor and a reason of at most 4096 bytes")
	}
	return nil
}

func transitionState(tx *bbolt.Tx, command StateCommand, result *StateResult) error {
	var channel *CommunicationChannel
	var run *Run
	var attempt *Attempt
	var resolution *Resolution
	var err error
	writeChannel, writeRun, writeAttempt := false, false, false
	switch {
	case command.Configure != nil:
		c := command.Configure
		if !validIdentity(c.ID) || c.ID != c.GatewayID || c.Protocol == "" || c.Endpoint == "" {
			return reject("invalid", "channel ID must equal the gateway ID; protocol and endpoint are required")
		}
		if tx.Bucket(channelsBucket).Get([]byte(c.ID)) == nil {
			if c.ExpectedRevision != 0 {
				return reject("revision_conflict", "new channel requires revision zero")
			}
			channel = &CommunicationChannel{ID: c.ID, GatewayID: c.GatewayID, State: ChannelAvailable}
		} else {
			channel, err = readRecord[CommunicationChannel](tx, channelsBucket, c.ID)
			if err != nil {
				return err
			}
			if channel.Revision != c.ExpectedRevision {
				return reject("revision_conflict", "channel revision does not match")
			}
			if channel.State != ChannelAvailable || channel.ActiveAttemptID != "" {
				return reject("channel_busy", "release channel ownership before reconfiguration")
			}
			if err := checkTime(channel, command.At); err != nil {
				return err
			}
		}
		if channel.Revision == ^uint64(0) {
			return reject("exhausted", "channel revision exhausted")
		}
		channel.Protocol, channel.Endpoint = c.Protocol, c.Endpoint
		channel.Revision++
		writeChannel = true

	case command.CreateRun != nil || command.EnqueueRun != nil:
		c := command.CreateRun
		if c == nil {
			c = command.EnqueueRun
		}
		if err := c.RetryPolicy.Validate(); err != nil {
			return err
		}
		if !validIdentity(c.ID) || strings.HasPrefix(c.ID, "scheduled/") || !validIdentity(c.ChannelID) || c.Executor == "" {
			return reject("invalid", "run ID, channel ID, and executor are required")
		}
		if command.EnqueueRun != nil && (!validIdentity(c.Executor) || len(c.Payload) > 64<<10) {
			return reject("invalid", "enqueue requires a valid executor and a payload of at most 64 KiB")
		}
		if tx.Bucket(runsBucket).Get([]byte(c.ID)) != nil {
			return reject("run_exists", "run already exists")
		}
		channel, err = readRecord[CommunicationChannel](tx, channelsBucket, c.ChannelID)
		if err != nil {
			return err
		}
		if command.EnqueueRun == nil {
			if err := checkTime(channel, command.At); err != nil {
				return err
			}
		}
		run = &Run{ID: c.ID, ChannelID: c.ChannelID, Executor: c.Executor, Payload: c.Payload, State: RunReady, CreatedAt: command.At}
		run.RetryPolicy = c.RetryPolicy.Clone()
		if c.StartDeadline != nil {
			if !validDeadline(*c.StartDeadline) {
				return reject("invalid", "start deadline must be a nonzero timestamp in years 1..9999")
			}
			run.StartDeadline = c.StartDeadline.UTC()
			if run.StartDeadlinePassed(command.At) {
				run.State = RunExpired
			}
		}
		writeChannel, writeRun = command.EnqueueRun == nil, true

	case command.Acquire != nil:
		c := command.Acquire
		if !validIdentity(c.OwnerID) || !c.LeaseUntil.After(command.At) {
			return reject("invalid", "owner incarnation and a future lease deadline are required")
		}
		channel, err = readRecord[CommunicationChannel](tx, channelsBucket, c.ChannelID)
		if err != nil {
			return err
		}
		if err := checkTime(channel, command.At); err != nil {
			return err
		}
		if channel.Epoch != c.ExpectedEpoch {
			return reject("stale_owner", "channel epoch does not match")
		}
		if channel.State != ChannelAvailable || channel.ActiveAttemptID != "" {
			return reject("channel_busy", "channel is unavailable; expiry alone does not authorize takeover")
		}
		if channel.Epoch == ^uint64(0) {
			return reject("exhausted", "channel epoch exhausted")
		}
		channel.Epoch++
		channel.State, channel.OwnerID, channel.LeaseUntil = ChannelOwned, c.OwnerID, c.LeaseUntil.UTC()
		writeChannel = true

	case command.Renew != nil:
		c := command.Renew
		channel, err = ownedChannel(tx, ChannelOwner{ChannelID: c.ChannelID, Epoch: c.ExpectedEpoch, OwnerID: c.OwnerID}, command.At)
		if err != nil {
			return err
		}
		if !c.LeaseUntil.After(command.At) || c.LeaseUntil.Before(channel.LeaseUntil) {
			return reject("invalid", "renewal cannot shorten or expire the lease")
		}
		channel.LeaseUntil = c.LeaseUntil.UTC()
		writeChannel = true

	case command.Drain != nil:
		channel, err = ownedChannel(tx, *command.Drain, command.At)
		if err != nil {
			return err
		}
		channel.State = ChannelDraining
		writeChannel = true

	case command.Release != nil:
		c := command.Release
		channel, err = ownedChannel(tx, c.ChannelOwner, command.At)
		if err != nil {
			return err
		}
		if err := noJobChannelReservation(tx, channel); err != nil {
			return err
		}
		if !c.Quiescent || channel.ActiveAttemptID != "" {
			return reject("channel_busy", "release requires confirmed quiescence and no active attempt")
		}
		channel.State, channel.OwnerID, channel.LeaseUntil = ChannelAvailable, "", time.Time{}
		writeChannel = true

	case command.Expire != nil:
		c := command.Expire
		channel, err = readRecord[CommunicationChannel](tx, channelsBucket, c.ChannelID)
		if err != nil {
			return err
		}
		if err := checkTime(channel, command.At); err != nil {
			return err
		}
		if channel.Epoch != c.Epoch {
			return reject("stale_owner", "channel epoch does not match")
		}
		if channel.State != ChannelOwned && channel.State != ChannelDraining {
			return reject("not_owned", "channel is not owned")
		}
		if command.At.Before(channel.LeaseUntil) {
			return reject("not_expired", "ownership has not expired")
		}
		channel.State = ChannelBlocked
		writeChannel = true
		if channel.ActiveAttemptID != "" {
			attempt, err = readRecord[Attempt](tx, attemptsBucket, channel.ActiveAttemptID)
			if err != nil {
				return fmt.Errorf("load active attempt: %v", err)
			}
			run, err = readRecord[Run](tx, runsBucket, attempt.RunID)
			if err != nil {
				return fmt.Errorf("load active run: %v", err)
			}
			if run.State != RunRunning || run.ActiveAttemptID != attempt.ID || attempt.Outcome != AttemptRunning || attempt.Epoch != channel.Epoch || attempt.OwnerID != channel.OwnerID {
				return fmt.Errorf("inconsistent active attempt for channel %q", channel.ID)
			}
			attempt.Outcome, attempt.FinishedAt, attempt.Quiescent = AttemptUnknown, command.At, false
			run.State, run.ActiveAttemptID = RunUnknown, ""
			writeRun, writeAttempt = true, true
		}

	case command.Claim != nil:
		c := command.Claim
		if !validIdentity(c.AttemptID) || !validIdentity(c.ClaimToken) {
			return reject("invalid", "attempt ID and claim token are required")
		}
		channel, err = ownedChannel(tx, c.ChannelOwner, command.At)
		if err != nil {
			return err
		}
		if err := noJobChannelReservation(tx, channel); err != nil {
			return err
		}
		if channel.State != ChannelOwned || channel.ActiveAttemptID != "" {
			return reject("channel_busy", "channel is occupied or draining")
		}
		run, err = readRecord[Run](tx, runsBucket, c.RunID)
		if err != nil {
			return err
		}
		if run.ChannelID != channel.ID || run.State != RunReady {
			return reject("run_unavailable", "run is not ready on this channel")
		}
		if run.StartDeadlinePassed(command.At) {
			return reject("start_deadline_expired", "run start deadline has passed")
		}
		if run.RetryPolicy != nil && run.AttemptCount >= uint64(run.RetryPolicy.MaxAttempts) {
			return reject("attempts_exhausted", "run attempt limit reached")
		}
		if command.At.Before(run.LastTransition) {
			return reject("stale_time", "claim precedes the run's last transition")
		}
		if tx.Bucket(attemptsBucket).Get([]byte(c.AttemptID)) != nil {
			return reject("attempt_exists", "attempt ID cannot be reused")
		}
		if run.AttemptCount == ^uint64(0) {
			return reject("exhausted", "attempt counter exhausted")
		}
		run.AttemptCount++
		run.State, run.ActiveAttemptID, run.LastAttemptID = RunRunning, c.AttemptID, c.AttemptID
		attempt = &Attempt{ID: c.AttemptID, RunID: run.ID, Number: run.AttemptCount, OwnerID: c.OwnerID, Epoch: c.Epoch,
			ClaimToken: c.ClaimToken, StartedAt: command.At, Outcome: AttemptRunning}
		channel.ActiveAttemptID = c.AttemptID
		writeChannel, writeRun, writeAttempt = true, true, true

	case command.Finish != nil:
		c := command.Finish
		if c.RetryAware && !c.DeadlineAware {
			return reject("invalid", "retry-aware completion requires deadline-aware semantics")
		}
		if c.RetryClass != "" && (!c.RetryAware || c.RetryClass != RetryNotSent || c.Outcome != AttemptFailed || len(c.Result) != 0) {
			return reject("invalid", "retry classification requires an aware failed attempt with no result and confirmed no request bytes")
		}
		if c.DeadlineAware && !c.CancellationAware {
			return reject("invalid", "deadline-aware completion requires cancellation-aware semantics")
		}
		if c.Outcome != AttemptSucceeded && c.Outcome != AttemptFailed && c.Outcome != AttemptUnknown && !(c.CancellationAware && c.Outcome == AttemptCancelled) && !(c.DeadlineAware && c.Outcome == AttemptExpired) {
			return reject("invalid", "finish requires a terminal attempt outcome")
		}
		if c.NoDelivery && (!c.CancellationAware || (c.Outcome != AttemptFailed && c.Outcome != AttemptCancelled && c.Outcome != AttemptExpired) || len(c.Result) != 0) {
			return reject("invalid", "no-delivery completion requires cancellation-aware semantics, a stopped outcome, and no result data")
		}
		if (c.Outcome == AttemptCancelled || c.Outcome == AttemptExpired) && !c.NoDelivery {
			return reject("invalid", "cancelled or expired requires confirmation that exchange never began")
		}
		channel, err = ownedChannel(tx, c.ChannelOwner, command.At)
		if err != nil {
			return err
		}
		run, err = readRecord[Run](tx, runsBucket, c.RunID)
		if err != nil {
			return err
		}
		attempt, err = readRecord[Attempt](tx, attemptsBucket, c.AttemptID)
		if err != nil {
			return err
		}
		if run.ChannelID != channel.ID || run.State != RunRunning || run.ActiveAttemptID != c.AttemptID || channel.ActiveAttemptID != c.AttemptID ||
			attempt.RunID != run.ID || attempt.OwnerID != c.OwnerID || attempt.Epoch != c.Epoch || attempt.ClaimToken != c.ClaimToken || attempt.Outcome != AttemptRunning {
			return reject("stale_attempt", "completion does not match the current claim")
		}
		outcome := c.Outcome
		if run.RetryPolicy != nil && !c.RetryAware {
			return reject("invalid", "run requires retry-aware completion")
		}
		if !run.StartDeadline.IsZero() && !c.DeadlineAware {
			return reject("invalid", "run requires deadline-aware completion")
		}
		if c.Outcome == AttemptExpired && (!run.StartDeadlinePassed(command.At) || run.CancellationRequested()) {
			return reject("invalid", "expired completion requires a passed deadline and no cancellation intent")
		}
		if c.CancellationAware {
			if c.Outcome == AttemptCancelled && !run.CancellationRequested() {
				return reject("invalid", "cancelled completion requires a durable cancellation request")
			}
			if c.NoDelivery && run.CancellationRequested() {
				outcome = AttemptCancelled
			} else if c.NoDelivery && c.DeadlineAware && run.StartDeadlinePassed(command.At) {
				outcome = AttemptExpired
			}
			if !c.NoDelivery && attempt.AuthorizedAt.IsZero() {
				return reject("invalid", "exchange completion requires committed start authorization")
			}
			attempt.NoDelivery = c.NoDelivery
		}
		attempt.Outcome, attempt.Result, attempt.FinishedAt, attempt.Quiescent = outcome, c.Result, command.At, c.Quiescent
		attempt.Error = c.Error
		attempt.RetryClass = c.RetryClass
		run.RetryAt = time.Time{}
		switch outcome {
		case AttemptSucceeded:
			run.State = RunSucceeded
		case AttemptFailed:
			run.State = RunFailed
		case AttemptUnknown:
			run.State = RunUnknown
		case AttemptCancelled:
			run.State = RunCancelled
		case AttemptExpired:
			run.State = RunExpired
		}
		if outcome == AttemptFailed && c.RetryClass == RetryNotSent && c.Quiescent && run.RetryPolicy != nil &&
			run.AttemptCount < uint64(run.RetryPolicy.MaxAttempts) && !run.CancellationRequested() && !run.StartDeadlinePassed(command.At) {
			at := command.At.Add(run.RetryPolicy.Backoff(run.AttemptCount)).UTC()
			if validDeadline(at) {
				run.State, run.RetryAt, attempt.RetryAt = RunRetryWait, at, at
			}
		}
		run.ActiveAttemptID = ""
		if c.Quiescent {
			channel.ActiveAttemptID = ""
		} else {
			channel.State = ChannelBlocked
		}
		writeChannel, writeRun, writeAttempt = true, true, true

	case command.ResolveChannel != nil:
		c := command.ResolveChannel
		if err := validResolution(c.Actor, c.Reason); err != nil {
			return err
		}
		if !c.Quiescent {
			return reject("not_quiescent", "resolution requires confirmation that previous access has ended")
		}
		channel, err = readRecord[CommunicationChannel](tx, channelsBucket, c.ChannelID)
		if err != nil {
			return err
		}
		if err := checkTime(channel, command.At); err != nil {
			return err
		}
		if channel.State != ChannelBlocked {
			return reject("not_blocked", "only blocked channels require resolution")
		}
		if channel.Epoch != c.Epoch {
			return reject("stale_owner", "channel epoch does not match")
		}
		resolution = &Resolution{RequestID: command.RequestID, ChannelID: c.ChannelID, AttemptID: channel.ActiveAttemptID,
			Epoch: c.Epoch, At: command.At, Actor: c.Actor, Reason: c.Reason, Quiescent: true}
		channel.State, channel.OwnerID, channel.ActiveAttemptID, channel.LeaseUntil = ChannelAvailable, "", "", time.Time{}
		writeChannel = true

	case command.ResolveRun != nil:
		c := command.ResolveRun
		if err := validResolution(c.Actor, c.Reason); err != nil {
			return err
		}
		if c.State != RunReady && c.State != RunSucceeded && c.State != RunFailed {
			return reject("invalid", "resolution must authorize retry or record a final run outcome")
		}
		run, err = readRecord[Run](tx, runsBucket, c.RunID)
		if err != nil {
			return err
		}
		if run.State != RunUnknown {
			return reject("not_unknown", "only unknown runs require resolution")
		}
		if c.State == RunReady && run.CancellationRequested() {
			return reject("cancel_requested", "a cancelled run cannot be authorized for another attempt")
		}
		if c.State == RunReady && run.StartDeadlinePassed(command.At) {
			return reject("start_deadline_expired", "a run past its start deadline cannot be retried")
		}
		if c.State == RunReady && run.RetryPolicy != nil && run.AttemptCount >= uint64(run.RetryPolicy.MaxAttempts) {
			return reject("attempts_exhausted", "run attempt limit reached")
		}
		if command.At.Before(run.LastTransition) {
			return reject("stale_time", "resolution precedes the run's last transition")
		}
		if !validIdentity(c.AttemptID) || run.LastAttemptID != c.AttemptID {
			return reject("stale_attempt", "resolution does not match the uncertain attempt")
		}
		attempt, err = readRecord[Attempt](tx, attemptsBucket, c.AttemptID)
		if err != nil {
			return err
		}
		if command.At.Before(attempt.FinishedAt) {
			return reject("stale_time", "resolution precedes the uncertain outcome")
		}
		channel, err = readRecord[CommunicationChannel](tx, channelsBucket, run.ChannelID)
		if err != nil {
			return err
		}
		if err := checkTime(channel, command.At); err != nil {
			return err
		}
		if c.State == RunReady && (channel.State == ChannelBlocked || channel.ActiveAttemptID == run.LastAttemptID) {
			return reject("channel_busy", "resolve physical access before authorizing another attempt")
		}
		run.State = c.State
		resolution = &Resolution{RequestID: command.RequestID, ChannelID: channel.ID, RunID: run.ID, AttemptID: c.AttemptID, Epoch: channel.Epoch,
			RunState: c.State, At: command.At, Actor: c.Actor, Reason: c.Reason}
		writeChannel, writeRun = true, true
	}
	// Every condition is checked before the first write. Rejections can therefore
	// be durably recorded without leaving a partial ownership or claim transition.
	if writeRun {
		if run.State == RunReady {
			run.ReadySince = command.At
		}
		if err := checkRunCapacity(tx, *run); err != nil {
			return err
		}
	}
	if writeChannel {
		channel.LastTransition = command.At
		if err := writeRecord(tx, channelsBucket, channel.ID, channel); err != nil {
			return err
		}
	}
	if writeRun {
		if command.Finish != nil && command.Finish.CancellationAware {
			run.LastTransition = later(run.LastTransition, command.At)
		} else {
			run.LastTransition = command.At
		}
		if err := writeRunRecord(tx, *run); err != nil {
			return err
		}
	}
	if writeAttempt {
		if err := writeRecord(tx, attemptsBucket, attempt.ID, attempt); err != nil {
			return err
		}
	}
	if resolution != nil {
		if err := writeRecord(tx, resolutionsBucket, resolution.RequestID, resolution); err != nil {
			return err
		}
	}
	result.Channel, result.Run, result.Attempt = channel, run, attempt
	return nil
}

func getStateRecord[T any](q *DB, bucket []byte, id string) (*T, error) {
	var record *T
	err := q.db.View(func(tx *bbolt.Tx) error {
		var err error
		record, err = readRecord[T](tx, bucket, id)
		return err
	})
	return record, err
}

func (q *DB) GetChannel(id string) (*CommunicationChannel, error) {
	return getStateRecord[CommunicationChannel](q, channelsBucket, id)
}
func (q *DB) GetRun(id string) (*Run, error) { return getStateRecord[Run](q, runsBucket, id) }
func (q *DB) GetAttempt(id string) (*Attempt, error) {
	return getStateRecord[Attempt](q, attemptsBucket, id)
}
func (q *DB) GetResolution(id string) (*Resolution, error) {
	return getStateRecord[Resolution](q, resolutionsBucket, id)
}
