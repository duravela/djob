package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"go.etcd.io/bbolt"
)

func changeJobCounter(tx *bbolt.Tx, s *jobChanges, key string, add bool, maximum uint64) error {
	data := tx.Bucket(jobCountersBucket).Get([]byte(key))
	var n uint64
	if data != nil {
		if len(data) != 8 {
			return fmt.Errorf("invalid job quota counter")
		}
		n = binary.BigEndian.Uint64(data)
	}
	if add {
		if n == math.MaxUint64 || (maximum != 0 && n >= maximum) {
			return reject("quota_exceeded", "concurrent job quota reached")
		}
		n++
	} else {
		if n == 0 {
			return fmt.Errorf("job quota counter underflow")
		}
		n--
	}
	s.index(jobCountersBucket, key, binary.BigEndian.AppendUint64(nil, n))
	return nil
}

func changeJobQuotas(tx *bbolt.Tx, s *jobChanges, run *JobRun, add bool) error {
	tenant, err := requireJobTenant(tx, run.TenantID)
	if err != nil {
		return err
	}
	if err := changeJobCounter(tx, s, jobKey("tenant", run.TenantID), add, tenant.MaxRunning); err != nil {
		return err
	}
	if run.Group != nil {
		maximum, err := jobGroupQuotaMaximum(tx, run.TenantID, run.Group.ID)
		if err != nil {
			return err
		}
		return changeJobCounter(tx, s, jobKey("group", run.TenantID, run.Group.ID), add, maximum)
	}
	return nil
}

func claimJob(tx *bbolt.Tx, s *jobChanges, at time.Time, c ClaimJob, r *JobResult) error {
	if !validIdentity(c.TenantID) || !validIdentity(c.RunID) || !validIdentity(c.AttemptID) || !validIdentity(c.OwnerID) || !c.LeaseUntil.After(at) || c.LeaseUntil.Sub(at) > time.Hour {
		return reject("invalid", "claim requires tenant, run, fresh attempt and owner identities, and a future lease of at most one hour")
	}
	run, err := readRecord[JobRun](tx, jobRunsBucket, jobKey(c.TenantID, c.RunID))
	if err != nil {
		return err
	}
	before := *run
	if run.Status != JobReady || run.OwnerID != "" || run.CancelRequested {
		return reject("not_ready", "job is not ready to claim")
	}
	if at.Before(run.LastTransition) {
		return reject("stale_time", "claim predates the run's last transition")
	}
	if run.AttemptCount == math.MaxUint64 {
		return reject("invalid", "attempt counter exhausted")
	}
	if tx.Bucket(jobAttemptsBucket).Get([]byte(jobKey(c.TenantID, c.RunID, c.AttemptID))) != nil {
		return reject("stale_attempt", "attempt identity has already been used")
	}
	if err := checkJobGrants(tx, c.TenantID, run.Resources); err != nil {
		return err
	}
	if err := changeJobQuotas(tx, s, run, true); err != nil {
		return err
	}
	for _, id := range run.Resources {
		resource, err := readRecord[JobResource](tx, jobResourcesBucket, id)
		if err != nil {
			return err
		}
		if resource.State != ResourceAvailable {
			return reject("resource_busy", "a required resource is owned or blocked")
		}
		if resource.Epoch == math.MaxUint64 {
			return reject("invalid", "resource epoch exhausted")
		}
		if at.Before(resource.LastTransition) {
			return reject("stale_time", "claim predates a resource transition")
		}
		if resource.Spec.ChannelID != "" {
			channel, err := readRecord[CommunicationChannel](tx, channelsBucket, resource.Spec.ChannelID)
			if err != nil {
				return err
			}
			if err := checkTime(channel, at); err != nil {
				return err
			}
			if channel.State != ChannelAvailable || channel.ActiveAttemptID != "" {
				return reject("channel_busy", "the gateway authority is owned or blocked")
			}
			if channel.Epoch == math.MaxUint64 {
				return reject("invalid", "gateway epoch exhausted")
			}
			channel.Epoch++
			channel.State, channel.OwnerID, channel.LeaseUntil, channel.LastTransition = ChannelOwned, c.OwnerID, c.LeaseUntil, at
			resource.ChannelEpoch = channel.Epoch
			s.put(channelsBucket, channel.ID, channel)
		}
		resource.Epoch++
		resource.State, resource.TenantID, resource.RunID, resource.AttemptID, resource.OwnerID = ResourceOwned, c.TenantID, c.RunID, c.AttemptID, c.OwnerID
		resource.LeaseUntil, resource.LastTransition = c.LeaseUntil, at
		s.put(jobResourcesBucket, id, resource)
	}
	run.OwnerID, run.AttemptID, run.LastAttemptID, run.LeaseUntil = c.OwnerID, c.AttemptID, c.AttemptID, c.LeaseUntil
	run.Status, run.LastTransition = JobRunning, at
	run.AttemptCount++
	attempt := &JobAttempt{TenantID: run.TenantID, RunID: run.ID, ID: c.AttemptID, OwnerID: c.OwnerID, Number: run.AttemptCount, Status: JobRunning, StartedAt: at}
	s.put(jobAttemptsBucket, jobKey(run.TenantID, run.ID, attempt.ID), attempt)
	saveJobRun(s, &before, run)
	r.Run = run
	return nil
}

func ownedJob(tx *bbolt.Tx, owner JobOwner, at time.Time) (*JobRun, error) {
	run, err := readRecord[JobRun](tx, jobRunsBucket, jobKey(owner.TenantID, owner.RunID))
	if err != nil {
		return nil, err
	}
	if !validIdentity(owner.OwnerID) || run.OwnerID != owner.OwnerID || run.AttemptID != owner.AttemptID || (run.Status != JobRunning && run.Status != JobWaiting) {
		return nil, reject("stale_owner", "job attempt or owner incarnation changed")
	}
	if at.Before(run.LastTransition) {
		return nil, reject("stale_time", "command predates the run's last transition")
	}
	if !at.Before(run.LeaseUntil) {
		return nil, reject("lease_expired", "job lease expired")
	}
	for _, id := range run.Resources {
		resource, err := ownedJobResource(tx, run, id, at, false)
		if err != nil {
			return nil, err
		}
		if resource.Spec.ChannelID != "" {
			channel, err := ownedChannel(tx, ChannelOwner{ChannelID: resource.Spec.ChannelID, OwnerID: run.OwnerID, Epoch: resource.ChannelEpoch}, at)
			if err != nil {
				return nil, err
			}
			if channel.State != ChannelOwned || channel.ActiveAttemptID != "" {
				return nil, reject("channel_busy", "gateway is draining or occupied")
			}
		}
	}
	return run, nil
}

func ownedJobResource(tx *bbolt.Tx, run *JobRun, id string, at time.Time, expired bool) (*JobResource, error) {
	resource, err := readRecord[JobResource](tx, jobResourcesBucket, id)
	if err != nil {
		return nil, err
	}
	if resource.TenantID != run.TenantID || resource.RunID != run.ID || resource.AttemptID != run.AttemptID || resource.OwnerID != run.OwnerID || (resource.State != ResourceOwned && !(expired && resource.State == ResourceBlocked)) {
		return nil, reject("stale_owner", "resource allocation changed")
	}
	if at.Before(resource.LastTransition) {
		return nil, reject("stale_time", "command predates a resource transition")
	}
	if !expired && !at.Before(resource.LeaseUntil) {
		return nil, reject("lease_expired", "resource lease expired")
	}
	return resource, nil
}

func renewJob(tx *bbolt.Tx, s *jobChanges, at time.Time, c RenewJob, r *JobResult) error {
	run, err := ownedJob(tx, c.JobOwner, at)
	if err != nil {
		return err
	}
	if !c.LeaseUntil.After(at) || c.LeaseUntil.Before(run.LeaseUntil) || c.LeaseUntil.Sub(at) > time.Hour {
		return reject("invalid", "renewal must not shorten the lease and must be within one hour")
	}
	before := *run
	for _, id := range run.Resources {
		resource, err := readRecord[JobResource](tx, jobResourcesBucket, id)
		if err != nil {
			return err
		}
		resource.LeaseUntil, resource.LastTransition = c.LeaseUntil, at
		s.put(jobResourcesBucket, id, resource)
		if resource.Spec.ChannelID != "" {
			channel, err := readRecord[CommunicationChannel](tx, channelsBucket, resource.Spec.ChannelID)
			if err != nil {
				return err
			}
			channel.LeaseUntil, channel.LastTransition = c.LeaseUntil, at
			s.put(channelsBucket, channel.ID, channel)
		}
	}
	run.LeaseUntil, run.LastTransition = c.LeaseUntil, at
	saveJobRun(s, &before, run)
	r.Run = run
	return nil
}

func yieldJob(tx *bbolt.Tx, s *jobChanges, at time.Time, c YieldJob, r *JobResult) error {
	run, err := ownedJob(tx, c.JobOwner, at)
	if err != nil {
		return err
	}
	if len(run.Resources) != 0 || (run.PendingAction != nil && (run.PendingAction.Status == EffectAuthorized || run.PendingAction.Status == EffectUnknown)) {
		return reject("effect_uncertain", "only a resource-free checkpoint with known effects can yield")
	}
	before := *run
	run.Status, run.LastTransition = JobReady, at
	run.ReadySince = at
	if run.CancelRequested {
		run.Status = JobCancelled
	}
	if err := endJobAttempt(tx, s, run, at, run.Status); err != nil {
		return err
	}
	saveJobRun(s, &before, run)
	r.Run = run
	return nil
}

func nextJobEvent(tx *bbolt.Tx, run *JobRun) (*JobEvent, error) {
	if run.WaitKey == "" {
		return nil, nil
	}
	prefix := []byte(jobKey(run.TenantID, run.ID, run.WaitKey))
	k, v := tx.Bucket(jobEventKeysBucket).Cursor().Seek(prefix)
	if k == nil || !bytes.HasPrefix(k, prefix) {
		return nil, nil
	}
	return readRecord[JobEvent](tx, jobEventsBucket, string(v))
}

func advanceJob(tx *bbolt.Tx, s *jobChanges, at time.Time, c AdvanceJob, r *JobResult) error {
	run, err := ownedJob(tx, c.JobOwner, at)
	if err != nil {
		return err
	}
	before := *run
	if c.ExpectedStep != run.Step || run.Step == math.MaxUint64 {
		return reject("step_conflict", "checkpoint version changed")
	}
	if len(c.State) > 1<<20 || len(c.Input) > 1<<20 || len(c.Result) > 1<<20 || len(c.Error) > 4096 {
		return reject("invalid", "checkpoint, action, and result limits exceeded")
	}
	if run.PendingAction != nil && (run.PendingAction.Status == EffectAuthorized || run.PendingAction.Status == EffectUnknown) {
		if run.PendingAction.Status == EffectAuthorized || c.Kind != JobComplete || c.Status != JobUnknown {
			return reject("effect_uncertain", "an unresolved effect cannot be replayed or hidden by a new checkpoint")
		}
	}
	if run.CancelRequested && c.Kind != JobComplete {
		return reject("cancel_requested", "cancellation prevents further actions and checkpoints")
	}
	if c.EventID != "" {
		event, err := readRecord[JobEvent](tx, jobEventsBucket, jobKey(run.TenantID, run.ID, c.EventID))
		if err != nil {
			return err
		}
		if run.WaitKey == "" || event.Key != run.WaitKey || !event.ConsumedAt.IsZero() {
			return reject("event_conflict", "event does not match this wait or was consumed")
		}
		event.ConsumedAt = at
		s.put(jobEventsBucket, jobKey(run.TenantID, run.ID, event.ID), event)
		s.del(jobEventKeysBucket, jobKey(run.TenantID, run.ID, event.Key, event.ID))
	} else if (run.WaitKey != "" || !run.WakeAt.IsZero()) && !run.CancelRequested && !(c.Kind == JobComplete && c.Status != JobSucceeded) {
		if run.WakeAt.IsZero() || at.Before(run.WakeAt) {
			return reject("not_ready", "wait needs a matching event or due timer")
		}
	}
	run.State, run.PendingAction, run.WaitKey, run.WakeAt = bytes.Clone(c.State), nil, "", time.Time{}
	run.Step++
	run.LastTransition = at
	switch c.Kind {
	case JobDoAction:
		if !validIdentity(c.Operation) || run.Binding == nil || run.ActionCount == math.MaxUint64 {
			return reject("invalid", "action requires a semantic operation and a driver binding")
		}
		run.ActionCount++
		action := &JobAction{TenantID: run.TenantID, RunID: run.ID, Sequence: run.ActionCount, AttemptID: run.AttemptID, Operation: c.Operation, Input: bytes.Clone(c.Input), Status: EffectAuthorized, AuthorizedAt: at}
		run.PendingAction, run.Status = action, JobRunning
		s.put(jobActionsBucket, jobActionKey(run.TenantID, run.ID, action.Sequence), action)
		r.Action = action
	case JobCheckpoint:
		run.Status = JobRunning
	case JobWait:
		if (c.WaitKey == "" && c.WakeAt.IsZero()) || (c.WaitKey != "" && !validIdentity(c.WaitKey)) || (!c.WakeAt.IsZero() && !c.WakeAt.After(at)) {
			return reject("invalid", "wait requires a correlation key or future timer")
		}
		run.Status, run.WaitKey, run.WakeAt = JobWaiting, c.WaitKey, c.WakeAt
		if len(run.Resources) == 0 {
			if err := endJobAttempt(tx, s, run, at, JobWaiting); err != nil {
				return err
			}
		}
		event, err := nextJobEvent(tx, run)
		if err != nil {
			return err
		}
		if event != nil {
			if run.OwnerID == "" {
				run.Status = JobReady
				run.ReadySince = at
			} else {
				run.Status = JobRunning
			}
		}
	case JobComplete:
		if c.Status != JobSucceeded && c.Status != JobFailed && c.Status != JobCancelled && c.Status != JobUnknown {
			return reject("invalid", "completion requires a terminal outcome")
		}
		if run.CancelRequested && c.Status == JobSucceeded {
			return reject("cancel_requested", "a cancelled workflow must retain its cancellation intent")
		}
		run.Status, run.Result, run.Error = c.Status, bytes.Clone(c.Result), c.Error
		if !c.Quiescent {
			run.Status = JobUnknown
		}
		if err := releaseJobResources(tx, s, &before, at, c.Quiescent, false); err != nil {
			return err
		}
		if err := endJobAttempt(tx, s, run, at, run.Status); err != nil {
			return err
		}
	default:
		return reject("invalid", "unknown job decision")
	}
	saveJobRun(s, &before, run)
	r.Run = run
	return nil
}

func completeJobAction(tx *bbolt.Tx, s *jobChanges, at time.Time, c CompleteJobAction, r *JobResult) error {
	run, err := ownedJob(tx, c.JobOwner, at)
	if err != nil {
		return err
	}
	before := *run
	if run.PendingAction == nil || run.PendingAction.Sequence != c.Sequence || run.PendingAction.Status != EffectAuthorized || run.PendingAction.AttemptID != c.AttemptID {
		return reject("stale_action", "action authorization changed or was already completed")
	}
	if c.Status != EffectSucceeded && c.Status != EffectFailed && c.Status != EffectUnknown {
		return reject("invalid", "action requires a known result or explicit uncertainty")
	}
	if len(c.Result) > 1<<20 || len(c.Error) > 4096 {
		return reject("invalid", "action result limit exceeded")
	}
	action := *run.PendingAction
	action.Status, action.Result, action.Error, action.FinishedAt = c.Status, bytes.Clone(c.Result), c.Error, at
	run.PendingAction, run.LastTransition = &action, at
	s.put(jobActionsBucket, jobActionKey(run.TenantID, run.ID, c.Sequence), &action)
	saveJobRun(s, &before, run)
	r.Run, r.Action = run, &action
	return nil
}

func endJobAttempt(tx *bbolt.Tx, s *jobChanges, run *JobRun, at time.Time, status JobStatus) error {
	attempt, err := readRecord[JobAttempt](tx, jobAttemptsBucket, jobKey(run.TenantID, run.ID, run.AttemptID))
	if err != nil {
		return err
	}
	attempt.Status, attempt.FinishedAt = status, at
	s.put(jobAttemptsBucket, jobKey(run.TenantID, run.ID, attempt.ID), attempt)
	if err := changeJobQuotas(tx, s, run, false); err != nil {
		return err
	}
	run.OwnerID, run.AttemptID, run.LeaseUntil = "", "", time.Time{}
	return nil
}

func releaseJobResources(tx *bbolt.Tx, s *jobChanges, run *JobRun, at time.Time, quiescent, expired bool) error {
	for _, id := range run.Resources {
		resource, err := ownedJobResource(tx, run, id, at, expired)
		if err != nil {
			return err
		}
		if resource.Spec.ChannelID != "" {
			channel, err := readRecord[CommunicationChannel](tx, channelsBucket, resource.Spec.ChannelID)
			if err != nil {
				return err
			}
			matches := channel.OwnerID == run.OwnerID && channel.Epoch == resource.ChannelEpoch && channel.ActiveAttemptID == "" && (channel.State == ChannelOwned || (expired && channel.State == ChannelBlocked))
			if !matches && !expired {
				return reject("stale_owner", "gateway authority changed")
			}
			// Legacy operator recovery may have already excluded this owner and
			// reassigned the gateway. Expire the abandoned job without touching
			// that newer authority. Its generic resource still needs resolution.
			if matches {
				if err := checkTime(channel, at); err != nil {
					return err
				}
				if quiescent {
					channel.State, channel.OwnerID, channel.LeaseUntil = ChannelAvailable, "", time.Time{}
				} else {
					channel.State = ChannelBlocked
				}
				channel.LastTransition = at
				s.put(channelsBucket, channel.ID, channel)
			}
		}
		if quiescent {
			resource.State, resource.TenantID, resource.RunID, resource.AttemptID, resource.OwnerID, resource.LeaseUntil = ResourceAvailable, "", "", "", "", time.Time{}
		} else {
			resource.State = ResourceBlocked
		}
		resource.LastTransition = at
		s.put(jobResourcesBucket, id, resource)
	}
	return nil
}

func signalJob(tx *bbolt.Tx, s *jobChanges, at time.Time, c SignalJob, r *JobResult) error {
	if !validIdentity(c.EventID) || !validIdentity(c.Key) || len(c.Data) > 1<<20 {
		return reject("invalid", "event identity, correlation key and data of at most 1 MiB are required")
	}
	run, err := readRecord[JobRun](tx, jobRunsBucket, jobKey(c.TenantID, c.RunID))
	if err != nil {
		return err
	}
	key := jobKey(c.TenantID, c.RunID, c.EventID)
	if tx.Bucket(jobEventsBucket).Get([]byte(key)) != nil {
		event, err := readRecord[JobEvent](tx, jobEventsBucket, key)
		if err != nil {
			return err
		}
		if event.Key != c.Key || !bytes.Equal(event.Data, c.Data) {
			return reject("event_conflict", "event ID was already used with different data")
		}
		r.Event, r.Run = event, run
		return nil
	}
	if run.Status.Terminal() {
		return reject("not_ready", "terminal jobs cannot accept new events")
	}
	event := &JobEvent{TenantID: c.TenantID, RunID: c.RunID, ID: c.EventID, Key: c.Key, Data: bytes.Clone(c.Data), At: at}
	s.put(jobEventsBucket, key, event)
	s.index(jobEventKeysBucket, jobKey(c.TenantID, c.RunID, c.Key, c.EventID), []byte(key))
	before := *run
	if run.Status == JobWaiting && run.WaitKey == c.Key {
		if run.OwnerID == "" {
			run.Status = JobReady
			run.ReadySince = at
		} else {
			run.Status = JobRunning
		}
	}
	// Inbox delivery need not invalidate an in-flight checkpoint's timestamp.
	// Consumption is serialized by Step and the event's consumed marker.
	saveJobRun(s, &before, run)
	r.Run, r.Event = run, event
	return nil
}

func cancelJob(tx *bbolt.Tx, s *jobChanges, at time.Time, c CancelJob, r *JobResult) error {
	if len(c.Reason) > 4096 {
		return reject("invalid", "cancellation reason is too long")
	}
	run, err := readRecord[JobRun](tx, jobRunsBucket, jobKey(c.TenantID, c.RunID))
	if err != nil {
		return err
	}
	before := *run
	if !run.Status.Terminal() && !run.CancelRequested {
		run.CancelRequested, run.CancelRequestedAt, run.CancelReason = true, at, c.Reason
		if run.OwnerID == "" {
			run.Status = JobCancelled
			run.LastTransition = later(run.LastTransition, at)
		}
		// Intent has its own timestamp. It must not make an in-flight known
		// action result or renewal stale solely by winning consensus first.
		saveJobRun(s, &before, run)
	}
	r.Run = run
	return nil
}

func wakeJob(tx *bbolt.Tx, s *jobChanges, at time.Time, c WakeJob, r *JobResult) error {
	run, err := readRecord[JobRun](tx, jobRunsBucket, jobKey(c.TenantID, c.RunID))
	if err != nil {
		return err
	}
	if run.Status != JobWaiting || run.OwnerID != "" || run.WakeAt.IsZero() || !run.WakeAt.Equal(c.WakeAt) || at.Before(run.WakeAt) {
		return reject("not_due", "observed timer is not due or changed")
	}
	before := *run
	run.Status, run.LastTransition = JobReady, later(run.LastTransition, at)
	run.ReadySince = at
	saveJobRun(s, &before, run)
	r.Run = run
	return nil
}

func expireJob(tx *bbolt.Tx, s *jobChanges, at time.Time, c ExpireJob, r *JobResult) error {
	run, err := readRecord[JobRun](tx, jobRunsBucket, jobKey(c.TenantID, c.RunID))
	if err != nil {
		return err
	}
	if !validIdentity(c.OwnerID) || run.OwnerID != c.OwnerID || run.AttemptID != c.AttemptID || !run.LeaseUntil.Equal(c.LeaseUntil) {
		return reject("stale_owner", "observed job claim changed")
	}
	if at.Before(run.LeaseUntil) || at.Before(run.LastTransition) {
		return reject("not_due", "job ownership has not expired")
	}
	before := *run
	unsafe := len(run.Resources) != 0 || (run.PendingAction != nil && (run.PendingAction.Status == EffectAuthorized || run.PendingAction.Status == EffectUnknown))
	if err := releaseJobResources(tx, s, run, at, false, true); err != nil {
		return err
	}
	if run.PendingAction != nil && run.PendingAction.Status == EffectAuthorized {
		action := *run.PendingAction
		action.Status, action.FinishedAt, action.Error = EffectUnknown, at, "owner expired before a durable result"
		run.PendingAction = &action
		s.put(jobActionsBucket, jobActionKey(run.TenantID, run.ID, action.Sequence), &action)
	}
	run.Status, run.LastTransition = JobReady, at
	run.ReadySince = at
	if unsafe {
		run.Status, run.Error = JobUnknown, "ownership expired; remote effects or session state may be uncertain"
	} else if run.CancelRequested {
		run.Status = JobCancelled
	}
	if err := endJobAttempt(tx, s, run, at, JobUnknown); err != nil {
		return err
	}
	saveJobRun(s, &before, run)
	r.Run = run
	return nil
}

func resolveJobResource(tx *bbolt.Tx, s *jobChanges, command StateCommand, c ResolveJobResource, r *JobResult) error {
	if err := validResolution(c.Actor, c.Reason); err != nil {
		return err
	}
	if !c.Quiescent {
		return reject("not_quiescent", "resource recovery requires evidence that prior access stopped")
	}
	resource, err := readRecord[JobResource](tx, jobResourcesBucket, c.ResourceID)
	if err != nil {
		return err
	}
	if resource.State != ResourceBlocked || resource.Epoch != c.Epoch {
		return reject("stale_owner", "blocked resource epoch changed")
	}
	if command.At.Before(resource.LastTransition) {
		return reject("stale_time", "resolution predates the blocked resource")
	}
	if resource.Spec.ChannelID != "" {
		channel, err := readRecord[CommunicationChannel](tx, channelsBucket, resource.Spec.ChannelID)
		if err != nil {
			return err
		}
		if channel.Epoch < resource.ChannelEpoch {
			return reject("stale_owner", "gateway authority predates the resource allocation")
		}
		if channel.Epoch == resource.ChannelEpoch {
			if channel.ActiveAttemptID != "" || (channel.State != ChannelBlocked && channel.State != ChannelAvailable) || (channel.State == ChannelBlocked && channel.OwnerID != resource.OwnerID) {
				return reject("stale_owner", "resolve the gateway's current authority first")
			}
			if err := checkTime(channel, command.At); err != nil {
				return err
			}
			channel.State, channel.OwnerID, channel.LeaseUntil, channel.LastTransition = ChannelAvailable, "", time.Time{}, command.At
			s.put(channelsBucket, channel.ID, channel)
		}
		// A higher epoch proves that legacy recovery already authorized a new
		// owner. Resolve only our abandoned allocation, preserving that owner.
	}
	audit := &JobResolution{RequestID: command.RequestID, TenantID: resource.TenantID, RunID: resource.RunID, ResourceID: c.ResourceID, Epoch: c.Epoch, Actor: c.Actor, Reason: c.Reason, Quiescent: true, At: command.At}
	s.put(jobResolutionsBucket, command.RequestID, audit)
	resource.State, resource.TenantID, resource.RunID, resource.AttemptID, resource.OwnerID, resource.LeaseUntil, resource.LastTransition = ResourceAvailable, "", "", "", "", time.Time{}, command.At
	s.put(jobResourcesBucket, resource.Spec.ID, resource)
	r.Resource = resource
	return nil
}

func resolveJob(tx *bbolt.Tx, s *jobChanges, command StateCommand, c ResolveJob, r *JobResult) error {
	if err := validResolution(c.Actor, c.Reason); err != nil {
		return err
	}
	if c.Status != JobSucceeded && c.Status != JobFailed && c.Status != JobCancelled {
		return reject("invalid", "resolution requires a terminal outcome; uncertain actions cannot be automatically replayed")
	}
	if len(c.Result) > 1<<20 {
		return reject("invalid", "resolution result is too long")
	}
	run, err := readRecord[JobRun](tx, jobRunsBucket, jobKey(c.TenantID, c.RunID))
	if err != nil {
		return err
	}
	if run.Status != JobUnknown || run.Step != c.ExpectedStep || run.Step == math.MaxUint64 || command.At.Before(run.LastTransition) {
		return reject("step_conflict", "uncertain job checkpoint changed")
	}
	before := *run
	run.Status, run.Result, run.LastTransition = c.Status, bytes.Clone(c.Result), command.At
	run.Step++
	saveJobRun(s, &before, run)
	s.put(jobResolutionsBucket, command.RequestID, &JobResolution{RequestID: command.RequestID, TenantID: c.TenantID, RunID: c.RunID, Actor: c.Actor, Reason: c.Reason, Status: c.Status, At: command.At})
	r.Run = run
	return nil
}
