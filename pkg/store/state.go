package store

import (
	"context"
	"fmt"
	"time"

	"github.com/duravela/djob/pkg/db"
)

type stateRequestKey struct {
	requestID   string
	fingerprint [32]byte
}

// ProposeStateCommand waits for durable application of this exact request.
// Retrying requires the same ID and payload, including timestamps and tokens.
// A CommandRejection is an expected state conflict; it does not stop Raft.
func (s *Store) ProposeStateCommand(ctx context.Context, command db.StateCommand) (db.StateResult, error) {
	if err := command.Validate(); err != nil {
		return db.StateResult{}, err
	}
	if s.recoveryHeld && command.NeedsAdmission() {
		return db.StateResult{}, fmt.Errorf("%w: recovery_hold", db.ErrAdmissionHeld)
	}
	s.mu.RLock()
	policy := s.admissionPolicy
	s.mu.RUnlock()
	if policy != nil {
		if err := policy(command); err != nil {
			return db.StateResult{}, err
		}
	}
	fingerprint, err := command.Fingerprint()
	if err != nil {
		return db.StateResult{}, fmt.Errorf("fingerprint state command: %w", err)
	}
	key := stateRequestKey{requestID: command.RequestID, fingerprint: fingerprint}
	data, err := encodeStateProposal(db.Command{Version: command.CommandVersion(), Type: db.CmdState, State: &command})
	if err != nil {
		return db.StateResult{}, fmt.Errorf("encode state command: %w", err)
	}
	release, err := s.reserveProposal(len(data))
	if err != nil {
		return db.StateResult{}, err
	}
	defer release()
	waiter := make(chan db.StateResult, 1)
	s.mu.Lock()
	if s.pendingState[key] == nil {
		s.pendingState[key] = make(map[chan db.StateResult]struct{})
	}
	s.pendingState[key][waiter] = struct{}{}
	s.mu.Unlock()
	s.metrics.PendingRequests.Inc()
	defer func() {
		s.mu.Lock()
		delete(s.pendingState[key], waiter)
		if len(s.pendingState[key]) == 0 {
			delete(s.pendingState, key)
		}
		s.mu.Unlock()
		s.metrics.PendingRequests.Dec()
	}()
	timeout := s.cfg.ProposalTimeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	select {
	case <-s.stopC:
		return db.StateResult{}, s.stopError()
	default:
	}
	start := time.Now()
	select {
	case s.proposeC <- data:
		s.metrics.Proposals.Inc()
	case <-ctx.Done():
		return db.StateResult{}, ctx.Err()
	case <-s.stopC:
		return db.StateResult{}, s.stopError()
	}
	select {
	case result := <-waiter:
		s.metrics.ProposalLatency.Observe(time.Since(start).Seconds())
		return result, result.Err()
	case <-ctx.Done():
		return db.StateResult{}, ctx.Err()
	case <-s.stopC:
		return db.StateResult{}, s.stopError()
	}
}

// SetAdmissionPolicy installs a local pre-proposal guard. It must not inspect
// store state while holding its own locks or perform a replicated transition.
func (s *Store) SetAdmissionPolicy(policy func(db.StateCommand) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.admissionPolicy = policy
}

func (s *Store) notifyStateProposals(results []db.StateResult) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, result := range results {
		key := stateRequestKey{requestID: result.RequestID, fingerprint: result.Fingerprint}
		for waiter := range s.pendingState[key] {
			select {
			case waiter <- result.Clone():
			default:
			}
		}
	}
}

// These reads inspect the local replica. Execution authority comes from a
// committed claim and valid ownership, not an unchecked read from a follower.
func (s *Store) GetChannel(id string) (*db.CommunicationChannel, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.GetChannel(id)
}

func (s *Store) GetRun(id string) (*db.Run, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.GetRun(id)
}

func (s *Store) GetAttempt(id string) (*db.Attempt, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.GetAttempt(id)
}

func (s *Store) GetResolution(id string) (*db.Resolution, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.GetResolution(id)
}

func (s *Store) GetSchedule(id string) (*db.Schedule, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.GetSchedule(id)
}

func (s *Store) ReadyRunCount(channel string) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.ReadyRunCount(channel)
}

func (s *Store) PendingRunCount(channel string) (uint64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.PendingRunCount(channel)
}

func (s *Store) ListDueRetries(at time.Time, after []byte, limit int) (db.RunPage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.ListDueRetries(at, after, limit)
}

func (s *Store) FirstReadyRun(channel string) (*db.Run, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.FirstReadyRun(channel)
}

func (s *Store) ListReadyRuns(after []byte, limit int) (db.RunPage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.ListReadyRuns(after, limit)
}

func (s *Store) ListExpiredReadyRuns(at time.Time, after []byte, limit int) (db.RunPage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.ListExpiredReadyRuns(at, after, limit)
}

func (s *Store) ListDueSchedules(at time.Time, after []byte, limit int) (db.SchedulePage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.ListDueSchedules(at, after, limit)
}

func (s *Store) ListChannels(after []byte, limit int) (db.ChannelPage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.ListChannels(after, limit)
}

func (s *Store) ListSchedules(after []byte, limit int) (db.SchedulePage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.ListSchedules(after, limit)
}

func (s *Store) ListRuns(after []byte, limit int) (db.RunPage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.ListRuns(after, limit)
}
