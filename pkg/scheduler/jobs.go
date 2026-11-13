package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/duravela/djob/pkg/db"
)

type JobMaintenanceBackend interface {
	ProposeStateCommand(context.Context, db.StateCommand) (db.StateResult, error)
	ListExpiredJobClaims(time.Time, []byte, int) (db.JobRunPage, error)
	ListDueJobTimers(time.Time, []byte, int) (db.JobRunPage, error)
}

type JobMaintenanceConfig struct {
	PollInterval  time.Duration
	BatchSize     int
	CommitTimeout time.Duration
	OnError       func(error)
	OnWoken       func(DueWorkObservation) // Resource-free timer wakes only; synchronous and aggregate-only.
}

// JobMaintenance discovers expired generic claims and resource-free wait timers.
// It runs on controller-only nodes too. It never retries uncertain effects or
// frees physical resources after expiry; the FSM determines safe checkpoint
// resumptions and blocks abandoned reservations.
type JobMaintenance struct {
	cfg            JobMaintenanceConfig
	backend        JobMaintenanceBackend
	isLeader       func() bool
	mu             sync.Mutex
	leases, timers []byte
	prefix         string
	sequence       uint64
}

func NewJobMaintenance(cfg JobMaintenanceConfig, backend JobMaintenanceBackend, isLeader func() bool) (*JobMaintenance, error) {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 100 * time.Millisecond
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 64
	}
	if cfg.CommitTimeout == 0 {
		cfg.CommitTimeout = 2 * time.Second
	}
	if backend == nil || isLeader == nil || cfg.PollInterval <= 0 || cfg.BatchSize < 1 || cfg.BatchSize > db.MaxQueryPage || cfg.CommitTimeout <= 0 {
		return nil, fmt.Errorf("invalid job maintenance configuration")
	}
	prefix, err := newPrefix()
	if err != nil {
		return nil, err
	}
	return &JobMaintenance{cfg: cfg, backend: backend, isLeader: isLeader, prefix: "job-maintenance/" + prefix}, nil
}

// Tick examines at most one page from each timer index. Observations never grant
// authority; each proposal checks the exact observed attempt, owner, and deadline.
func (s *JobMaintenance) Tick(ctx context.Context, at time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if at.IsZero() {
		return 0, fmt.Errorf("job maintenance requires an explicit timestamp")
	}
	if !s.isLeader() {
		s.leases, s.timers = nil, nil
		return 0, nil
	}
	expired, err := s.backend.ListExpiredJobClaims(at, s.leases, s.cfg.BatchSize)
	if err != nil {
		return 0, err
	}
	timers, err := s.backend.ListDueJobTimers(at, s.timers, s.cfg.BatchSize)
	if err != nil {
		return 0, err
	}
	s.leases, s.timers = expired.NextCursor, timers.NextCursor
	changed := 0
	var failures []error
	for i, rows := range [][]db.JobRun{expired.Runs, timers.Runs} {
		for _, run := range rows {
			if err := ctx.Err(); err != nil {
				return changed, err
			}
			if !s.isLeader() {
				s.leases, s.timers = nil, nil
				return changed, errors.Join(failures...)
			}
			if s.sequence == ^uint64(0) {
				return changed, fmt.Errorf("job maintenance request sequence exhausted")
			}
			s.sequence++
			job := &db.JobCommand{}
			if i == 0 {
				job.Expire = &db.ExpireJob{JobOwner: db.JobOwner{TenantID: run.TenantID, RunID: run.ID, AttemptID: run.AttemptID, OwnerID: run.OwnerID}, LeaseUntil: run.LeaseUntil}
			} else {
				job.Wake = &db.WakeJob{TenantID: run.TenantID, RunID: run.ID, WakeAt: run.WakeAt}
			}
			command := db.StateCommand{RequestID: fmt.Sprintf("%s/%d", s.prefix, s.sequence), At: at.UTC(), Job: job}
			var result db.StateResult
			for range 2 {
				commit, cancel := context.WithTimeout(ctx, s.cfg.CommitTimeout)
				result, err = s.backend.ProposeStateCommand(commit, command)
				cancel()
				var rejection *db.CommandRejection
				if err == nil || errors.As(err, &rejection) || ctx.Err() != nil {
					break
				}
			}
			if err == nil {
				err = result.Err()
			}
			if err != nil {
				var rejection *db.CommandRejection
				if errors.As(err, &rejection) {
					switch rejection.Code {
					case "stale_owner", "stale_time", "not_due", "not_found":
						continue
					}
				}
				failures = append(failures, err)
				continue
			}
			fingerprint, hashErr := command.Fingerprint()
			if hashErr != nil || result.RequestID != command.RequestID || result.Fingerprint != fingerprint || result.Job == nil || result.Job.Run == nil || result.Job.Run.TenantID != run.TenantID || result.Job.Run.ID != run.ID {
				failures = append(failures, fmt.Errorf("job maintenance receipt does not match request"))
				continue
			}
			changed++
			if job.Wake != nil {
				s.observeWoken(*job.Wake, result)
			}
		}
	}
	return changed, errors.Join(failures...)
}

func (s *JobMaintenance) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		_, err := s.Tick(ctx, time.Now().UTC())
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil && s.cfg.OnError != nil {
			s.cfg.OnError(err)
		}
	}
}
