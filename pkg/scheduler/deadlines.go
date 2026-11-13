package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/duravela/djob/pkg/db"
)

type DeadlineBackend interface {
	ProposeStateCommand(context.Context, db.StateCommand) (db.StateResult, error)
	ListExpiredReadyRuns(time.Time, []byte, int) (db.RunPage, error)
}

type DeadlineSweeperConfig struct {
	PollInterval  time.Duration
	BatchSize     int
	CommitTimeout time.Duration
	OnError       func(error)
}

// DeadlineSweeper expires queued runs without acquiring or touching a gateway.
// It operates on controllers too. Running attempts belong to their executor.
type DeadlineSweeper struct {
	cfg      DeadlineSweeperConfig
	backend  DeadlineBackend
	isLeader func() bool
	mu       sync.Mutex
	cursor   []byte
	prefix   string
	sequence uint64
}

func NewDeadlineSweeper(cfg DeadlineSweeperConfig, backend DeadlineBackend, isLeader func() bool) (*DeadlineSweeper, error) {
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
		return nil, fmt.Errorf("invalid deadline sweeper configuration")
	}
	prefix, err := newPrefix()
	if err != nil {
		return nil, err
	}
	return &DeadlineSweeper{cfg: cfg, backend: backend, isLeader: isLeader, prefix: "deadline/" + prefix}, nil
}

func (s *DeadlineSweeper) Tick(ctx context.Context, at time.Time) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if at.IsZero() {
		return 0, fmt.Errorf("deadline sweep requires an explicit timestamp")
	}
	if !s.isLeader() {
		s.cursor = nil
		return 0, nil
	}
	page, err := s.backend.ListExpiredReadyRuns(at, s.cursor, s.cfg.BatchSize)
	if err != nil {
		return 0, err
	}
	s.cursor = page.NextCursor
	expired := 0
	var failures []error
	for _, run := range page.Runs {
		if err := ctx.Err(); err != nil {
			return expired, err
		}
		if !s.isLeader() {
			s.cursor = nil
			break
		}
		if s.sequence == ^uint64(0) {
			return expired, fmt.Errorf("deadline request sequence exhausted")
		}
		s.sequence++
		command := db.StateCommand{RequestID: fmt.Sprintf("%s/%d", s.prefix, s.sequence), At: at.UTC(),
			ExpireRun: &db.ExpireRun{RunID: run.ID, StartDeadline: run.StartDeadline, RetryAware: true}}
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
			if errors.As(err, &rejection) && (rejection.Code == "deadline_conflict" || rejection.Code == "not_due" || rejection.Code == "not_found") {
				continue
			}
			failures = append(failures, err)
			continue
		}
		fingerprint, err := command.Fingerprint()
		if err != nil || result.RequestID != command.RequestID || result.Fingerprint != fingerprint || result.Run == nil || result.Run.ID != run.ID {
			failures = append(failures, fmt.Errorf("run expiry receipt does not match request"))
			continue
		}
		if result.Run.State == db.RunExpired {
			expired++
		}
	}
	return expired, errors.Join(failures...)
}

func (s *DeadlineSweeper) Run(ctx context.Context) error {
	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()
	for {
		_, err := s.Tick(ctx, time.Now().UTC())
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil && s.cfg.OnError != nil {
			s.cfg.OnError(err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
