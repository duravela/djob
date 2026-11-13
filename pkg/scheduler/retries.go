package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/duravela/djob/pkg/db"
)

type RetryBackend interface {
	ProposeStateCommand(context.Context, db.StateCommand) (db.StateResult, error)
	ListDueRetries(time.Time, []byte, int) (db.RunPage, error)
}

type RetryPromoterConfig struct {
	PollInterval  time.Duration
	BatchSize     int
	CommitTimeout time.Duration
	OnError       func(error)
	OnReleased    func(DueWorkObservation) // Synchronous, aggregate-only; must return promptly.
}

// RetryPromoter moves due retries back into the durable ready queue. It never
// claims a run, sleeps while holding a gateway, or performs physical I/O.
type RetryPromoter struct {
	cfg      RetryPromoterConfig
	backend  RetryBackend
	isLeader func() bool
	mu       sync.Mutex
	cursor   []byte
	prefix   string
	sequence uint64
}

func NewRetryPromoter(cfg RetryPromoterConfig, backend RetryBackend, isLeader func() bool) (*RetryPromoter, error) {
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
		return nil, fmt.Errorf("invalid retry promoter configuration")
	}
	prefix, err := newPrefix()
	if err != nil {
		return nil, err
	}
	return &RetryPromoter{cfg: cfg, backend: backend, isLeader: isLeader, prefix: "retry/" + prefix}, nil
}

func (p *RetryPromoter) Tick(ctx context.Context, at time.Time) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if at.IsZero() {
		return 0, fmt.Errorf("retry promotion requires an explicit timestamp")
	}
	if !p.isLeader() {
		p.cursor = nil
		return 0, nil
	}
	page, err := p.backend.ListDueRetries(at, p.cursor, p.cfg.BatchSize)
	if err != nil {
		return 0, err
	}
	p.cursor = page.NextCursor
	ready := 0
	var failures []error
	for _, run := range page.Runs {
		if err := ctx.Err(); err != nil {
			return ready, err
		}
		if !p.isLeader() {
			p.cursor = nil
			break
		}
		if p.sequence == ^uint64(0) {
			return ready, fmt.Errorf("retry request sequence exhausted")
		}
		p.sequence++
		command := db.StateCommand{RequestID: fmt.Sprintf("%s/%d", p.prefix, p.sequence), At: at.UTC(),
			ReleaseRetry: &db.ReleaseRetry{RunID: run.ID, AttemptID: run.LastAttemptID, RetryAt: run.RetryAt}}
		var result db.StateResult
		for range 2 {
			commit, cancel := context.WithTimeout(ctx, p.cfg.CommitTimeout)
			result, err = p.backend.ProposeStateCommand(commit, command)
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
			if errors.As(err, &rejection) && (rejection.Code == "retry_conflict" || rejection.Code == "not_due" || rejection.Code == "not_found") {
				continue
			}
			failures = append(failures, err)
			continue
		}
		fingerprint, err := command.Fingerprint()
		if err != nil || result.RequestID != command.RequestID || result.Fingerprint != fingerprint || result.Run == nil || result.Run.ID != run.ID {
			failures = append(failures, fmt.Errorf("retry release receipt does not match request"))
			continue
		}
		if result.Run.State == db.RunReady {
			ready++
			p.observeReleased(*command.ReleaseRetry, result)
		}
	}
	return ready, errors.Join(failures...)
}

func (p *RetryPromoter) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.cfg.PollInterval)
	defer ticker.Stop()
	for {
		_, err := p.Tick(ctx, time.Now().UTC())
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil && p.cfg.OnError != nil {
			p.cfg.OnError(err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
