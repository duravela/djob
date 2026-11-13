package scheduler

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/duravela/djob/pkg/db"
)

func PrepareJobTrigger(d db.JobTriggerDefinition, expected uint64, at time.Time) (db.PutJobTrigger, error) {
	if d.MaxPending == 0 {
		d.MaxPending = db.DefaultTriggerMaxPending
	}
	if d.Event != nil {
		return db.PutJobTrigger{Definition: d, ExpectedRevision: expected}, db.ValidateJobTrigger(d)
	}
	if d.Timing.MissedPolicy == "" {
		d.Timing.MissedPolicy = db.MissedCoalesce
	}
	if d.Timing.MissedPolicy == db.MissedSkip && d.Timing.MisfireGrace == 0 {
		d.Timing.MisfireGrace = time.Second
	}
	if d.Timing.MissedPolicy == db.MissedReplay && d.Timing.CatchUpLimit == 0 {
		d.Timing.CatchUpLimit = min(32, d.MaxPending)
	}
	d.Timing.Spec.StartAt = d.Timing.Spec.StartAt.UTC()
	if err := db.ValidateJobTrigger(d); err != nil {
		return db.PutJobTrigger{}, err
	}
	timing, next, err := prepareTiming(d.Timing, expected, at)
	d.Timing = timing
	return db.PutJobTrigger{Definition: d, ExpectedRevision: expected, NextAt: next}, err
}

func PlanJobTrigger(t db.JobTrigger, at time.Time) (db.MaterializeJobs, error) {
	if t.Event != nil {
		return db.MaterializeJobs{}, fmt.Errorf("event triggers have no calendar plan")
	}
	if err := db.ValidateJobTrigger(t.JobTriggerDefinition); err != nil {
		return db.MaterializeJobs{}, err
	}
	plan, err := planTiming(t.TimingCursor(), at)
	return db.MaterializeJobs{TenantID: t.TenantID, Plan: plan}, err
}

type JobTriggerBackend interface {
	ProposeStateCommand(context.Context, db.StateCommand) (db.StateResult, error)
	ListDueJobTriggers(time.Time, []byte, int) (db.JobTriggerPage, error)
	JobTriggerCapacity(tenant, trigger string) (int, error)
}

type JobTriggerMaterializer struct {
	cfg      MaterializerConfig
	backend  JobTriggerBackend
	isLeader func() bool
	mu       sync.Mutex
	cursor   []byte
	prefix   string
	sequence atomic.Uint64
}

func NewJobTriggerMaterializer(cfg MaterializerConfig, backend JobTriggerBackend, isLeader func() bool) (*JobTriggerMaterializer, error) {
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
		return nil, fmt.Errorf("invalid generic trigger materializer configuration")
	}
	prefix, err := newPrefix()
	if err != nil {
		return nil, err
	}
	return &JobTriggerMaterializer{cfg: cfg, backend: backend, isLeader: isLeader, prefix: "job-triggers/" + prefix}, nil
}

func (m *JobTriggerMaterializer) submit(ctx context.Context, command db.StateCommand) (db.StateResult, error) {
	command.RequestID = fmt.Sprintf("%s/%d", m.prefix, m.sequence.Add(1))
	var result db.StateResult
	var err error
	for i := 0; i < 2; i++ {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		commitCtx, cancel := context.WithTimeout(ctx, m.cfg.CommitTimeout)
		result, err = m.backend.ProposeStateCommand(commitCtx, command)
		cancel()
		var rejection *db.CommandRejection
		if err == nil || errors.As(err, &rejection) {
			break
		}
	}
	if err != nil {
		return result, err
	}
	if err := result.Err(); err != nil {
		return result, err
	}
	fingerprint, err := command.Fingerprint()
	if err != nil {
		return result, err
	}
	if result.RequestID != command.RequestID || result.Fingerprint != fingerprint || result.Job == nil || result.Job.Trigger == nil {
		return result, fmt.Errorf("generic trigger receipt does not match request")
	}
	return result, nil
}

// Local leadership avoids unnecessary proposals. Durable revision/cursor checks
// and the occurrence ledger handle concurrent or replaced leaders.
func (m *JobTriggerMaterializer) Tick(ctx context.Context, at time.Time) (MaterializationStats, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	stats := MaterializationStats{}
	if !m.isLeader() {
		m.cursor = nil
		return stats, nil
	}
	page, err := m.backend.ListDueJobTriggers(at, m.cursor, m.cfg.BatchSize)
	if err != nil {
		return stats, err
	}
	m.cursor = page.NextCursor
	var failures []error
	for _, trigger := range page.Triggers {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		if !m.isLeader() {
			m.cursor = nil
			break
		}
		plan, err := PlanJobTrigger(trigger, at)
		if err != nil {
			failures = append(failures, err)
			continue
		}
		if len(plan.Plan.Occurrences) > 0 {
			capacity, e := m.backend.JobTriggerCapacity(trigger.TenantID, trigger.ID)
			var rejection *db.CommandRejection
			if errors.As(e, &rejection) && rejection.Code == "not_found" {
				stats.Conflicts++ // Retired after the due page was read.
				continue
			}
			if errors.As(e, &rejection) && rejection.Code == "resource_denied" {
				stats.Deferred++
				continue
			}
			if e != nil {
				failures = append(failures, e)
				continue
			}
			if capacity < len(plan.Plan.Occurrences) {
				stats.Deferred++
				continue
			}
		}
		result, err := m.submit(ctx, db.StateCommand{At: at.UTC(), Job: &db.JobCommand{Materialize: &plan}})
		if err != nil {
			var rejection *db.CommandRejection
			if errors.As(err, &rejection) {
				switch rejection.Code {
				case "backpressure", "resource_denied", "quota_exceeded":
					stats.Deferred++
					continue
				case "schedule_conflict", "schedule_inactive", "not_due", "not_found":
					stats.Conflicts++
					continue
				}
			}
			failures = append(failures, err)
			continue
		}
		stats.Advanced++
		stats.Created += result.Job.Created
		m.observeMaterialized(plan, result)
	}
	return stats, errors.Join(failures...)
}

func (m *JobTriggerMaterializer) Run(ctx context.Context) error {
	ticker := time.NewTicker(m.cfg.PollInterval)
	defer ticker.Stop()
	for {
		_, err := m.Tick(ctx, time.Now().UTC())
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil && m.cfg.OnError != nil {
			m.cfg.OnError(err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
