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

type JobRetentionBackend interface {
	ProposeStateCommand(context.Context, db.StateCommand) (db.StateResult, error)
	ListPendingJobPurges([]byte, int) (db.JobDeletionPage, error)
	ListJobRetentionCandidates(time.Time, []byte, int) (db.JobPurgePage, error)
	ListPendingJobReceiptCompactions([]byte, int) (db.JobReceiptCompactionPage, error)
	ListPendingJobConfigurationReceiptCompactions([]byte, int) (db.JobConfigurationReceiptCompactionPage, error)
}
type JobRetentionConfig struct {
	PollInterval    time.Duration
	BatchSize       int
	DeleteBatchSize int
	CommitTimeout   time.Duration
	OnError         func(error)
}
type JobRetention struct {
	cfg                            JobRetentionConfig
	backend                        JobRetentionBackend
	isLeader                       func() bool
	mu                             sync.Mutex
	pendingCursor, candidateCursor []byte
	receiptCursor                  []byte
	configurationReceiptCursor     []byte
	prefix                         string
	sequence                       atomic.Uint64
}
type JobRetentionStats struct{ Batches, Completed, ReceiptBatches, ReceiptsCompleted, ConfigurationReceiptBatches, ConfigurationReceiptsCompleted, Conflicts int }

func NewJobRetention(cfg JobRetentionConfig, backend JobRetentionBackend, isLeader func() bool) (*JobRetention, error) {
	if cfg.PollInterval == 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 16
	}
	if cfg.DeleteBatchSize == 0 {
		cfg.DeleteBatchSize = db.MaxJobPurgeBatch
	}
	if cfg.CommitTimeout == 0 {
		cfg.CommitTimeout = 2 * time.Second
	}
	if backend == nil || isLeader == nil || cfg.PollInterval <= 0 || cfg.CommitTimeout <= 0 || cfg.BatchSize < 1 || cfg.BatchSize > db.MaxQueryPage || cfg.DeleteBatchSize < 1 || cfg.DeleteBatchSize > db.MaxJobPurgeBatch {
		return nil, fmt.Errorf("invalid job retention configuration")
	}
	prefix, err := newPrefix()
	if err != nil {
		return nil, err
	}
	return &JobRetention{cfg: cfg, backend: backend, isLeader: isLeader, prefix: "retention/" + prefix}, nil
}
func (m *JobRetention) submit(ctx context.Context, at time.Time, c db.JobCommand) (db.StateResult, error) {
	command := db.StateCommand{RequestID: fmt.Sprintf("%s/%d", m.prefix, m.sequence.Add(1)), At: at.UTC(), Job: &c}
	var result db.StateResult
	var err error
	for i := 0; i < 2; i++ {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		commit, cancel := context.WithTimeout(ctx, m.cfg.CommitTimeout)
		result, err = m.backend.ProposeStateCommand(commit, command)
		cancel()
		var rejection *db.CommandRejection
		if err == nil || errors.As(err, &rejection) {
			break
		}
	}
	if err != nil {
		return result, err
	}
	if err = result.Err(); err != nil {
		return result, err
	}
	fp, err := command.Fingerprint()
	if err != nil {
		return result, err
	}
	if result.RequestID != command.RequestID || result.Fingerprint != fp || result.Job == nil {
		return result, fmt.Errorf("cleanup receipt does not match request")
	}
	if c.Purge != nil {
		d := result.Job.Deletion
		if d == nil || d.TenantID != c.Purge.TenantID || d.RunID != c.Purge.RunID {
			return result, fmt.Errorf("purge receipt does not match request")
		}
	} else if c.CompactConfigurationReceipts != nil {
		d := result.Job.ConfigurationReceiptCompaction
		if d == nil || d.TenantID != c.CompactConfigurationReceipts.TenantID || d.ID != c.CompactConfigurationReceipts.ID || d.Step != c.CompactConfigurationReceipts.ExpectedStep+1 {
			return result, fmt.Errorf("configuration compaction receipt does not match request")
		}
	} else if c.CompactReceipts != nil {
		d := result.Job.ReceiptCompaction
		if d == nil || d.TenantID != c.CompactReceipts.TenantID || d.RunID != c.CompactReceipts.RunID {
			return result, fmt.Errorf("compaction receipt does not match request")
		}
	}
	return result, nil
}
func (m *JobRetention) Tick(ctx context.Context, at time.Time) (JobRetentionStats, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	stats := JobRetentionStats{}
	if !m.isLeader() {
		m.pendingCursor = nil
		m.candidateCursor = nil
		m.receiptCursor = nil
		m.configurationReceiptCursor = nil
		return stats, nil
	}
	pending, err := m.backend.ListPendingJobPurges(m.pendingCursor, m.cfg.BatchSize)
	if err != nil {
		return stats, err
	}
	m.pendingCursor = pending.NextCursor
	var commands []db.JobCommand
	for _, d := range pending.Deletions {
		commands = append(commands, db.JobCommand{Purge: &db.PurgeJob{TenantID: d.TenantID, RunID: d.RunID, ExpectedStep: d.Step, FinishedBefore: d.FinishedAt, BatchSize: m.cfg.DeleteBatchSize, Actor: "djob/retention", Reason: "continue committed purge"}})
	}
	receipts, err := m.backend.ListPendingJobReceiptCompactions(m.receiptCursor, m.cfg.BatchSize)
	if err != nil {
		return stats, err
	}
	m.receiptCursor = receipts.NextCursor
	for _, c := range receipts.Compactions {
		commands = append(commands, db.JobCommand{CompactReceipts: &db.CompactJobReceipts{TenantID: c.TenantID, RunID: c.RunID, ExpectedStep: c.Step, BatchSize: m.cfg.DeleteBatchSize, Actor: "djob/retention", Reason: "continue committed receipt compaction"}})
	}
	configurationReceipts, err := m.backend.ListPendingJobConfigurationReceiptCompactions(m.configurationReceiptCursor, m.cfg.BatchSize)
	if err != nil {
		return stats, err
	}
	m.configurationReceiptCursor = configurationReceipts.NextCursor
	for _, c := range configurationReceipts.Compactions {
		commands = append(commands, db.JobCommand{CompactConfigurationReceipts: &db.CompactJobConfigurationReceipts{TenantID: c.TenantID, ID: c.ID, ExpectedStep: c.Step, BatchSize: m.cfg.DeleteBatchSize, Actor: "djob/retention", Reason: "continue committed configuration receipt compaction"}})
	}
	candidates, err := m.backend.ListJobRetentionCandidates(at, m.candidateCursor, m.cfg.BatchSize)
	if err != nil {
		return stats, err
	}
	m.candidateCursor = candidates.NextCursor
	for _, c := range candidates.Candidates {
		commands = append(commands, db.JobCommand{Purge: &db.PurgeJob{TenantID: c.TenantID, RunID: c.RunID, ExpectedStep: c.Step, FinishedBefore: c.FinishedBefore, PolicyRevision: c.PolicyRevision, BatchSize: m.cfg.DeleteBatchSize, Actor: "djob/retention", Reason: "retention policy"}})
	}
	var failures []error
	for _, c := range commands {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		if !m.isLeader() {
			m.pendingCursor = nil
			m.candidateCursor = nil
			m.receiptCursor = nil
			m.configurationReceiptCursor = nil
			break
		}
		result, err := m.submit(ctx, at, c)
		if err != nil {
			var rejection *db.CommandRejection
			if errors.As(err, &rejection) {
				switch rejection.Code {
				case "not_found", "step_conflict", "revision_conflict", "not_due", "stale_time", "job_protected":
					stats.Conflicts++
					continue
				}
			}
			failures = append(failures, err)
			continue
		}
		if c.CompactConfigurationReceipts != nil {
			stats.ConfigurationReceiptBatches++
			if result.Job.ConfigurationReceiptCompaction.Complete {
				stats.ConfigurationReceiptsCompleted++
			}
		} else if c.CompactReceipts != nil {
			stats.ReceiptBatches++
			if result.Job.ReceiptCompaction.Complete {
				stats.ReceiptsCompleted++
			}
		} else {
			stats.Batches++
			if result.Job.Deletion.CleanupComplete {
				stats.Completed++
			}
		}
	}
	return stats, errors.Join(failures...)
}
func (m *JobRetention) Run(ctx context.Context) error {
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
