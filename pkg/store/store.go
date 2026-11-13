package store

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/duravela/djob/pkg/commit"
	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/pool"
	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

const (
	defaultStoreDir = "store"
)

var (
	ErrProposalTimeout = errors.New("raft proposal timeout")
	MaxPendingRequests = 1000

	MaxWorkers           = 10
	DefaultLeaseDuration = 30 * time.Second
	DefaultWorkerID      = "default-worker"
)

type StoreConfig struct {
	DataDir              string
	ProposalTimeout      time.Duration
	DisableLegacyWorkers bool

	Logger *zap.Logger
}

type Submitter interface {
	Submit(string) error
	Close()
}

/*
Store manages a distributed, fault-tolerant job queue using Raft consensus.

The Store is responsible for:
  - Replicated Job Storage: Persists jobs in a local database (db.DB) and ensures
    the job state is replicated across all nodes in the cluster via Raft. This allows
    any node to take over processing if the leader fails.
  - Consensus on State Changes: All job-related state changes (add, delete, update)
    are proposed to the Raft cluster, guaranteeing strong consistency and durability.
  - Proposal Tracking: Tracks pending job proposals, notifies clients of their outcome,
    and handles timeouts for proposals that are not committed in time.
  - Commit Processing: Applies committed Raft log entries to the job store and notifies
    waiting clients of the result.
  - Snapshotting and Recovery: Supports creating and loading snapshots of the job store
    for fast recovery and log compaction.
  - Metrics and Monitoring: Exposes Prometheus metrics for proposals, commits, latency,
    and pending requests.

In summary, Store is the core component that manages distributed, fault-tolerant job
storage and processing coordination, leveraging Raft for consensus and providing APIs for
job proposal, tracking, and state inspection.
It coordinates job assignment, tracks job state, and ensures consistency
across distributed worker nodes.
*/
type Store struct {

	// cfg holds configuration parameters for the job store, such as data directory,
	// snapshot settings, proposal timeouts, and other tunables.
	cfg StoreConfig

	// logger is used for structured logging of store operations, errors, and state changes.
	// It aids in debugging and monitoring.
	logger *zap.Logger

	// storedir is the filesystem path where persistent job data and Raft state are stored.
	storedir string

	// proposeC is a channel for submitting new commands (jobs) to the Raft cluster.
	// The Store sends serialized job proposals into this channel for consensus.
	proposeC chan<- []byte

	// commitC is a channel for receiving committed Raft log entries.
	// The Store listens on this channel to apply committed jobs and state changes to the database.
	commitC <-chan *commit.Commit

	// snapshotter manages Raft state snapshots, enabling log compaction and fast recovery.
	snapshotter *snap.Snapshotter

	// metrics collects Prometheus metrics for monitoring proposals, commits, latency,
	// and pending requests, supporting observability and performance analysis.
	metrics *storeMetrics

	// mu is a read-write mutex used to synchronize access to the Store's internal state,
	// ensuring thread-safe operations.
	mu              sync.RWMutex
	admissionPolicy func(db.StateCommand) error
	recoveryHeld    bool

	// db is the underlying persistent job database, backed by BoltDB.
	// It stores job definitions, states, and metadata.
	db *db.DB

	// pendingReqs tracks outstanding job proposals and their associated completion channels.
	// This allows the Store to notify callers when their proposals have been committed.
	pendingProposalBytes int
	pendingReqs          map[string]*pendingRequest
	pendingState         map[stateRequestKey]map[chan db.StateResult]struct{}
	pendingSessions      map[[32]byte]map[chan db.ConsoleSessionResult]struct{}

	// jobSubmitter abstracts the logic for submitting jobs to the Store,
	// enabling different submission strategies or implementations.
	jobSubmitter Submitter

	// SnapshotterReady signals when the snapshotter instance is initialized and ready for use.
	// Background routines can wait on this channel before performing snapshot operations.
	SnapshotterReady chan *snap.Snapshotter // signals when snapshotter is ready

	// reportStore collects and stores job execution results from workers.
	// It is queried for job status and used to track job outcomes and metrics.
	reportStore *pool.JobReportStore

	// stopC is a channel used to signal shutdown of background goroutines managed by the Store.
	// Closing this channel initiates a graceful shutdown sequence.
	stopC        chan struct{}
	stopOnce     sync.Once
	shutdownOnce sync.Once
	wg           sync.WaitGroup
	failure      error
	recovered    chan struct{}
	recoveryOnce sync.Once

	// jobC is a channel for sending job IDs to worker routines for processing.
	// The Store enqueues jobs into this channel for execution.
	jobC chan string

	// jobReportC is a channel for receiving job results from workers.
	// The Store listens on this channel to update the reportStore with job outcomes.
	jobReportC chan *pool.JobResult

	// jobReportStopC is a channel used to signal shutdown of job report processing routines.
	jobReportStopC chan struct{}

	// OnJobReportReceived is an optional callback invoked whenever a job report is received.
	// This allows external components to react to job completion events.
	OnJobReportReceived func(report *pool.JobResult)

	// OnJobIDReceived is an optional callback invoked whenever a job ID is received for processing.
	// This allows external components to react to job assignment events.
	OnJobIDReceived func(jobID string)
}

type storeMetrics struct {
	Proposals       prometheus.Counter
	Commits         prometheus.Counter
	ProposalLatency prometheus.Histogram
	PendingRequests prometheus.Gauge
}

type pendingRequest struct {
	reqc      chan error
	startTime time.Time
}

func NewStore(
	cfg StoreConfig,
	proposeC chan<- []byte,
	commitC <-chan *commit.Commit,
	snapshotter *snap.Snapshotter,
) (*Store, error) {
	js := &Store{
		cfg:              cfg,
		proposeC:         proposeC,
		commitC:          commitC,
		jobC:             make(chan string, 100), // Buffer size
		logger:           cfg.Logger.Named("store"),
		pendingReqs:      make(map[string]*pendingRequest),
		pendingState:     make(map[stateRequestKey]map[chan db.StateResult]struct{}),
		stopC:            make(chan struct{}),
		recovered:        make(chan struct{}),
		metrics:          newStoreMetrics(),
		storedir:         filepath.Join(cfg.DataDir, defaultStoreDir),
		SnapshotterReady: make(chan *snap.Snapshotter, 1),
		jobReportC:       make(chan *pool.JobResult, 1000),
		jobReportStopC:   make(chan struct{}),
		snapshotter:      snapshotter,
	}

	js.OnJobReportReceived = js.defaultOnJobReportReceived

	return js, nil
}

func (jq *Store) Start() error {
	if info, err := os.Lstat(filepath.Join(jq.cfg.DataDir, ".djob-recovery-hold")); err == nil {
		if !info.Mode().IsRegular() {
			return fmt.Errorf("recovery hold must be a regular file")
		}
		jq.recoveryHeld = true
	} else if !os.IsNotExist(err) {
		return err
	}
	if !fileutil.Exist(jq.storedir) {
		if err := os.MkdirAll(jq.storedir, 0o750); err != nil {
			return fmt.Errorf("create job store: %w", err)
		}
	}
	db, err := db.NewDB(filepath.Join(jq.storedir, "jobs.snap"))
	if err != nil {
		return fmt.Errorf("open job database: %w", err)
	}
	jq.db = db

	// jq.snapshotter = snap.New(jq.logger.Named("snap"), jq.storedir)
	jq.SnapshotterReady <- jq.snapshotter
	jq.reportStore = pool.NewJobReportStore(10*time.Minute, 1000)
	if jq.cfg.DisableLegacyWorkers {
		jq.OnJobIDReceived = func(string) {}
	} else if jq.jobSubmitter == nil {
		wp, err := pool.NewWorkerPool(
			MaxWorkers,
			db,
			DefaultWorkerID,
			DefaultLeaseDuration,
			jq.jobReportC,
		)
		if err != nil {
			_ = db.Close()
			return fmt.Errorf("create worker pool: %w", err)
		}
		jq.jobSubmitter = wp
		jq.OnJobIDReceived = jq.defaultOnJobIDReceived
	}

	for _, run := range []func(){jq.readCommits, jq.monitorPendingRequests, jq.readJobs, jq.readJobReports} {
		jq.wg.Add(1)
		go func() { defer jq.wg.Done(); run() }()
	}
	return nil
}

func (jq *Store) Stop() {
	jq.shutdownOnce.Do(func() {
		jq.stopOnce.Do(func() { close(jq.stopC) })
		jq.wg.Wait()
		if jq.jobSubmitter != nil {
			jq.jobSubmitter.Close()
		}
		if jq.db != nil {
			_ = jq.db.Close()
		}
	})
}

func (jq *Store) stopError() error {
	jq.mu.RLock()
	defer jq.mu.RUnlock()
	if jq.failure != nil {
		return fmt.Errorf("job queue stopped: %w", jq.failure)
	}
	return errors.New("job queue stopped")
}

func (jq *Store) fail(err error) {
	jq.mu.Lock()
	jq.failure = err
	jq.mu.Unlock()
	jq.stopOnce.Do(func() { close(jq.stopC) })
}

func (jq *Store) ProposeCommand(ctx context.Context, cmd db.Command) (map[string]error, error) {
	select {
	case <-jq.stopC:
		return nil, jq.stopError()
	default:
	}

	if len(cmd.Jobs) == 0 {
		return nil, errors.New("empty command")
	}

	// Create tracking structure for each job
	type jobTracking struct {
		id   string
		reqc chan error
	}
	trackers := make([]*jobTracking, 0, len(cmd.Jobs))

	jq.mu.Lock()
	// Validate jobs and create tracking channels
	for _, job := range cmd.Jobs {
		if job.ID == "" {
			jq.mu.Unlock()
			return nil, fmt.Errorf("job has empty ID: %+v", job)
		}

		tracker := &jobTracking{
			id:   job.ID,
			reqc: make(chan error, 1),
		}
		trackers = append(trackers, tracker)
		jq.pendingReqs[job.ID] = &pendingRequest{
			reqc:      tracker.reqc,
			startTime: time.Now(),
		}
		jq.metrics.PendingRequests.Inc()
	}
	jq.mu.Unlock()

	// Cleanup function for pending requests
	defer func() {
		jq.mu.Lock()
		for _, tracker := range trackers {
			delete(jq.pendingReqs, tracker.id)
			jq.metrics.PendingRequests.Dec()
		}
		jq.mu.Unlock()
	}()

	// Encode and propose the command
	cmd.Version = db.LegacyCommandVersion
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(cmd); err != nil {
		// Notify all trackers about encoding failure
		for _, tracker := range trackers {
			tracker.reqc <- fmt.Errorf("encoding failed: %w", err)
		}
		return nil, fmt.Errorf("encoding failed: %w", err)
	}

	release, err := jq.reserveProposal(buf.Len())
	if err != nil {
		return nil, err
	}
	defer release()
	select {
	case jq.proposeC <- buf.Bytes():
		jq.metrics.Proposals.Inc()
		start := time.Now()

		// Collect results from all job channels
		results := make(map[string]error)
		for _, tracker := range trackers {
			select {
			case err := <-tracker.reqc:
				results[tracker.id] = err
			case <-ctx.Done():
				return results, ctx.Err()
			case <-jq.stopC:
				return results, jq.stopError()
			}
		}

		jq.metrics.ProposalLatency.Observe(time.Since(start).Seconds())
		return results, nil

	case <-ctx.Done():
		return nil, ctx.Err()
	case <-jq.stopC:
		return nil, jq.stopError()
	}
}

func (jq *Store) applyCommand(c *commit.Commit) *db.ApplyCommandResult {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	jobResults := jq.db.ApplyEntries(c.Entries)

	if jobResults.Error == nil {
		jq.metrics.Commits.Inc()
	}
	return jobResults
}

func (jq *Store) readCommits() {
	var recoveryTarget uint64
	recoverySeen := false
	for {
		select {
		case c, ok := <-jq.commitC:
			if !ok {
				jq.fail(errors.New("Raft commit stream closed"))
				return
			}
			if c == nil {
				if err := jq.loadLatestSnapshot(); err != nil {
					jq.fail(err)
					return
				}
				continue
			}
			var err error
			var results map[string]error
			var stateResults []db.StateResult
			var sessionResults []db.ConsoleSessionResult
			switch {
			case c.Recovery != nil:
				recoveryTarget, recoverySeen = c.Recovery.CommittedIndex, true
				if jq.LastAppliedIndex() > c.Recovery.CommittedIndex {
					err = fmt.Errorf("database index %d exceeds WAL commit index %d", jq.LastAppliedIndex(), c.Recovery.CommittedIndex)
				} else if c.Recovery.Snapshot != nil {
					err = jq.restoreRaftSnapshot(c.Recovery.Snapshot)
				}
			case c.Snapshot != nil:
				err = jq.restoreRaftSnapshot(c.Snapshot)
			default:
				result := jq.applyCommand(c)
				err, results = result.Error, result.JobErrors
				stateResults = result.StateResults
				sessionResults = result.ConsoleSessionResults
			}
			if err != nil {
				jq.logger.Error("state application failed", zap.Error(err))
				jq.fail(err)
				if c.ApplyErrorC != nil {
					select {
					case c.ApplyErrorC <- err:
					default:
					}
				}
				return
			}
			jq.notifyProposals(results)
			jq.notifyStateProposals(stateResults)
			jq.notifyConsoleSessions(sessionResults)
			if recoverySeen && jq.LastAppliedIndex() >= recoveryTarget {
				jq.recoveryOnce.Do(func() { close(jq.recovered) })
			}
			if c.ApplyDoneC != nil {
				close(c.ApplyDoneC)
			}

		case <-jq.stopC:
			return
		}
	}
}

// WaitForRecovery waits for the WAL-selected snapshot and committed tail to be
// durably applied before the public server exposes local inspection queries.
func (jq *Store) WaitForRecovery(ctx context.Context) error {
	select {
	case <-jq.recovered:
		select {
		case <-jq.stopC:
			return jq.stopError()
		default:
			return nil
		}
	case <-jq.stopC:
		return jq.stopError()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (jq *Store) notifyProposals(jobResults map[string]error) {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	for jobId, err := range jobResults {
		if reqc, exists := jq.pendingReqs[jobId]; exists {
			select {
			case reqc.reqc <- err:
			default:
			}
		}
	}
}

// loadLatestSnapshot handles snapshot loading with proper error handling
func (jq *Store) loadLatestSnapshot() error {
	if jq.snapshotter == nil {
		return nil
	}
	snapshot, err := jq.snapshotter.Load()
	switch {
	case errors.Is(err, snap.ErrNoSnapshot):
		jq.logger.Info("no existing snapshot found")
		return nil
	case err != nil:
		return fmt.Errorf("failed to load snapshot: %w", err)
	}

	jq.logger.Info("loading snapshot",
		zap.Uint64("term", snapshot.Metadata.Term),
		zap.Uint64("index", snapshot.Metadata.Index),
	)

	if err := jq.restoreRaftSnapshot(&commit.Snapshot{Index: snapshot.Metadata.Index, Data: snapshot.Data}); err != nil {
		return fmt.Errorf("failed to restore snapshot: %w", err)
	}

	jq.logger.Info("snapshot restored successfully",
		zap.Uint64("term", snapshot.Metadata.Term),
		zap.Uint64("index", snapshot.Metadata.Index),
	)
	return nil
}

func (jq *Store) loadSnapshot(snapshot []byte) error {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	if err := jq.db.RestoreBytes(snapshot); err != nil {
		return fmt.Errorf("restore failed: %w", err)
	}
	return nil
}

func (jq *Store) restoreRaftSnapshot(snapshot *commit.Snapshot) error {
	jq.mu.Lock()
	defer jq.mu.Unlock()
	// A local database can be ahead of the WAL-selected snapshot. Replaying
	// individual indexed entries preserves that newer durable prefix.
	if jq.db.LastAppliedIndex() >= snapshot.Index {
		return nil
	}
	return jq.db.RestoreBytesAt(snapshot.Data, snapshot.Index)
}

func (jq *Store) LastAppliedIndex() uint64 {
	jq.mu.RLock()
	defer jq.mu.RUnlock()
	return jq.db.LastAppliedIndex()
}

// newStoreMetrics creates private collectors. Registration is explicit so two
// members in one process, or a replacement store, cannot share stale collectors.
func newStoreMetrics() *storeMetrics {
	const namespace = "jobqueue"
	const subsystem = "raft"

	metrics := &storeMetrics{
		Proposals: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "proposals_total",
			Help:      "Total number of job proposals made to the Raft cluster",
		}),
		Commits: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "commits_total",
			Help:      "Successfully applied Raft entry batches, including replay and no-op entries",
		}),
		ProposalLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "proposal_latency_seconds",
			Help:      "Local proposal latency through receipt acknowledgement, including durable rejections",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 16),
		}),
		PendingRequests: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "pending_requests",
			Help:      "Current number of pending job proposals awaiting consensus",
		}),
	}

	return metrics
}

// RegisterMetrics registers this store's in-memory collectors with a caller's
// registry. Collection performs no database reads or consensus operations.
func (s *Store) RegisterMetrics(registry prometheus.Registerer) error {
	for _, metric := range []prometheus.Collector{
		s.metrics.Proposals,
		s.metrics.Commits,
		s.metrics.ProposalLatency,
		s.metrics.PendingRequests,
	} {
		if err := registry.Register(metric); err != nil {
			return fmt.Errorf("register store metrics: %w", err)
		}
	}
	return nil
}

func (jq *Store) monitorPendingRequests() {
	const monitoringInterval = 1 * time.Second
	ticker := time.NewTicker(monitoringInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			jq.mu.RLock()
			now := time.Now()
			pendingCount := len(jq.pendingReqs)
			var timedOutJobs []string

			// Check for stale proposals and collect metrics
			for jobID, pr := range jq.pendingReqs {
				// Track proposals approaching timeout
				age := now.Sub(pr.startTime)
				if age > jq.cfg.ProposalTimeout {
					timedOutJobs = append(timedOutJobs, jobID)
					jq.logger.Error("job proposal timeout exceeded",
						zap.String("jobID", jobID),
						zap.Duration("timeout", jq.cfg.ProposalTimeout),
						zap.Duration("age", age),
					)
				} else if age > jq.cfg.ProposalTimeout/2 {
					jq.logger.Warn("job proposal approaching timeout",
						zap.String("jobID", jobID),
						zap.Duration("remaining", jq.cfg.ProposalTimeout-age),
					)
				}
			}
			jq.mu.RUnlock()

			// Log system-level warnings
			if pendingCount > MaxPendingRequests {
				jq.logger.Error("pending requests exceeded maximum capacity",
					zap.Int("pending", pendingCount),
					zap.Int("max", MaxPendingRequests),
				)
			} else if pendingCount > int(float64(MaxPendingRequests)*0.8) {
				jq.logger.Warn("high pending requests",
					zap.Int("pending", pendingCount),
					zap.Int("max", MaxPendingRequests),
				)
			}

			// Handle timed out jobs (outside the lock)
			if len(timedOutJobs) > 0 {
				jq.handleTimedOutJobs(timedOutJobs)
			}

		case <-jq.stopC:
			return
		}
	}
}

// handleTimedOutJobs cleans up expired proposals
func (jq *Store) handleTimedOutJobs(jobIDs []string) {
	jq.mu.Lock()
	defer jq.mu.Unlock()

	for _, jobID := range jobIDs {
		if pr, exists := jq.pendingReqs[jobID]; exists {
			// Send timeout error to waiting goroutine
			select {
			case pr.reqc <- ErrProposalTimeout:
				jq.logger.Debug("notified timeout for job", zap.String("jobID", jobID))
			default:
				jq.logger.Warn("timeout notification failed for job",
					zap.String("jobID", jobID),
				)
			}

			// Cleanup the pending request
			delete(jq.pendingReqs, jobID)
			jq.metrics.PendingRequests.Dec()
		}
	}
}

func validateCommand(cmd db.Command) error {
	return nil
}

func (jq *Store) Snapshot() ([]byte, error) {
	jq.mu.RLock()
	defer jq.mu.RUnlock()
	var snapData bytes.Buffer
	if err := jq.db.Snapshot(&snapData); err != nil {
		return nil, fmt.Errorf("failed to write snapshot: %w", err)
	}
	return snapData.Bytes(), nil
}

func (jq *Store) SnapshotAt(index uint64) ([]byte, error) {
	encode, err := jq.PrepareSnapshotAt(index)
	if err != nil {
		return nil, err
	}
	return encode()
}

// PrepareSnapshotAt captures a consistent view before Raft advances its Ready.
// The caller must invoke the returned encoder exactly once, even on shutdown.
func (jq *Store) PrepareSnapshotAt(index uint64) (func() ([]byte, error), error) {
	jq.mu.RLock()
	encode, err := jq.db.PrepareSnapshotAt(index)
	jq.mu.RUnlock()
	if err != nil {
		return nil, err
	}
	return func() ([]byte, error) {
		return spoolSnapshot(jq.storedir, encode)
	}, nil
}

func (s *Store) LoadSnapshot() (*raftpb.Snapshot, error) {
	if s.snapshotter == nil {
		return nil, nil
	}
	snapshot, err := s.snapshotter.Load()
	if errors.Is(err, snap.ErrNoSnapshot) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (s *Store) RecoverFromSnapshot(snapshot []byte) error {
	return s.loadSnapshot(snapshot)
}

func (s *Store) BucketStats() *db.DBMetrics {
	stats := s.db.Stats()
	return &stats
}

// Assign a job to the worker pool
func (js *Store) AssignJob(jobID string) error {
	if !js.db.HasPendingJob(jobID) {
		return fmt.Errorf("job %s does not exist", jobID)
	}
	js.jobC <- jobID
	return nil
}

func (js *Store) AssignJobs(jobIDs []string) error {
	if !js.db.HasPendingJobs(jobIDs) {
		return fmt.Errorf("none of the provided jobs exist or are pending: %v", jobIDs)
	}
	for _, jobID := range jobIDs {
		js.jobC <- jobID
	}
	return nil
}

func (s *Store) StatusJob(jobID string) (*db.Lease, error) {
	panic("Not implemented")
}

func (js *Store) GetJobReport(jobID string) (*pool.JobResult, bool) {
	return js.reportStore.Get(jobID)
}

func (js *Store) GetAllJobReports() []*pool.JobResult {
	return js.reportStore.GetAll()
}

func (js *Store) DeleteJobReport(jobID string) {
	js.reportStore.Delete(jobID)
}

func (jq *Store) readJobs() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-jq.stopC:
			return
		case jobID, ok := <-jq.jobC:
			if !ok {
				return // channel closed
			}
			jq.OnJobIDReceived(jobID)
		}
	}
}

func (jq *Store) readJobReports() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-jq.stopC:
			return
		case report, ok := <-jq.jobReportC:
			if !ok {
				return // channel closed
			}
			jq.OnJobReportReceived(report)
		}
	}
}

func (jq *Store) defaultOnJobReportReceived(report *pool.JobResult) {
	jq.reportStore.Add(report)
}

func (jq *Store) defaultOnJobIDReceived(jobID string) {
	_ = jq.jobSubmitter.Submit(jobID)
}
