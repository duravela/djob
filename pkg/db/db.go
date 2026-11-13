package db

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sync"
	"time"

	"go.etcd.io/bbolt"
)

func init() {
	gob.Register(Lease{})
	gob.Register(Job{})
}

type Bucket int

const (
	IN_PROGRESS Bucket = iota
	PENDING_PRIO_NORMAL
	PENDING_PRIO_HIGH
	DATA
)

var (
	jobBuckets = [][]byte{
		[]byte("jobs_in_progress"), // Bucket for leased jobs  | Leases
		[]byte("jobs_pending"),     // Bucket for pending jobs | Jobs
		[]byte("jobs_high_pri"),    // Bucket for high priority jobs | jobs
		[]byte("jobs_data"),        // Bucket for job data | Jobs
	}
)

// CommandType defines actions supported via Raft.
type CommandType uint8

const (
	CmdAddJobs        CommandType = 0
	CmdDeleteJob      CommandType = 1
	CmdAddBatchJobs   CommandType = 2 // Retained for historical WAL entries.
	CmdAssignJobs     CommandType = 3 // Reserved; not implemented by this state machine.
	CmdState          CommandType = 4 // Versioned channel, run, and attempt transitions.
	CmdConsoleSession CommandType = 5 // Session-local retries, without permanent job receipts.
)

// Command represents a Raft-applied state mutation.
type Command struct {
	Version        uint32 // Zero denotes the original gob command format.
	Type           CommandType
	Jobs           []Job // Added for batch operations
	State          *StateCommand
	ConsoleSession *ConsoleSessionCommand
}

// Job represents a unit of work with enhanced metadata.
type Job struct {
	ID       string
	Data     string
	TS       time.Time // Creation timestamp
	Deadline time.Time // When this job must be completed by
	Priority int       // 0=low, 1=medium, 2=high
}

// Lease tracks job assignment to workers.
type Lease struct {
	WorkerID       string
	ExpiresAt      time.Time
	PriorityBucket []byte
}

// DB is a raft FSM backed by BoltDB with enhanced features.
type DB struct {
	mu               sync.Mutex
	db               *bbolt.DB
	lastAppliedIndex uint64
	pageSize         int
	closed           bool
	Metrics          *DBMetrics // Added for instrumentation
}

// DBMetrics tracks queue performance (optional).
type DBMetrics struct {
	Pending   int
	Progress  int
	Processed int
}

// NewDB opens the BoltDB and prepares all buckets.
func NewDB(path string) (*DB, error) {
	db, err := bbolt.Open(path, 0600, &bbolt.Options{
		InitialMmapSize: 1 << 30, // 1GB mmap size for better large-queue performance
	})
	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		if err := initializeMetadata(tx); err != nil {
			return err
		}
		for _, bucket := range jobBuckets {
			if _, err := tx.CreateBucketIfNotExists(bucket); err != nil {
				return err
			}
		}
		if err := initializeChannelState(tx); err != nil {
			return err
		}
		if err := initializeSchedulingState(tx); err != nil {
			return err
		}
		if err := initializeDeadlineState(tx); err != nil {
			return err
		}
		if err := initializeRetryState(tx); err != nil {
			return err
		}
		if err := initializeJobState(tx); err != nil {
			return err
		}
		if err := initializeJobTriggerState(tx); err != nil {
			return err
		}
		if err := initializeJobActivationState(tx); err != nil {
			return err
		}
		if err := initializeJobQuotaState(tx); err != nil {
			return err
		}
		if err := initializeJobRetentionState(tx); err != nil {
			return err
		}
		if err := initializeJobReceiptState(tx); err != nil {
			return err
		}
		if err := initializeJobConfigurationState(tx); err != nil {
			return err
		}
		if err := initializeJobConfigurationReceiptState(tx); err != nil {
			return err
		}
		if err := initializeJobTriggerRetirementState(tx); err != nil {
			return err
		}
		if err := initializeJobConfigurationRetirementState(tx); err != nil {
			return err
		}
		if err := initializeWorkerEnrollments(tx); err != nil {
			return err
		}
		if err := initializeWorkerDeployments(tx); err != nil {
			return err
		}
		if err := initializeWorkerReleases(tx); err != nil {
			return err
		}
		if err := initializeWorkerReleaseRunIndex(tx); err != nil {
			return err
		}
		return initializeConsoleSessions(tx)
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	q := &DB{
		db:       db,
		pageSize: db.Info().PageSize,
		Metrics:  &DBMetrics{},
	}
	err = db.View(func(tx *bbolt.Tx) error {
		var err error
		q.lastAppliedIndex, err = readAppliedIndex(tx)
		q.refreshMetrics(tx)
		return err
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return q, nil
}

func (q *DB) HasPendingJob(jobID string) bool {
	bJobID := []byte(jobID)
	found := false
	_ = q.db.View(func(tx *bbolt.Tx) error {
		for _, bucket := range []int{int(PENDING_PRIO_HIGH), int(PENDING_PRIO_NORMAL)} { // high, normal
			if tx.Bucket(jobBuckets[bucket]).Get(bJobID) != nil {
				found = true
				return nil
			}
		}

		return nil
	})

	return found
}

func (q *DB) HasPendingJobs(jobIDs []string) bool {
	if len(jobIDs) == 0 {
		return false
	}
	idSet := make(map[string]struct{}, len(jobIDs))
	for _, id := range jobIDs {
		idSet[id] = struct{}{}
	}
	found := false
	_ = q.db.View(func(tx *bbolt.Tx) error {
		for _, bucket := range []int{int(PENDING_PRIO_HIGH), int(PENDING_PRIO_NORMAL)} { // jobs_high_pri, jobs_pending
			b := tx.Bucket(jobBuckets[bucket])
			if b == nil {
				continue
			}
			c := b.Cursor()
			for k, _ := c.First(); k != nil; k, _ = c.Next() {
				if _, ok := idSet[string(k)]; ok {
					found = true
					return nil
				}
			}
			if found {
				break
			}
		}
		return nil
	})
	return found
}

func (q *DB) FetchPendingJobs(count int) ([]Job, error) {
	var jobs []Job
	err := q.db.View(func(tx *bbolt.Tx) error {
		scanBucket(tx.Bucket(jobBuckets[PENDING_PRIO_HIGH]), &jobs, count)
		if len(jobs) < count {
			scanBucket(tx.Bucket(jobBuckets[PENDING_PRIO_NORMAL]), &jobs, count)
		}
		return nil
	})

	return jobs, err
}

func scanBucket(b *bbolt.Bucket, jobs *[]Job, count int) {
	if b == nil {
		return
	}
	c := b.Cursor()
	for k, v := c.First(); k != nil && len(*jobs) < count; k, v = c.Next() {
		var job Job
		if err := gob.NewDecoder(bytes.NewReader(v)).Decode(&job); err != nil {
			continue
		}
		*jobs = append(*jobs, job)
	}
}

type ApplyCommandResult struct {
	JobErrors             map[string]error // Errors per job
	StateResults          []StateResult
	ConsoleSessionResults []ConsoleSessionResult
	Error                 error // General error not tied to specific jobs
}

func (q *DB) AddJob(job Job) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	before := *q.Metrics
	err := q.db.Update(func(tx *bbolt.Tx) error {
		return q.addJob(tx, job)
	})
	if err != nil {
		*q.Metrics = before
		return fmt.Errorf("failed to add job %s: %w", job.ID, err)
	}
	return nil
}

// addJob handles the actual job insertion into the appropriate bucket.
// This function is used by ApplyCommand via Raft.
//
// One might call it directly
// for testing purposes, but it is not intended for direct use in normal
func (q *DB) addJob(tx *bbolt.Tx, job Job) error {
	bucketName := jobBuckets[PENDING_PRIO_NORMAL] // Default: normal priority | pending
	if job.Priority == 1 {
		bucketName = jobBuckets[PENDING_PRIO_HIGH] // High priority
	}

	b := tx.Bucket(bucketName)
	if b == nil {
		// should not happen!
		return fmt.Errorf("bucket %s does not exist", bucketName)
	}
	bytes, err := EncodeJob(job)
	if err != nil {
		return err
	}
	err = b.Put([]byte(job.ID), bytes)
	if err == nil {
		q.Metrics.Pending++
	}

	return err
}

// deleteJob removes a job from all buckets.
// Its intended use case is administrative or manual deletion of jobs
// — removing a job from all buckets, regardless of its state (pending,
// in-progress, etc). This is useful for:
//   - Canceling jobs before they are processed.
//   - Cleaning up jobs due to errors or admin actions.
//   - Removing jobs that may not have been acquired/leased.
func (q *DB) deleteJob(tx *bbolt.Tx, jobID string) error {
	for i, bucket := range jobBuckets {
		existed := tx.Bucket(bucket).Get([]byte(jobID)) != nil
		if err := tx.Bucket(bucket).Delete([]byte(jobID)); err != nil {
			return err
		}
		if existed && (Bucket(i) == PENDING_PRIO_NORMAL || Bucket(i) == PENDING_PRIO_HIGH) {
			q.Metrics.Pending--
		}
	}
	return nil
}

// Acquire moves a job to in-progress with a worker lease.
// The workerID references the node local process.
func (q *DB) Acquire(jobID string, workerID string, leaseDuration time.Duration) (bool, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	acquired := false
	err := q.db.Update(func(tx *bbolt.Tx) error {
		var jobData []byte
		var priorityBucket []byte
		for _, bucketName := range jobBuckets[PENDING_PRIO_NORMAL:DATA] { // Only check pending/high
			if data := tx.Bucket(bucketName).Get([]byte(jobID)); data != nil {
				jobData = data
				priorityBucket = bucketName
				break
			}
		}
		if jobData == nil {
			return nil
		}
		if err := tx.Bucket(jobBuckets[DATA]).Put([]byte(jobID), jobData); err != nil {
			return err
		}
		lease := Lease{WorkerID: workerID, ExpiresAt: time.Now().Add(leaseDuration), PriorityBucket: priorityBucket}
		leaseData, err := lease.Encode()
		if err != nil {
			return err
		}
		if err := tx.Bucket(priorityBucket).Delete([]byte(jobID)); err != nil {
			return err
		}
		if err := tx.Bucket(jobBuckets[IN_PROGRESS]).Put([]byte(jobID), leaseData); err != nil {
			return err
		}
		acquired = true
		return nil
	})
	if err == nil && acquired {
		q.Metrics.Pending--
		q.Metrics.Progress++
	}
	return acquired && err == nil, err
}

// Release remove both the lease (from the jobs_in_progress bucket)
// and the job data (from the jobs_data bucket). This prevents the job
// from being re-queued.
func (q *DB) Release(jobID, workerID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	released := false
	err := q.db.Update(func(tx *bbolt.Tx) error {
		inProgress := tx.Bucket(jobBuckets[IN_PROGRESS])
		dataBucket := tx.Bucket(jobBuckets[DATA])

		// Optionally, check lease ownership
		leaseData := inProgress.Get([]byte(jobID))
		if leaseData != nil {
			lease, err := DecodeLease(leaseData)
			if err != nil {
				return err
			}
			if lease.WorkerID != workerID {
				return fmt.Errorf("job %s is not leased to worker %s", jobID, workerID)
			}
			released = true
		}

		if err := inProgress.Delete([]byte(jobID)); err != nil {
			return err
		}
		if err := dataBucket.Delete([]byte(jobID)); err != nil {
			return err
		}
		return nil
	})
	if err == nil && released {
		q.Metrics.Progress--
		q.Metrics.Processed++
	}
	return err
}

// encodeLease helper for serializing lease data
func (l *Lease) Encode() ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(l); err != nil {
		return nil, fmt.Errorf("lease encode failed: %w", err)
	}
	return buf.Bytes(), nil
}

func DecodeLease(data []byte) (*Lease, error) {
	var l Lease
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&l); err != nil {
		return nil, fmt.Errorf("lease decode failed: %w", err)
	}
	return &l, nil
}

func EncodeJob(j Job) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(j); err != nil {
		return nil, fmt.Errorf("job encode failed: %w", err)
	}
	return buf.Bytes(), nil
}

// DecodeJob handles proper deserialization of Job struct
func DecodeJob(data []byte) (*Job, error) {
	var j Job
	dec := gob.NewDecoder(bytes.NewReader(data))
	if err := dec.Decode(&j); err != nil {
		return nil, fmt.Errorf("job decode failed: %w", err)
	}
	return &j, nil
}

// RequeueExpiredJobs moves expired leases back to pending.
func (q *DB) RequeueExpiredJobs() (int, error) {
	return q.requeueExpiredJobs(time.Now())
}

func (q *DB) requeueExpiredJobs(now time.Time) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var count int

	err := q.db.Update(func(tx *bbolt.Tx) error {
		dataBucket := tx.Bucket(jobBuckets[DATA])
		src := tx.Bucket(jobBuckets[0]) // in_progress

		return src.ForEach(func(k, v []byte) error {
			lease, err := DecodeLease(v)
			if err != nil {
				return fmt.Errorf("failed to decode lease for job %s: %w", k, err)
			}

			if now.After(lease.ExpiresAt) {
				jobData := dataBucket.Get(k)
				if jobData == nil {
					return fmt.Errorf("missing job data for %s", k)
				}

				targetBucket := tx.Bucket(lease.PriorityBucket)
				if err := targetBucket.Put(k, jobData); err != nil {
					return err
				}

				// Clean up
				if err := src.Delete(k); err != nil {
					return err
				}
				if err := dataBucket.Delete(k); err != nil {
					return err
				}

				count++
			}
			return nil
		})
	})

	if err == nil {
		q.Metrics.Progress -= count
		q.Metrics.Pending += count
	}
	return count, err
}

// LastAppliedIndex returns the last applied log index.
func (q *DB) LastAppliedIndex() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.lastAppliedIndex
}

func (q *DB) Stats() DBMetrics {
	q.mu.Lock()
	defer q.mu.Unlock()
	return *q.Metrics
}

// Close shuts down the database.
func (q *DB) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	return q.db.Close()
}

// Size returns approximate queue sizes (pending + in-progress).
func (q *DB) Size() (int, int) {
	q.mu.Lock()
	defer q.mu.Unlock()

	var pending, inProgress int
	_ = q.db.View(func(tx *bbolt.Tx) error {
		pending = tx.Bucket(jobBuckets[PENDING_PRIO_NORMAL]).Stats().KeyN + tx.Bucket(jobBuckets[PENDING_PRIO_HIGH]).Stats().KeyN
		inProgress = tx.Bucket(jobBuckets[IN_PROGRESS]).Stats().KeyN
		return nil
	})
	return pending, inProgress
}
