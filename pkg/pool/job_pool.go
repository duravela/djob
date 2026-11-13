package pool

import (
	"expvar"
	"fmt"
	"time"

	"github.com/panjf2000/ants/v2"
)

type leaser interface {
	Acquire(jobID, nodeID string, leaseDur time.Duration) (bool, error)
	Release(jobID, nodeID string) error
}

// JobPoolStats holds expvar counters for observability.
var JobPoolStats *expvar.Map

func init() {
	JobPoolStats = expvar.NewMap("workerpool")
	JobPoolStats.Add("active_workers", 0)
	JobPoolStats.Add("completed_jobs", 0)
	JobPoolStats.Add("failed_jobs", 0)
}

// JobTask represents a unit of work for the pool.
type JobTask struct {
	JobID  string
	NodeID string
}

// JobPool wraps ants.Pool and uses expvar for stats.
type JobPool struct {
	pool       *ants.Pool
	leaser     leaser
	nodeID     string
	jobReportC chan *JobResult
	leaseDur   time.Duration
}

// NewWorkerPool creates a new pool with the given concurrency.
func NewWorkerPool(
	size int,
	db leaser,
	nodeID string,
	leaseDur time.Duration,
	jobReportC chan *JobResult,
) (*JobPool, error) {
	pool, err := ants.NewPool(size, ants.WithPreAlloc(true), ants.WithNonblocking(true))
	if err != nil {
		return nil, err
	}
	return &JobPool{
		pool:       pool,
		leaser:     db,
		nodeID:     nodeID,
		leaseDur:   leaseDur,
		jobReportC: jobReportC, //make(chan *JobReport, 100),
	}, nil
}

func (jp *JobPool) Submit(jobID string) error {
	return jp.pool.Submit(func() {
		JobPoolStats.Add("active_workers", 1)
		defer JobPoolStats.Add("active_workers", -1)
		acquired, err := jp.leaser.Acquire(jobID, jp.nodeID, jp.leaseDur)
		if err != nil || !acquired {
			JobPoolStats.Add("failed_jobs", 1)
			return
		}
		// Process the job and generate report
		report := processJob(jobID, jp.nodeID)
		if report.Status != "success" {
			JobPoolStats.Add("failed_jobs", 1)
		} else {
			JobPoolStats.Add("completed_jobs", 1)
			_ = jp.leaser.Release(jobID, jp.nodeID)
		}
		jp.jobReportC <- report
	})
}

// TODO: Implement the actual job processing logic.
func processJob(jobID, nodeID string) *JobResult {
	start := time.Now()

	// Simulate work
	fmt.Printf("Processing job %s\n", jobID)
	time.Sleep(500 * time.Millisecond)

	return &JobResult{
		JobID:      jobID,
		NodeID:     nodeID,
		Status:     "success",
		StartedAt:  start,
		FinishedAt: time.Now(),
		Output:     []byte("result"),
		Error:      "",
	}
}

// Close gracefully shuts down the pool.
func (jp *JobPool) Close() {
	_ = jp.pool.ReleaseTimeout(5 * time.Second)
}
