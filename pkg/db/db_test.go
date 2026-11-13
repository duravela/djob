package db

import (
	"bytes"
	"encoding/gob"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func NewTestDB(t *testing.T) (*DB, func()) {
	t.Helper()
	path, cleanup := tempDB(t)
	db, err := NewDB(path)
	require.NoError(t, err)
	return db, func() {
		db.Close()
		cleanup()
	}
}

func tempDB(t *testing.T) (string, func()) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	return path, func() { os.RemoveAll(dir) }
}

func encode(data any) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func newCommit(cmdType CommandType, jobs []Job) ([][]byte, error) {
	cmd := Command{Type: cmdType, Jobs: jobs}
	data, err := encode(cmd)
	if err != nil {
		return nil, err
	}
	return [][]byte{data}, nil
}

func TestJobQueue_ApplyCommand_AddAndDelete(t *testing.T) {
	path, cleanup := tempDB(t)
	defer cleanup()

	q, err := NewDB(path)
	require.NoError(t, err)
	defer q.Close()

	// Test single job add
	job := Job{ID: "abc", Data: "payload", TS: time.Now()}
	commit, err := newCommit(CmdAddJobs, []Job{job})
	if err != nil {
		t.Fatalf("encoding failed: %v", err)
	}
	require.NoError(t, q.ApplyCommand(commit, 1).Error)

	// Verify job exists by trying to delete it
	commit, err = newCommit(CmdDeleteJob, []Job{{ID: "abc"}})
	if err != nil {
		t.Fatalf("encoding failed: %v", err)
	}
	require.NoError(t, q.ApplyCommand(commit, 2).Error)

	// Verify deletion by trying to delete again (should be no-op)
	require.NoError(t, q.ApplyCommand(commit, 3).Error)
}

func TestJobQueue_ApplyCommand_BatchDeleteAcknowledgesEveryJob(t *testing.T) {
	q, cleanup := NewTestDB(t)
	defer cleanup()
	jobs := []Job{{ID: "normal"}, {ID: "high", Priority: 1}, {ID: "keep"}}
	data, err := newCommit(CmdAddJobs, jobs)
	require.NoError(t, err)
	require.NoError(t, q.ApplyCommand(data, 1).Error)

	data, err = newCommit(CmdDeleteJob, []Job{{ID: "normal"}, {ID: "high"}, {ID: "missing"}})
	require.NoError(t, err)
	result := q.ApplyCommand(data, 2)
	require.NoError(t, result.Error)
	require.Len(t, result.JobErrors, 3)
	for _, id := range []string{"normal", "high", "missing"} {
		err, acknowledged := result.JobErrors[id]
		require.True(t, acknowledged, "missing acknowledgement for %s", id)
		require.NoError(t, err)
	}
	pending, err := q.FetchPendingJobs(10)
	require.NoError(t, err)
	require.Equal(t, []Job{{ID: "keep"}}, pending)
	require.Equal(t, 1, q.Metrics.Pending)
}

func TestJobQueue_SnapshotAndRestore(t *testing.T) {
	path, cleanup := tempDB(t)
	defer cleanup()

	q1, err := NewDB(path)
	require.NoError(t, err)

	jobs := []Job{
		{ID: "job1", Data: "payload1", TS: time.Now()},
		{ID: "job2", Data: "payload2", TS: time.Now().Add(-time.Hour), Priority: 1},
	}
	commit, err := newCommit(CmdAddJobs, jobs)
	if err != nil {
		t.Fatalf("encoding failed: %v", err)
	}
	require.NoError(t, q1.ApplyCommand(commit, 1).Error)

	var buf bytes.Buffer
	require.NoError(t, q1.Snapshot(&buf))
	require.NoError(t, q1.Close())

	// Restore into a new instance
	q2, err := NewDB(path)
	require.NoError(t, err)
	defer q2.Close()

	require.NoError(t, q2.Restore(&buf))
	require.Equal(t, uint64(1), q2.LastAppliedIndex())

	// Verify jobs were restored by checking size
	pending, inProgress := q2.Size()
	require.Equal(t, 2, pending)
	require.Equal(t, 0, inProgress)
}

func TestJobQueue_AcquireAndRelease(t *testing.T) {
	path, cleanup := tempDB(t)
	defer cleanup()

	q, err := NewDB(path)
	require.NoError(t, err)
	defer q.Close()

	job := Job{ID: "lease-test", Data: "test"}
	commit, err := newCommit(CmdAddJobs, []Job{job})
	if err != nil {
		t.Fatalf("encoding failed: %v", err)
	}
	require.NoError(t, q.ApplyCommand(commit, 1).Error)

	// Test successful acquire
	acquired, err := q.Acquire(job.ID, "worker1", time.Minute)
	require.NoError(t, err)
	require.True(t, acquired)

	// Verify job moved to in-progress
	pending, inProgress := q.Size()
	require.Equal(t, 0, pending)
	require.Equal(t, 1, inProgress)

	// Test duplicate acquire fails
	acquired, err = q.Acquire(job.ID, "worker2", time.Minute)
	require.NoError(t, err)
	require.False(t, acquired)

	// Test requeue of expired jobs
	count, err := q.RequeueExpiredJobs()
	require.NoError(t, err)
	require.Equal(t, 0, count) // Lease not expired yet

	// Should be able to re-acquire after expiration
	count, err = q.requeueExpiredJobs(time.Now().Add(time.Minute + time.Second))
	require.NoError(t, err)
	require.Equal(t, 1, count)

	// Verify job moved back to pending
	pending, inProgress = q.Size()
	require.Equal(t, 1, pending)
	require.Equal(t, 0, inProgress)
}

func TestJobQueue_PriorityHandling(t *testing.T) {
	path, cleanup := tempDB(t)
	defer cleanup()

	q, err := NewDB(path)
	require.NoError(t, err)
	defer q.Close()

	jobs := []Job{
		{ID: "normal", Priority: 0},
		{ID: "high", Priority: 1},
	}
	commit, err := newCommit(CmdAddJobs, jobs)
	if err != nil {
		t.Fatalf("encoding failed: %v", err)
	}
	require.NoError(t, q.ApplyCommand(commit, 1).Error)

	// Verify jobs can be acquired regardless of priority
	for _, job := range jobs {
		acquired, err := q.Acquire(job.ID, "worker1", time.Minute)
		require.NoError(t, err)
		require.True(t, acquired)
	}
}

func TestJobQueue_Size(t *testing.T) {
	path, cleanup := tempDB(t)
	defer cleanup()

	q, err := NewDB(path)
	require.NoError(t, err)
	defer q.Close()

	// Initial state
	pending, inProgress := q.Size()
	require.Equal(t, 0, pending)
	require.Equal(t, 0, inProgress)

	// Add some jobs
	jobs := make([]Job, 10)
	for i := range jobs {
		jobs[i] = Job{ID: string(rune('a' + i)), Data: "test"}
	}

	commit, err := newCommit(CmdAddJobs, jobs)
	if err != nil {
		t.Fatalf("encoding failed: %v", err)
	}
	require.NoError(t, q.ApplyCommand(commit, 1).Error)

	// Verify all jobs pending
	pending, inProgress = q.Size()
	require.Equal(t, 10, pending)
	require.Equal(t, 0, inProgress)

	// Acquire some jobs
	for i := 0; i < 3; i++ {
		_, err := q.Acquire(string(rune('a'+i)), "worker1", time.Minute)
		require.NoError(t, err)
	}

	// Verify counts
	pending, inProgress = q.Size()
	require.Equal(t, 7, pending)
	require.Equal(t, 3, inProgress)
}

func TestJobQueue_ConcurrentAccess(t *testing.T) {
	path, cleanup := tempDB(t)
	defer cleanup()

	q, err := NewDB(path)
	require.NoError(t, err)
	defer q.Close()

	// Add test jobs
	jobs := make([]Job, 100)
	for i := range jobs {
		jobs[i] = Job{ID: string(rune('a' + i)), Data: "test"}
	}

	commit, err := newCommit(CmdAddJobs, jobs)
	if err != nil {
		t.Fatalf("encoding failed: %v", err)
	}
	require.NoError(t, q.ApplyCommand(commit, 1).Error)

	var workers sync.WaitGroup
	var applyMu sync.Mutex
	index := uint64(1)
	processed := make(map[string]int)
	errCh := make(chan error, 5)
	worker := func(id string) {
		defer workers.Done()
		for i := range 20 {
			jobID := string(rune('a' + i))
			acquired, err := q.Acquire(jobID, id, time.Minute)
			if err != nil {
				errCh <- err
				return
			}
			if acquired {
				time.Sleep(10 * time.Millisecond) // Simulate work
				commit, err := newCommit(CmdDeleteJob, []Job{{ID: jobID}})
				if err != nil {
					errCh <- err
					return
				}
				// Concurrent workers finish out of order. Consensus assigns log
				// indexes in application order, independently of the job number.
				applyMu.Lock()
				index++
				result := q.ApplyCommand(commit, index)
				if result.Error == nil {
					processed[jobID]++
				}
				applyMu.Unlock()
				if err := result.Error; err != nil {
					errCh <- err
					return
				}
			}
		}
	}

	for i := range 5 {
		workers.Add(1)
		go worker(string(rune('1' + i)))
	}
	workers.Wait()
	close(errCh)
	for err := range errCh {
		require.NoError(t, err, "all workers must finish before the database closes")
	}

	// Verify all jobs were processed exactly once
	pending, inProgress := q.Size()
	require.Equal(t, 80, pending) // 100 total - 20 processed
	require.Equal(t, 0, inProgress)
	require.Len(t, processed, 20)
	for i := range 20 {
		require.Equal(t, 1, processed[string(rune('a'+i))], "each contested job must be processed exactly once")
	}
	require.Equal(t, uint64(21), q.LastAppliedIndex())
}

func TestDB_HasPendingJob(t *testing.T) {
	db, cleanup := NewTestDB(t)
	defer cleanup()

	job := Job{ID: "pending1", Priority: 1}
	addJobForTest(t, db, job)

	assert.True(t, db.HasPendingJob("pending1"), "should find the pending job")
	assert.False(t, db.HasPendingJob("notfound"), "should not find a non-existent job")
}

func TestDB_HasPendingJobs(t *testing.T) {
	db, cleanup := NewTestDB(t)
	defer cleanup()

	addJobForTest(t, db, Job{ID: "jobA", Priority: 1})
	addJobForTest(t, db, Job{ID: "jobB", Priority: 2})

	assert.True(t, db.HasPendingJobs([]string{"jobA"}), "should find jobA")
	assert.True(t, db.HasPendingJobs([]string{"jobB"}), "should find jobB")
	assert.True(t, db.HasPendingJobs([]string{"jobA", "jobB"}), "should find at least one job")
	assert.False(t, db.HasPendingJobs([]string{"notfound"}), "should not find missing job")
	assert.False(t, db.HasPendingJobs([]string{}), "should return false for empty input")
}

var commitIndex uint64 = 1

func addJobForTest(t *testing.T, db *DB, job Job) {
	t.Helper()
	cmd := Command{
		Type: CmdAddJobs,
		Jobs: []Job{job},
	}
	data, err := encode(cmd)
	require.NoError(t, err)
	result := db.ApplyCommand([][]byte{data}, commitIndex)
	commitIndex++
	require.NoError(t, result.Error)
}
