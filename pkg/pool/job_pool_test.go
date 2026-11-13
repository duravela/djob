package pool

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type fakeDB struct {
	mu         sync.Mutex
	acquired   map[string]bool
	completed  map[string]bool
	acquireErr error
}

func newFakeDB() *fakeDB {
	return &fakeDB{
		acquired:  make(map[string]bool),
		completed: make(map[string]bool),
	}
}

func (db *fakeDB) Acquire(jobID, workerID string, leaseDur time.Duration) (bool, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.acquireErr != nil {
		return false, db.acquireErr
	}
	if db.acquired[jobID] {
		return false, nil // already acquired
	}
	db.acquired[jobID] = true
	return true, nil
}

func (db *fakeDB) Release(jobID, workerID string) error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if !db.acquired[jobID] {
		return nil
	}
	db.completed[jobID] = true
	delete(db.acquired, jobID)
	return nil
}

func TestWorkerPoolProcessesJobsAndStoresReports(t *testing.T) {
	db := newFakeDB()
	jobs := []string{"job1", "job2"}
	reportC := make(chan *JobResult)
	wp, err := NewWorkerPool(2, db, "worker1", 10*time.Second, reportC)
	if err != nil {
		t.Fatalf("failed to create worker pool: %v", err)
	}
	defer wp.Close()

	for _, j := range jobs {
		_ = wp.Submit(j)
	}

	deadline := time.Now().Add(5 * time.Second)
	var done = 0
	for done < 2 && time.Now().Before(deadline) {
		select {
		case <-reportC:
			done++
		case <-time.After(5 * time.Second):
			panic("report not ready")
		}
	}

	for _, jobID := range jobs {
		if !db.completed[jobID] {
			t.Errorf("job %s was not completed", jobID)
		}
	}
}

func TestWorkerPoolHandlesAcquireError(t *testing.T) {
	reportC := make(chan *JobResult)
	db := newFakeDB()
	db.acquireErr = fmt.Errorf("acquire error")

	wp, err := NewWorkerPool(1, db, "worker1", 10*time.Second, reportC)
	if err != nil {
		t.Fatalf("failed to create worker pool: %v", err)
	}
	defer wp.Close()

	require.Equal(t, JobPoolStats.Get("failed_jobs").String(), "0")

	_ = wp.Submit("jobX")
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, JobPoolStats.Get("failed_jobs").String(), "1")
}
