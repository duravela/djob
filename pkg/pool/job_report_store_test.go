package pool

import (
	"fmt"
	"testing"
	"time"
)

func newReport(id string, finishedAt time.Time) *JobResult {
	return &JobResult{
		JobID:      id,
		Group:      "group1",
		NodeID:     "worker1",
		Status:     "success",
		StartedAt:  finishedAt.Add(-time.Second),
		FinishedAt: finishedAt,
		Output:     []byte("ok"),
	}
}

func TestAddAndGet(t *testing.T) {
	reportsC := make(chan *JobResult)
	defer close(reportsC)
	store := NewJobReportStore(time.Minute, 10)
	r := newReport("job1", time.Now())
	store.Add(r)

	got, ok := store.Get("job1")
	if !ok || got.JobID != "job1" {
		t.Fatalf("expected to get job1, got %v", got)
	}
}

func TestDelete(t *testing.T) {
	reportsC := make(chan *JobResult)
	defer close(reportsC)
	store := NewJobReportStore(time.Minute, 10)
	r := newReport("job2", time.Now())
	store.Add(r)
	store.Delete("job2")

	_, ok := store.Get("job2")
	if ok {
		t.Fatal("expected job2 to be deleted")
	}
}

func TestTTLExpiry(t *testing.T) {
	reportsC := make(chan *JobResult)
	defer close(reportsC)
	store := NewJobReportStore(10*time.Millisecond, 10)
	r := newReport("job3", time.Now().Add(-time.Second))
	store.Add(r)
	time.Sleep(20 * time.Millisecond)
	store.cleanup()

	_, ok := store.Get("job3")
	if ok {
		t.Fatal("expected job3 to expire by TTL")
	}
}

func TestMaxReports(t *testing.T) {
	reportsC := make(chan *JobResult)
	defer close(reportsC)
	store := NewJobReportStore(time.Minute, 3)
	now := time.Now()
	for i := range 5 {
		store.Add(newReport(
			"job"+fmt.Sprint('a'+i),
			now.Add(time.Duration(i)*time.Millisecond),
		))
	}
	store.cleanup()

	if len(store.reports) != 3 {
		t.Fatalf("expected 3 reports, got %d", len(store.reports))
	}
}
