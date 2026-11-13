package pool

import (
	"container/heap"
	"sync"
	"time"
)

type entry struct {
	report *JobResult
	index  int
}

func (e *entry) key() string {
	return e.report.JobID
}

func (e *entry) finishedAt() time.Time {
	return e.report.FinishedAt
}

type Heap []*entry

func (h Heap) Len() int           { return len(h) }
func (h Heap) Less(i, j int) bool { return h[i].finishedAt().Before(h[j].finishedAt()) }
func (h Heap) Swap(i, j int)      { h[i], h[j] = h[j], h[i]; h[i].index = i; h[j].index = j }
func (h *Heap) Push(x any)        { *h = append(*h, &entry{report: x.(*JobResult)}) }
func (h *Heap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}

type JobReportStore struct {
	mu         sync.Mutex
	reports    map[string]*JobResult
	heap       Heap
	ttl        time.Duration
	maxReports int
}

func NewJobReportStore(ttl time.Duration, maxReports int) *JobReportStore {
	jrs := &JobReportStore{
		reports:    make(map[string]*JobResult),
		heap:       make(Heap, 0),
		ttl:        ttl,
		maxReports: maxReports,
	}
	return jrs
}

func (s *JobReportStore) Add(report *JobResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reports[report.JobID] = report
	heap.Push(&s.heap, report)
	s.cleanup()
}

func (s *JobReportStore) Get(jobID string) (*JobResult, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.reports[jobID]
	return r, ok
}

func (s *JobReportStore) GetAll() []*JobResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	reports := make([]*JobResult, 0, len(s.reports))
	for _, r := range s.reports {
		reports = append(reports, r)
	}
	return reports
}

func (s *JobReportStore) Delete(jobID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.reports[jobID]; ok {
		delete(s.reports, jobID)
		r.FinishedAt = time.Time{} // Mark as expired
	}
	s.cleanup()
}

func (s *JobReportStore) cleanup() {
	cutoff := time.Now().Add(-s.ttl)
	for s.heap.Len() > 0 {
		oldest := s.heap[0]
		if (s.ttl > 0 && oldest.finishedAt().Before(cutoff)) || (s.maxReports > 0 && s.heap.Len() > s.maxReports) {
			heap.Pop(&s.heap)
			delete(s.reports, oldest.key())
		} else {
			break
		}
	}
}

func (s *JobReportStore) StartGC() {
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			s.mu.Lock()
			s.cleanup()
			s.mu.Unlock()
		}
	}()
}
