package store

import (
	"github.com/duravela/djob/pkg/db"
	"time"
)

// These queries serve trusted controllers and the separately authorized Jobs
// service. The legacy HTTP/gRPC API does not expose generic job records.
func (s *Store) GetJobRun(tenant, id string) (*db.JobRun, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.GetJobRun(tenant, id)
}
func (s *Store) GetJobView(tenant, id string) (db.JobView, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.GetJobView(tenant, id)
}
func (s *Store) GetJobResource(id string) (*db.JobResource, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.GetJobResource(id)
}
func (s *Store) GetJobAction(tenant, run string, sequence uint64) (*db.JobAction, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.GetJobAction(tenant, run, sequence)
}
func (s *Store) ListJobRuns(tenant string, after []byte, limit int) (db.JobRunPage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.ListJobRuns(tenant, after, limit)
}
func (s *Store) ListReadyJobs(tenant string, after []byte, limit int) (db.JobRunPage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.ListReadyJobs(tenant, after, limit)
}
func (s *Store) ListExpiredJobClaims(at time.Time, after []byte, limit int) (db.JobRunPage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.ListExpiredJobClaims(at, after, limit)
}
func (s *Store) ListDueJobTimers(at time.Time, after []byte, limit int) (db.JobRunPage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.ListDueJobTimers(at, after, limit)
}

func (s *Store) GetJobRecord(query db.JobRecordQuery) (db.JobRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.GetJobRecord(query)
}
func (s *Store) ListJobRecords(query db.JobRecordQuery, after []byte, limit int) (db.JobRecordPage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.ListJobRecords(query, after, limit)
}
func (s *Store) ListJobTenants(after []byte, limit int) (db.JobTenantPage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.ListJobTenants(after, limit)
}

func (s *Store) ListDispatchJobs(after []byte, limit int, skipped []string) (db.JobRecordPage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.ListDispatchJobs(after, limit, skipped)
}

func (s *Store) ListDueJobTriggers(at time.Time, after []byte, limit int) (db.JobTriggerPage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.ListDueJobTriggers(at, after, limit)
}
func (s *Store) JobTriggerCapacity(tenant, id string) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.JobTriggerCapacity(tenant, id)
}
func (s *Store) GetJobTrigger(tenant, id string) (*db.JobTrigger, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.GetJobTrigger(tenant, id)
}

func (s *Store) GetJobTriggerRetirement(tenant, id string) (*db.JobTriggerRetirement, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.GetJobTriggerRetirement(tenant, id)
}

func (s *Store) GetJobConfigurationRetirement(tenant string, kind db.JobRecordKind, id string) (*db.JobConfigurationRetirement, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.GetJobConfigurationRetirement(tenant, kind, id)
}

func (s *Store) GetJobActivation(tenant, trigger, source, event string) (*db.JobActivation, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.GetJobActivation(tenant, trigger, source, event)
}

func (s *Store) GetJobLimits(tenant string) (*db.JobLimits, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.GetJobLimits(tenant)
}
func (s *Store) GetJobUsage(tenant string) (*db.JobUsage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.GetJobUsage(tenant)
}

func (s *Store) GetJobDeletion(tenant, run string) (*db.JobDeletion, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.GetJobDeletion(tenant, run)
}
func (s *Store) GetJobReceiptCompaction(tenant, run string) (*db.JobReceiptCompaction, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.GetJobReceiptCompaction(tenant, run)
}
func (s *Store) ListPendingJobReceiptCompactions(after []byte, limit int) (db.JobReceiptCompactionPage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.ListPendingJobReceiptCompactions(after, limit)
}
func (s *Store) GetJobRetention(tenant string) (*db.JobRetentionPolicy, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.GetJobRetention(tenant)
}
func (s *Store) ListPendingJobPurges(after []byte, limit int) (db.JobDeletionPage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.ListPendingJobPurges(after, limit)
}
func (s *Store) ListJobRetentionCandidates(at time.Time, after []byte, limit int) (db.JobPurgePage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.ListJobRetentionCandidates(at, after, limit)
}

func (s *Store) GetJobConfigurationLimits(tenant string) (*db.JobConfigurationLimits, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.GetJobConfigurationLimits(tenant)
}
func (s *Store) GetJobConfigurationUsage(tenant string) (*db.JobConfigurationUsage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.GetJobConfigurationUsage(tenant)
}

func (s *Store) GetJobConfigurationReceiptCompaction(tenant, id string) (*db.JobConfigurationReceiptCompaction, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.GetJobConfigurationReceiptCompaction(tenant, id)
}
func (s *Store) ListPendingJobConfigurationReceiptCompactions(after []byte, limit int) (db.JobConfigurationReceiptCompactionPage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.ListPendingJobConfigurationReceiptCompactions(after, limit)
}
