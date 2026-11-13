package store

import "github.com/duravela/djob/pkg/db"

func (s *Store) GetWorkerEnrollment(tenant, id string) (*db.WorkerEnrollment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.GetWorkerEnrollment(tenant, id)
}
func (s *Store) LookupWorkerCredential(hash [32]byte) (*db.WorkerEnrollment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.LookupWorkerCredential(hash)
}
func (s *Store) ListWorkerEnrollments(tenant, after string, limit int) ([]db.WorkerEnrollment, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.ListWorkerEnrollments(tenant, after, limit)
}
