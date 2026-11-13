package store

import "github.com/duravela/djob/pkg/db"

func (s *Store) GetWorkerDeployment(tenant, id string) (*db.WorkerDeployment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.GetWorkerDeployment(tenant, id)
}
func (s *Store) ListWorkerDeployments(tenant, after string, limit int) ([]db.WorkerDeployment, string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.ListWorkerDeployments(tenant, after, limit)
}
func (s *Store) ListWorkerDeploymentRecords(after []byte, limit int) ([]db.WorkerDeployment, []byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.ListWorkerDeploymentRecords(after, limit)
}
func (s *Store) CheckWorkerDeploymentAdmission(tenant string, guard db.WorkerAdmission) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.db.CheckWorkerDeploymentAdmission(tenant, guard)
}
