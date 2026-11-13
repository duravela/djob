package server

import (
	"time"

	"github.com/duravela/djob/pkg/db"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
)

func (s *jobsServer) workerDeploymentAccess(v db.WorkerDeployment) jp.WorkerDeploymentAccess {
	out := jp.WorkerDeploymentAccess{WorkerID: v.WorkerID, ObservedAt: time.Now().UTC()}
	backend, ok := s.backend.(interface {
		GetWorkerEnrollment(string, string) (*db.WorkerEnrollment, error)
	})
	if !ok {
		return out
	}
	enrollment, err := backend.GetWorkerEnrollment(v.TenantID, v.WorkerID)
	if err != nil || enrollment == nil || enrollment.TenantID != v.TenantID || enrollment.WorkerID != v.WorkerID || enrollment.Revision == 0 {
		return out
	}
	switch enrollment.State {
	case "enabled", "draining", "revoked":
		out.Observed, out.Revision, out.State = true, enrollment.Revision, enrollment.State
	}
	return out
}
