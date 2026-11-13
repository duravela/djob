package server

import (
	"strconv"
	"time"

	"github.com/duravela/djob/pkg/db"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
)

func (s *topLevelServer) workerDeploymentSessions(v db.WorkerDeployment, at time.Time) jp.WorkerDeploymentSessions {
	out := jp.WorkerDeploymentSessions{ControllerID: strconv.FormatUint(s.nodeID, 10), InstanceID: s.workerInstance, Generation: v.Generation, ObservedAt: at.UTC(), Enabled: s.remoteWorkers != nil}
	if s.remoteWorkers == nil {
		return out
	}
	accepting := s.remoteWorkers.acceptingWorkers()
	for _, session := range s.remoteWorkers.snapshot() {
		if session.tenant != v.TenantID {
			continue
		}
		info, visible := session.observation(at, accepting)
		if !visible || info.DeploymentID != v.ID || info.WorkerID != v.WorkerID {
			continue
		}
		if info.DeploymentGeneration != v.Generation {
			out.OtherGenerations++
			continue
		}
		switch info.State {
		case "connected":
			out.Connected++
		case "registered":
			out.Registered++
		case "draining":
			out.Draining++
		case "relocating":
			out.Relocating++
		case "stale":
			out.Stale++
		default:
			out.Unavailable++
		}
	}
	return out
}
