package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/duravela/djob/pkg/db"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	wire "github.com/duravela/djob/sdk/go/worker/protocol/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type workerDeploymentBackend interface {
	GetWorkerDeployment(string, string) (*db.WorkerDeployment, error)
	ListWorkerDeployments(string, string, int) ([]db.WorkerDeployment, string, error)
}

func deploymentSpec(s jp.WorkerDeploymentSpec) db.WorkerDeploymentSpec {
	return db.WorkerDeploymentSpec{Pool: s.Pool, Image: s.Image, ConfigSecret: s.ConfigSecret, PullSecret: s.PullSecret, Args: append([]string(nil), s.Args...), Replicas: s.Replicas, CPUMillis: s.CPUMillis, MemoryMiB: s.MemoryMiB, DrainSeconds: s.DrainSeconds}
}
func publicWorkerDeployment(v *db.WorkerDeployment) jp.WorkerDeployment {
	s := v.Spec
	return jp.WorkerDeployment{Published: v.Published, OutstandingRuns: v.OutstandingRuns, TenantID: v.TenantID, ID: v.ID, WorkerID: v.WorkerID, Revision: v.Revision, Generation: v.Generation, State: v.State, ChangedAt: v.ChangedAt, Actor: v.Actor, Reason: v.Reason, Spec: jp.WorkerDeploymentSpec{Pool: s.Pool, Image: s.Image, ConfigSecret: s.ConfigSecret, PullSecret: s.PullSecret, Args: append([]string(nil), s.Args...), Replicas: s.Replicas, CPUMillis: s.CPUMillis, MemoryMiB: s.MemoryMiB, DrainSeconds: s.DrainSeconds}}
}
func (s *jobsServer) workerDeploymentsHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Vary", "Authorization")
	p, err := s.authenticate(r.Context())
	if err != nil {
		writeAPIError(w, err)
		return
	}
	backend, ok := s.backend.(workerDeploymentBackend)
	if !ok {
		writeAPIError(w, status.Error(codes.Unavailable, "worker deployment storage unavailable"))
		return
	}
	decode := func(value any) bool {
		d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32768))
		d.DisallowUnknownFields()
		if len(r.URL.RawQuery) != 0 || d.Decode(value) != nil || d.Decode(new(any)) != io.EOF {
			writeAPIError(w, status.Error(codes.InvalidArgument, "invalid worker deployment request"))
			return false
		}
		return true
	}
	switch r.PathValue("method") {
	case "get", "list", "status":
		var q jp.WorkerDeploymentQuery
		if !decode(&q) {
			return
		}
		tenant, err := jobScope(p, q.TenantID, APIReader, false, false, false, false)
		if err != nil {
			writeAPIError(w, err)
			return
		}
		if r.PathValue("method") == "get" || r.PathValue("method") == "status" {
			if !wire.ValidWorkerID(q.ID) || q.PageToken != "" {
				writeAPIError(w, status.Error(codes.InvalidArgument, "select one worker deployment"))
				return
			}
			v, err := backend.GetWorkerDeployment(tenant, q.ID)
			if err != nil {
				writeAPIError(w, apiError(err))
				return
			}
			if r.PathValue("method") == "status" {
				response := map[string]any{"tenant_id": tenant, "id": q.ID, "revision": fmt.Sprint(v.Revision), "configured": s.deployments != nil, "observed": false, "worker_access": s.workerDeploymentAccess(*v)}
				if s.deploymentSessions != nil {
					response["worker_sessions"] = s.deploymentSessions(*v, time.Now())
				}
				if s.deployments != nil {
					if observation, ok := s.deployments.Observation(tenant, q.ID); ok {
						response["observed"], response["runtime"] = true, observation
					}
				}
				workerJSON(w, 200, response)
				return
			}
			workerJSON(w, 200, publicWorkerDeployment(v))
			return
		}
		if q.ID != "" || q.PageToken != "" && !wire.ValidWorkerID(q.PageToken) {
			writeAPIError(w, status.Error(codes.InvalidArgument, "invalid worker deployment page"))
			return
		}
		values, next, err := backend.ListWorkerDeployments(tenant, q.PageToken, 32)
		if err != nil {
			writeAPIError(w, apiError(err))
			return
		}
		page := jp.WorkerDeploymentPage{TenantID: tenant, NextPageToken: next, Deployments: []jp.WorkerDeployment{}}
		for i := range values {
			page.Deployments = append(page.Deployments, publicWorkerDeployment(&values[i]))
		}
		workerJSON(w, 200, page)
	case "mutate":
		var q jp.WorkerDeploymentMutation
		if !decode(&q) {
			return
		}
		tenant, err := jobScope(p, q.TenantID, APIOperator, true, false, false, false)
		if err != nil {
			writeAPIError(w, err)
			return
		}
		if q.Command == nil || apiID(q.Command.RequestID) != nil || len(q.Command.RequestID) > 240 || q.Command.At.IsZero() {
			writeAPIError(w, status.Error(codes.InvalidArgument, "provide an explicit request ID and command time"))
			return
		}
		c := db.StateCommand{RequestID: "worker-deploy/" + jobRequestID(tenant, q.Command.RequestID), At: q.Command.At.UTC(), WorkerDeployment: &db.WorkerDeploymentCommand{TenantID: tenant, ID: q.ID, WorkerID: q.WorkerID, Action: q.Action, ProtectOutstanding: true, Actor: p.subject, Reason: q.Reason, ExpectedRevision: q.ExpectedRevision, Replicas: q.Replicas}}
		if q.Spec != nil {
			spec := deploymentSpec(*q.Spec)
			c.WorkerDeployment.Spec = &spec
		}
		if err := c.Validate(); err != nil {
			writeAPIError(w, status.Error(codes.InvalidArgument, err.Error()))
			return
		}
		if s.validateTime != nil {
			err = s.validateTime(c.At)
		} else {
			err = validatePublicCommandTime(c.At, 5*time.Second)
		}
		if err != nil {
			writeAPIError(w, err)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()
		result, err := s.backend.ProposeStateCommand(ctx, c)
		// A committed conflict proves the permanent deployment receipt already
		// exists. This legacy-form lookup can only recover that receipt, never
		// authorize a new unprotected lifecycle mutation.
		if result.RequestID == c.RequestID && result.Rejection != nil && result.Rejection.Code == "request_conflict" {
			legacy := c
			payload := *c.WorkerDeployment
			payload.ProtectOutstanding = false
			legacy.WorkerDeployment = &payload
			result, err = s.backend.ProposeStateCommand(ctx, legacy)
			c = legacy
		}
		if err == nil {
			err = result.Err()
		}
		if err != nil {
			writeAPIError(w, apiError(err))
			return
		}
		fingerprint, err := c.Fingerprint()
		if err != nil || result.RequestID != c.RequestID || result.Fingerprint != fingerprint || result.WorkerDeployment == nil || result.WorkerDeployment.TenantID != tenant || result.WorkerDeployment.ID != q.ID || result.WorkerDeployment.WorkerID != q.WorkerID {
			writeAPIError(w, status.Error(codes.Internal, "worker deployment receipt does not match request"))
			return
		}
		workerJSON(w, 200, jp.WorkerDeploymentAcknowledgement{RequestID: q.Command.RequestID, Deployment: publicWorkerDeployment(result.WorkerDeployment)})
	default:
		writeAPIError(w, status.Error(codes.NotFound, "unknown worker deployment operation"))
	}
}
