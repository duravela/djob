package server

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/duravela/djob/pkg/db"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	wire "github.com/duravela/djob/sdk/go/worker/protocol/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type workerEnrollmentBackend interface {
	GetWorkerEnrollment(string, string) (*db.WorkerEnrollment, error)
	ListWorkerEnrollments(string, string, int) ([]db.WorkerEnrollment, string, error)
	LookupWorkerCredential([32]byte) (*db.WorkerEnrollment, error)
}

func publicWorkerEnrollment(v *db.WorkerEnrollment) jp.WorkerEnrollment {
	return jp.WorkerEnrollment{TenantID: v.TenantID, WorkerID: v.WorkerID, Subject: v.Subject, Revision: v.Revision, State: v.State, ChangedAt: v.ChangedAt, Actor: v.Actor, Reason: v.Reason}
}

func (s *jobsServer) workerEnrollmentsHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Vary", "Authorization")
	p, err := s.authenticate(r.Context())
	if err != nil {
		writeAPIError(w, err)
		return
	}
	backend, ok := s.backend.(workerEnrollmentBackend)
	if !ok {
		writeAPIError(w, status.Error(codes.Unavailable, "worker enrollment storage unavailable"))
		return
	}
	decode := func(value any) bool {
		d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8192))
		d.DisallowUnknownFields()
		if len(r.URL.RawQuery) != 0 || d.Decode(value) != nil || d.Decode(new(any)) != io.EOF {
			writeAPIError(w, status.Error(codes.InvalidArgument, "invalid worker enrollment request"))
			return false
		}
		return true
	}
	switch r.PathValue("method") {
	case "get", "list":
		var q jp.WorkerEnrollmentQuery
		if !decode(&q) {
			return
		}
		tenant, err := jobScope(p, q.TenantID, APIReader, false, false, false, false)
		if err != nil {
			writeAPIError(w, err)
			return
		}
		if r.PathValue("method") == "get" {
			if !wire.ValidWorkerID(q.WorkerID) || q.PageToken != "" {
				writeAPIError(w, status.Error(codes.InvalidArgument, "select one worker enrollment"))
				return
			}
			v, err := backend.GetWorkerEnrollment(tenant, q.WorkerID)
			if err != nil {
				writeAPIError(w, apiError(err))
				return
			}
			workerJSON(w, 200, publicWorkerEnrollment(v))
			return
		}
		if q.WorkerID != "" || q.PageToken != "" && !wire.ValidWorkerID(q.PageToken) {
			writeAPIError(w, status.Error(codes.InvalidArgument, "invalid worker enrollment page"))
			return
		}
		values, next, err := backend.ListWorkerEnrollments(tenant, q.PageToken, 32)
		if err != nil {
			writeAPIError(w, apiError(err))
			return
		}
		page := jp.WorkerEnrollmentPage{TenantID: tenant, NextPageToken: next, Enrollments: []jp.WorkerEnrollment{}}
		for i := range values {
			page.Enrollments = append(page.Enrollments, publicWorkerEnrollment(&values[i]))
		}
		workerJSON(w, 200, page)
	case "mutate":
		var q jp.WorkerEnrollmentMutation
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
		var hash [32]byte
		if q.CredentialHash != "" {
			data, err := hex.DecodeString(q.CredentialHash)
			if err != nil || len(data) != 32 || hex.EncodeToString(data) != q.CredentialHash {
				writeAPIError(w, status.Error(codes.InvalidArgument, "provide a SHA-256 credential hash"))
				return
			}
			copy(hash[:], data)
		}
		c := db.StateCommand{RequestID: "worker-admin/" + jobRequestID(tenant, q.Command.RequestID), At: q.Command.At.UTC(), WorkerEnrollment: &db.WorkerEnrollmentCommand{TenantID: tenant, WorkerID: q.WorkerID, Action: q.Action, Subject: q.Subject, Actor: p.subject, Reason: q.Reason, ExpectedRevision: q.ExpectedRevision, CredentialHash: hash}}
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
		if err == nil {
			err = result.Err()
		}
		if err != nil {
			writeAPIError(w, apiError(err))
			return
		}
		fingerprint, err := c.Fingerprint()
		if err != nil || result.RequestID != c.RequestID || result.Fingerprint != fingerprint || result.WorkerEnrollment == nil || result.WorkerEnrollment.TenantID != tenant || result.WorkerEnrollment.WorkerID != q.WorkerID {
			writeAPIError(w, status.Error(codes.Internal, "worker enrollment receipt does not match request"))
			return
		}
		workerJSON(w, 200, jp.WorkerEnrollmentAcknowledgement{RequestID: q.Command.RequestID, Enrollment: publicWorkerEnrollment(result.WorkerEnrollment)})
	default:
		writeAPIError(w, status.Error(codes.NotFound, "unknown worker enrollment operation"))
	}
}
