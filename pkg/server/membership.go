package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/duravela/djob/pkg/node"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *jobsServer) membershipHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Vary", "Authorization")
	p, err := s.authenticate(r.Context())
	if err != nil {
		writeAPIError(w, err)
		return
	}
	if p.role != APIOperator || p.tenantID != "" {
		writeAPIError(w, status.Error(codes.PermissionDenied, "cluster membership requires a platform operator"))
		return
	}
	if s.membership == nil || !s.membership.Membership().Enabled {
		writeAPIError(w, status.Error(codes.Unimplemented, "dynamic membership is disabled"))
		return
	}
	if r.URL.RawQuery != "" {
		writeAPIError(w, status.Error(codes.InvalidArgument, "membership queries do not accept parameters"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if _, err = s.membership.ReadBarrier(ctx); err != nil {
		writeAPIError(w, status.Error(codes.Unavailable, "membership quorum unavailable"))
		return
	}
	if r.Method == http.MethodGet {
		workerJSON(w, 200, s.membership.Membership())
		return
	}
	if s.membershipGuard != nil {
		if err = s.membershipGuard(); err != nil {
			writeAPIError(w, err)
			return
		}
	}
	var q node.MembershipChange
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8192))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&q) != nil || decoder.Decode(new(any)) != io.EOF {
		writeAPIError(w, status.Error(codes.InvalidArgument, "provide a reviewed membership change"))
		return
	}
	q.Actor = p.subject
	if s.validateTime != nil {
		if err = s.validateTime(time.Now().UTC()); err != nil {
			writeAPIError(w, err)
			return
		}
	}
	if err = s.membership.ProposeMembership(ctx, q); err != nil {
		writeAPIError(w, status.Error(codes.Aborted, err.Error()))
		return
	}
	// Proposal acceptance is not a durable acknowledgement. A client must read
	// membership again and verify its desired state before taking the next step.
	workerJSON(w, http.StatusAccepted, map[string]any{"submitted": true, "member_id": q.MemberID, "action": q.Action})
}
