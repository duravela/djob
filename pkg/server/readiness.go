package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/duravela/djob/pkg/node"
)

const (
	DefaultReadinessTimeout = 2 * time.Second
	MaxReadinessTimeout     = 5 * time.Second
)

type readinessResponse struct {
	NodeID         uint64 `json:"node_id"`
	Ready          bool   `json:"ready"`
	Leader         bool   `json:"leader"`
	ConfirmedIndex uint64 `json:"confirmed_index,omitempty,string"`
	AppliedIndex   uint64 `json:"applied_index,omitempty,string"`
	Reason         string `json:"reason,omitempty"`
}

func (s *topLevelServer) readiness(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	reply := readinessResponse{NodeID: s.nodeID}
	write := func(status int, reason string) {
		if s.metrics != nil {
			s.metrics.observeReadiness(reason, time.Since(started))
		}
		reply.Reason = reason
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(reply)
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		write(http.StatusMethodNotAllowed, "invalid_method")
		return
	}
	if r.URL.RawQuery != "" || r.URL.ForceQuery {
		write(http.StatusBadRequest, "invalid_request")
		return
	}
	if s.application == nil {
		write(http.StatusServiceUnavailable, "starting")
		return
	}
	reply.Leader = s.application.Raft.IsLeader()
	if s.startupContext.Err() != nil {
		write(http.StatusServiceUnavailable, "stopping")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.ReadinessTimeout)
	defer cancel()
	stop := context.AfterFunc(s.startupContext, cancel)
	defer stop()
	result, err := s.application.Raft.ReadBarrier(ctx)
	reply.Leader = s.application.Raft.IsLeader()
	if s.startupContext.Err() != nil {
		write(http.StatusServiceUnavailable, "stopping")
		return
	}
	if err != nil {
		reason := "quorum_unavailable"
		if errors.Is(err, node.ErrReadBarrierBusy) {
			reason = "busy"
		}
		write(http.StatusServiceUnavailable, reason)
		return
	}
	reply.ConfirmedIndex, reply.AppliedIndex = result.ConfirmedIndex, result.AppliedIndex
	if reason := s.admission.reason(); reason != "" {
		write(http.StatusServiceUnavailable, reason)
		return
	}
	reply.Ready = true
	write(http.StatusOK, "")
}
