package console

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/duravela/djob/pkg/db"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
)

func (s *Server) deploymentQuery(w http.ResponseWriter, r *http.Request, sess *session) {
	method := r.PathValue("method")
	if method != "get" && method != "list" && method != "status" {
		problem(w, 404, "unknown worker deployment query")
		return
	}
	var q jp.WorkerDeploymentQuery
	if err := decode(w, r, 4096, &q); err != nil {
		problem(w, 400, "invalid worker deployment query")
		return
	}
	if sess.identity.TenantID != "" {
		if q.TenantID != "" && q.TenantID != sess.identity.TenantID {
			problem(w, 403, "credential does not permit this tenant")
			return
		}
		q.TenantID = sess.identity.TenantID
	}
	data, _ := json.Marshal(q)
	if method == "status" {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		results := make([]map[string]any, len(s.controllers))
		var wg sync.WaitGroup
		for i := range s.controllers {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				out := map[string]any{"index": index, "origin": s.controllers[index].String()}
				results[index] = out
				select {
				case s.discovery <- struct{}{}:
					defer func() { <-s.discovery }()
				case <-ctx.Done():
					out["error"] = "Observation deadline exceeded"
					return
				}
				response, _, err := s.request(ctx, sess.credential(), index, "worker-deployments/status", http.MethodPost, data, false)
				if err != nil || response.status != 200 {
					out["error"] = "Controller observation unavailable"
					return
				}
				var value struct {
					TenantID string `json:"tenant_id"`
					ID       string `json:"id"`
				}
				if len(response.body) > 32768 || json.Unmarshal(response.body, &value) != nil || value.TenantID != q.TenantID || value.ID != q.ID {
					out["error"] = "Invalid controller observation"
					return
				}
				out["status"] = json.RawMessage(response.body)
			}(i)
		}
		wg.Wait()
		writeJSON(w, 200, map[string]any{"tenant_id": q.TenantID, "id": q.ID, "controllers": results})
		return
	}
	sess.mu.Lock()
	index := sess.controller
	sess.mu.Unlock()
	response, index, err := s.request(r.Context(), sess.credential(), index, "worker-deployments/"+method, http.MethodPost, data, true)
	if err != nil {
		problem(w, 502, err.Error())
		return
	}
	sess.mu.Lock()
	sess.controller = index
	sess.mu.Unlock()
	w.Header().Set("X-Djob-Controller", s.controllers[index].String())
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(response.status)
	_, _ = w.Write(response.body)
}

func (s *Server) prepareDeployment(w http.ResponseWriter, r *http.Request, sess *session) {
	if !platformSession(sess) {
		problem(w, 403, "worker deployment changes require a platform operator")
		return
	}
	var q jp.WorkerDeploymentMutation
	if err := decode(w, r, 32768, &q); err != nil || q.Command != nil {
		problem(w, 400, "provide a deployment action without a command envelope")
		return
	}
	c := db.WorkerDeploymentCommand{TenantID: q.TenantID, ID: q.ID, WorkerID: q.WorkerID, Action: q.Action, Actor: "console/prepare", Reason: q.Reason, ExpectedRevision: q.ExpectedRevision, Replicas: q.Replicas}
	if q.Spec != nil {
		v := q.Spec
		c.Spec = &db.WorkerDeploymentSpec{Pool: v.Pool, Image: v.Image, ConfigSecret: v.ConfigSecret, PullSecret: v.PullSecret, Args: append([]string(nil), v.Args...), Replicas: v.Replicas, CPUMillis: v.CPUMillis, MemoryMiB: v.MemoryMiB, DrainSeconds: v.DrainSeconds}
	}
	id, err := randomID()
	if err != nil {
		problem(w, 500, "cannot allocate deployment request identity")
		return
	}
	q.Command = &jp.WorkerManagementCommand{RequestID: "console/" + id, At: time.Now().UTC()}
	command := db.StateCommand{RequestID: q.Command.RequestID, At: q.Command.At, WorkerDeployment: &c}
	if err := command.Validate(); err != nil {
		problem(w, 400, err.Error())
		return
	}
	data, err := json.Marshal(q)
	if err != nil {
		problem(w, 400, "invalid deployment request")
		return
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	pending := &pendingMutation{RequestID: q.Command.RequestID, Request: data, State: "prepared", Endpoint: "worker-deployments/mutate"}
	if !retainPreparedLocked(r.Context(), sess, pending) {
		problem(w, 409, "review changed; finish the current mutation before preparing another")
		return
	}
	writeJSON(w, 200, sess.pending)
}
