package console

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
)

type workerSource struct {
	Index      int                 `json:"index"`
	Origin     string              `json:"origin"`
	Inventory  *jp.WorkerInventory `json:"inventory,omitempty"`
	Error      string              `json:"error,omitempty"`
	HTTPStatus int                 `json:"http_status,omitempty"`
}

type workerObservations struct {
	TenantID    string         `json:"tenant_id"`
	ObservedAt  time.Time      `json:"observed_at"`
	Controllers []workerSource `json:"controllers"`
}

// Inventory stays pinned to its source controller: worker sessions are local,
// and a pagination token must never fail over to a different process.
func (s *Server) workers(w http.ResponseWriter, r *http.Request, sess *session) {
	var q struct {
		TenantID        string `json:"tenant_id"`
		ControllerIndex *int   `json:"controller_index"`
		PageToken       string `json:"page_token"`
	}
	if err := decode(w, r, 8192, &q); err != nil {
		problem(w, 400, err.Error())
		return
	}
	if sess.identity.TenantID != "" {
		if q.TenantID != "" && q.TenantID != sess.identity.TenantID {
			problem(w, 403, "credential does not permit this tenant")
			return
		}
		q.TenantID = sess.identity.TenantID
	}
	if !boundedName(q.TenantID, 256) || len(q.PageToken) > 4096 || q.PageToken != "" && q.ControllerIndex == nil {
		problem(w, 400, "select a tenant; page tokens require their configured controller index")
		return
	}
	indices := make([]int, len(s.controllers))
	for i := range indices {
		indices[i] = i
	}
	if q.ControllerIndex != nil {
		if *q.ControllerIndex < 0 || *q.ControllerIndex >= len(s.controllers) {
			problem(w, 400, "unknown configured controller index")
			return
		}
		indices = []int{*q.ControllerIndex}
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	result := workerObservations{TenantID: q.TenantID, ObservedAt: time.Now().UTC(), Controllers: make([]workerSource, len(indices))}
	var wg sync.WaitGroup
	for slot, index := range indices {
		wg.Add(1)
		go func() {
			defer wg.Done()
			source := workerSource{Index: index, Origin: s.controllers[index].String()}
			defer func() { result.Controllers[slot] = source }()
			select {
			case s.discovery <- struct{}{}:
				defer func() { <-s.discovery }()
			case <-ctx.Done():
				source.Error = "Observation deadline exceeded; refresh to try again."
				return
			}
			probe, done := context.WithTimeout(ctx, 2*time.Second)
			defer done()
			params := url.Values{"tenant_id": {q.TenantID}, "page_size": {"16"}}
			if q.PageToken != "" {
				params.Set("page_token", q.PageToken)
			}
			response, _, err := s.request(probe, sess.credential(), index, "workers", http.MethodGet, nil, false, params)
			if err != nil {
				source.Error = "Controller observation unavailable; refresh to try again."
				return
			}
			source.HTTPStatus = response.status
			if response.status != http.StatusOK {
				source.Error = fmt.Sprintf("Controller returned HTTP %d; refresh after checking access and availability.", response.status)
				return
			}
			var inventory jp.WorkerInventory
			if len(response.body) > 512<<10 || json.Unmarshal(response.body, &inventory) != nil || !validInventory(inventory, q.TenantID) {
				source.Error, source.HTTPStatus = "Controller returned invalid worker metadata.", http.StatusBadGateway
				return
			}
			source.Inventory = &inventory
		}()
	}
	wg.Wait()
	writeJSON(w, http.StatusOK, result)
}

func boundedName(value string, limit int) bool {
	return strings.TrimSpace(value) != "" && len(value) <= limit && utf8.ValidString(value)
}

func validInventory(v jp.WorkerInventory, tenant string) bool {
	id, err := strconv.ParseUint(v.ControllerID, 10, 64)
	if err != nil || id == 0 || !boundedName(v.InstanceID, 128) || v.TenantID != tenant || v.ObservedAt.IsZero() || len(v.Workers) > 16 || len(v.NextPageToken) > 4096 {
		return false
	}
	seen := make(map[string]bool, len(v.Workers))
	for _, worker := range v.Workers {
		if !boundedName(worker.ID, 128) || seen[worker.ID] || !boundedName(worker.WorkerID, 256) || worker.TenantID != tenant || len(worker.Subject) > 256 || worker.LocalTasks < 0 || worker.LocalTasks > 1024 || len(worker.Runtimes) > 8 || len(worker.Handlers) > 64 || len(worker.Drivers) > 64 {
			return false
		}
		seen[worker.ID] = true
		if worker.Mode != "embedded" && worker.Mode != "remote" {
			return false
		}
		switch worker.State {
		case "connected", "registered", "draining", "relocating", "stale", "unavailable", "stopped":
		default:
			return false
		}
		for _, runtime := range worker.Runtimes {
			if !boundedName(runtime, 256) {
				return false
			}
		}
		for _, versions := range [][]jp.WorkerVersion{worker.Handlers, worker.Drivers} {
			for _, version := range versions {
				if !boundedName(version.Name, 256) || !boundedName(version.Version, 256) {
					return false
				}
			}
		}
	}
	return true
}
