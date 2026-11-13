package console

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/duravela/djob/pkg/db"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"github.com/duravela/djob/sdk/go/worker"
)

func platformSession(sess *session) bool {
	return sess.identity.Role == "operator" && sess.identity.TenantID == ""
}

// The worker SDK accepts eight distinct origins. Console discovery can cover
// more controllers and aliases, so bound and normalize its bootstrap list.
func (s *Server) workerOrigins() []string {
	seen := map[string]bool{}
	origins := []string{}
	for _, controller := range s.controllers {
		u := *controller
		u.Host = strings.ToLower(u.Host)
		u.ForceQuery = false
		origin := u.String()
		if seen[origin] {
			continue
		}
		seen[origin] = true
		origins = append(origins, origin)
		if len(origins) == 8 {
			break
		}
	}
	return origins
}

func (s *Server) enrollmentQuery(w http.ResponseWriter, r *http.Request, sess *session) {
	method := r.PathValue("method")
	if method != "get" && method != "list" {
		problem(w, 404, "unknown enrollment query")
		return
	}
	var q jp.WorkerEnrollmentQuery
	if err := decode(w, r, 4096, &q); err != nil {
		problem(w, 400, "invalid enrollment query")
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
	sess.mu.Lock()
	index := sess.controller
	sess.mu.Unlock()
	response, index, err := s.request(r.Context(), sess.credential(), index, "worker-enrollments/"+method, http.MethodPost, data, true)
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

func (s *Server) prepareEnrollment(w http.ResponseWriter, r *http.Request, sess *session) {
	if !platformSession(sess) {
		problem(w, 403, "worker enrollment requires a platform operator")
		return
	}
	var q jp.WorkerEnrollmentMutation
	if err := decode(w, r, 4096, &q); err != nil || q.Command != nil || q.CredentialHash != "" {
		problem(w, 400, "provide an enrollment action without a command or credential hash")
		return
	}
	id, err := randomID()
	if err != nil {
		problem(w, 500, "cannot prepare enrollment identity")
		return
	}
	q.Command = &jp.WorkerManagementCommand{RequestID: "console/" + id, At: time.Now().UTC()}
	c := db.WorkerEnrollmentCommand{TenantID: q.TenantID, WorkerID: q.WorkerID, Action: q.Action, Subject: q.Subject, Actor: "console/prepare", Reason: q.Reason, ExpectedRevision: q.ExpectedRevision}
	var token string
	if q.Action == "enroll" || q.Action == "rotate" {
		secret, err := randomID()
		if err != nil {
			problem(w, 500, "cannot allocate worker credential")
			return
		}
		token = "djobw1." + secret
		c.CredentialHash = sha256.Sum256([]byte(token))
		q.CredentialHash = hex.EncodeToString(c.CredentialHash[:])
	}
	command := db.StateCommand{RequestID: q.Command.RequestID, At: q.Command.At, WorkerEnrollment: &c}
	if err := command.Validate(); err != nil {
		problem(w, 400, err.Error())
		return
	}
	var config json.RawMessage
	if token != "" {
		origins := s.workerOrigins()
		config, _ = json.Marshal(map[string]any{"transport": worker.TransportGRPC, "server_url": origins[0], "fallback_urls": origins[1:], "tenant_id": q.TenantID, "worker_id": q.WorkerID, "token": token})
	}
	data, err := json.Marshal(q)
	if err != nil {
		problem(w, 400, "invalid worker enrollment request")
		return
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if sess.pending != nil {
		problem(w, 409, "review the current mutation before preparing another")
		return
	}
	clearDraftLocked(sess)
	sess.pending = &pendingMutation{RequestID: q.Command.RequestID, Request: data, State: "prepared", Endpoint: "worker-enrollments/mutate", WorkerConfig: config, WorkerConfigAvailable: len(config) != 0}
	sess.pending.DraftRevision = sess.draftRevision
	writeJSON(w, 200, sess.pending)
}

func (s *Server) downloadWorkerConfig(w http.ResponseWriter, r *http.Request, sess *session) {
	var q struct {
		RequestID string `json:"request_id"`
	}
	if err := decode(w, r, 1024, &q); err != nil {
		problem(w, 400, "provide the enrollment request ID")
		return
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	p := sess.pending
	if p == nil || p.RequestID != q.RequestID || p.State != "confirmed" || len(p.WorkerConfig) == 0 {
		problem(w, 409, "confirm this enrollment before downloading its private configuration")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(p.WorkerConfig)
}
