package console

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"time"

	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type pendingMutation struct {
	DraftRevision         uint64          `json:"draft_revision,string,omitempty"`
	Endpoint              string          `json:"-"`
	WorkerConfig          json.RawMessage `json:"-"`
	WorkerConfigAvailable bool            `json:"worker_config_available,omitempty"`
	RequestID             string          `json:"request_id"`
	Request               json.RawMessage `json:"request"`
	State                 string          `json:"state"`
	Response              json.RawMessage `json:"response,omitempty"`
	HTTPCode              int             `json:"http_status,omitempty"`
	Error                 string          `json:"error,omitempty"`
}

func (s *Server) query(w http.ResponseWriter, r *http.Request, sess *session) {
	method := r.PathValue("method")
	var query proto.Message
	switch method {
	case "get":
		query = new(jp.GetRequest)
	case "list":
		query = new(jp.ListRequest)
	default:
		problem(w, 404, "unknown query")
		return
	}
	data, err := readBody(w, r, maxRequest)
	if err != nil {
		problem(w, 400, err.Error())
		return
	}
	if err = protojson.Unmarshal(data, query); err != nil {
		problem(w, 400, "invalid job query")
		return
	}
	sess.mu.Lock()
	controller := sess.controller
	sess.mu.Unlock()
	response, index, err := s.request(r.Context(), sess.credential(), controller, method, http.MethodPost, data, true)
	sess.mu.Lock()
	sess.controller = index
	sess.mu.Unlock()
	if err != nil {
		problem(w, 502, err.Error())
		return
	}
	w.Header().Set("X-Djob-Controller", s.controllers[index].String())
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(response.status)
	_, _ = w.Write(response.body)
}

func supportedMutation(q *jp.MutationRequest) bool {
	switch q.Operation.(type) {
	case *jp.MutationRequest_PutTenant, *jp.MutationRequest_PutGroup, *jp.MutationRequest_PutDefinition,
		*jp.MutationRequest_PutBinding, *jp.MutationRequest_PutResource, *jp.MutationRequest_PutGrant,
		*jp.MutationRequest_Launch, *jp.MutationRequest_Cancel, *jp.MutationRequest_PutTrigger,
		*jp.MutationRequest_PauseTrigger, *jp.MutationRequest_PutLimits, *jp.MutationRequest_PutConfigurationLimits,
		*jp.MutationRequest_ResolveRun, *jp.MutationRequest_ResolveResource, *jp.MutationRequest_Purge:
		return true
	default:
		return false
	}
}

func (s *Server) prepare(w http.ResponseWriter, r *http.Request, sess *session) {
	if sess.identity.Role == "reader" {
		problem(w, 403, "reader sessions cannot prepare mutations")
		return
	}
	data, err := readBody(w, r, maxRequest)
	if err != nil {
		problem(w, 400, err.Error())
		return
	}
	var q jp.MutationRequest
	if err := protojson.Unmarshal(data, &q); err != nil || q.Command != nil || !supportedMutation(&q) {
		problem(w, 400, "prepare one supported mutation without a command envelope")
		return
	}
	if (q.GetPutLimits() != nil || q.GetPutConfigurationLimits() != nil) && (sess.identity.Role != "operator" || sess.identity.TenantID != "") {
		problem(w, 403, "changing tenant limits requires a platform operator")
		return
	}
	if code, message := validateRecoveryPreparation(&q, sess.identity); code != 0 {
		problem(w, code, message)
		return
	}
	id, err := randomID()
	if err != nil {
		problem(w, 500, "cannot allocate a mutation identity")
		return
	}
	q.Command = &jp.Command{RequestId: "console/" + id, At: timestamppb.Now()}
	if launch := q.GetLaunch(); launch != nil && launch.RunId == "" {
		launch.RunId = "run-" + id
	}
	data, err = (protojson.MarshalOptions{UseProtoNames: true}).Marshal(&q)
	if err != nil || len(data) > maxRequest {
		problem(w, 400, "prepared mutation exceeds the controller request limit")
		return
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	pending := &pendingMutation{RequestID: q.Command.RequestId, Request: data, State: "prepared"}
	if !retainPreparedLocked(r.Context(), sess, pending) {
		problem(w, 409, "review changed; finish the current mutation before preparing another")
		return
	}
	writeJSON(w, 200, sess.pending)
}

// Bind receipts to the reviewed record, including the effective tenant when a
// scoped credential omitted it. Configuration writes advance exactly one revision.
func mutationAcknowledgementMatches(q *jp.MutationRequest, ack *jp.MutationResponse, identity Identity) bool {
	if ack.Reference == nil {
		return false
	}
	tenant := q.TenantId
	if tenant == "" {
		tenant = identity.TenantID
	}
	var kind jp.Kind
	var id string
	var expected uint64
	revisioned := true
	switch op := q.Operation.(type) {
	case *jp.MutationRequest_PutTenant:
		kind, expected = jp.Kind_TENANT, op.PutTenant.GetExpectedRevision()
	case *jp.MutationRequest_PutGroup:
		kind, id, expected = jp.Kind_GROUP, op.PutGroup.GetGroup().GetId(), op.PutGroup.GetExpectedRevision()
	case *jp.MutationRequest_PutDefinition:
		kind, id, expected = jp.Kind_DEFINITION, op.PutDefinition.GetDefinition().GetId(), op.PutDefinition.GetExpectedRevision()
	case *jp.MutationRequest_PutBinding:
		kind, id, expected = jp.Kind_BINDING, op.PutBinding.GetBinding().GetId(), op.PutBinding.GetExpectedRevision()
	case *jp.MutationRequest_PutResource:
		kind, id, expected = jp.Kind_RESOURCE, op.PutResource.GetSpec().GetId(), op.PutResource.GetExpectedRevision()
		tenant = ""
	case *jp.MutationRequest_PutGrant:
		kind, id, expected = jp.Kind_GRANT, op.PutGrant.GetResourceId(), op.PutGrant.GetExpectedRevision()
	case *jp.MutationRequest_PutTrigger:
		kind, id, expected = jp.Kind_TRIGGER, op.PutTrigger.GetDefinition().GetId(), op.PutTrigger.GetExpectedRevision()
	case *jp.MutationRequest_PauseTrigger:
		kind, id, expected = jp.Kind_TRIGGER, op.PauseTrigger.GetId(), op.PauseTrigger.GetExpectedRevision()
	case *jp.MutationRequest_PutLimits:
		kind, expected = jp.Kind_LIMITS, op.PutLimits.GetExpectedRevision()
	case *jp.MutationRequest_PutConfigurationLimits:
		kind, expected = jp.Kind_CONFIGURATION_LIMITS, op.PutConfigurationLimits.GetExpectedRevision()
	case *jp.MutationRequest_Launch:
		kind, id, revisioned = jp.Kind_RUN, op.Launch.GetRunId(), false
	case *jp.MutationRequest_Cancel:
		kind, id, revisioned = jp.Kind_RUN, op.Cancel.GetRunId(), false
	case *jp.MutationRequest_ResolveRun, *jp.MutationRequest_ResolveResource, *jp.MutationRequest_Purge:
		return recoveryAcknowledgementMatches(q, ack)
	default:
		return false
	}
	return ack.Reference.Kind == kind && ack.Reference.TenantId == tenant && ack.Reference.Id == id &&
		(!revisioned || expected < math.MaxUint64 && ack.Revision == expected+1)
}

func (s *Server) commit(w http.ResponseWriter, r *http.Request, sess *session) {
	var input struct {
		RequestID string `json:"request_id"`
	}
	if err := decode(w, r, 1024, &input); err != nil {
		problem(w, 400, "provide the current mutation request_id")
		return
	}
	sess.mu.Lock()
	p := sess.pending
	if p == nil || p.RequestID != input.RequestID {
		sess.mu.Unlock()
		problem(w, 409, "mutation identity does not match the current review")
		return
	}
	if p.State == "submitting" {
		sess.mu.Unlock()
		problem(w, 409, "this mutation is already being submitted")
		return
	}
	if p.State == "confirmed" || p.State == "rejected" {
		writeJSON(w, 200, p)
		sess.mu.Unlock()
		return
	}
	wasUncertain := p.State == "uncertain"
	p.State = "submitting"
	if sess.shared != nil {
		sess.shared.attemptUntil = time.Now().Add(20 * time.Second).UTC()
		if err := s.saveShared(r.Context(), sess); err != nil {
			p.State = "uncertain"
			sess.mu.Unlock()
			problem(w, 503, "submission marker could not be confirmed; reload the retained review")
			return
		}
	}
	controller := sess.controller
	sess.mu.Unlock()
	endpoint := p.Endpoint
	if endpoint == "" {
		endpoint = "mutate"
	}
	requestContext := r.Context()
	if sess.shared != nil {
		var cancel context.CancelFunc
		requestContext, cancel = context.WithDeadline(requestContext, sess.shared.attemptUntil)
		defer cancel()
	}
	response, index, err := s.request(requestContext, sess.credential(), controller, endpoint, http.MethodPost, p.Request, false)
	sess.mu.Lock()
	defer sess.mu.Unlock()
	sess.controller = index
	p.State, p.Response, p.HTTPCode, p.Error = "uncertain", response.body, response.status, ""
	if err != nil {
		p.Error = err.Error()
	} else if response.status >= 200 && response.status < 300 {
		matches := false
		if p.Endpoint == "legacy/ResolveChannel" || p.Endpoint == "legacy/ExpireChannel" {
			matches = gatewayAcknowledgementMatches(p, response.body)
		} else if p.Endpoint == "worker-enrollments/mutate" {
			var ack jp.WorkerEnrollmentAcknowledgement
			var q jp.WorkerEnrollmentMutation
			matches = json.Unmarshal(response.body, &ack) == nil && json.Unmarshal(p.Request, &q) == nil && ack.RequestID == p.RequestID && ack.Enrollment.TenantID == q.TenantID && ack.Enrollment.WorkerID == q.WorkerID && ack.Enrollment.Revision == q.ExpectedRevision+1 && ack.Enrollment.Revision != 0
		} else if p.Endpoint == "worker-deployments/mutate" {
			var ack jp.WorkerDeploymentAcknowledgement
			var q jp.WorkerDeploymentMutation
			matches = json.Unmarshal(response.body, &ack) == nil && json.Unmarshal(p.Request, &q) == nil && ack.RequestID == p.RequestID && ack.Deployment.TenantID == q.TenantID && ack.Deployment.ID == q.ID && ack.Deployment.WorkerID == q.WorkerID && ack.Deployment.Revision == q.ExpectedRevision+1 && ack.Deployment.Revision != 0 && ack.Deployment.Generation != 0
		} else {
			var ack jp.MutationResponse
			matches = protojson.Unmarshal(response.body, &ack) == nil && ack.RequestId == p.RequestID && ack.Reference != nil
			if matches {
				var q jp.MutationRequest
				matches = protojson.Unmarshal(p.Request, &q) == nil && mutationAcknowledgementMatches(&q, &ack, sess.identity)
			}
		}
		if matches && sess.releasePlan != nil {
			matches = releaseAcknowledgementMatches(sess.releasePlan, p)
		}
		if !matches {
			p.Error = "controller acknowledgement does not match this mutation; retain and retry the exact request"
		} else {
			p.State = "confirmed"
			if sess.releasePlan != nil {
				for i := range sess.releasePlan.Steps {
					step := &sess.releasePlan.Steps[i]
					if step.RequestID == p.RequestID {
						step.Confirmed = true
						step.Acknowledgement = append([]byte(nil), p.Response...)
					}
				}
			}
		}
	} else if !wasUncertain && response.status < 500 && response.status != http.StatusRequestTimeout {
		p.State = "rejected"
	}
	if response.status >= 500 {
		sess.controller = (index + 1) % len(s.controllers)
	}
	w.Header().Set("X-Djob-Controller", s.controllers[index].String())
	writeJSON(w, 200, p)
}

func (s *Server) discard(w http.ResponseWriter, r *http.Request, sess *session) {
	var input struct {
		RequestID string `json:"request_id"`
	}
	if err := decode(w, r, 1024, &input); err != nil {
		problem(w, 400, "provide the current mutation request_id")
		return
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	p := sess.pending
	if p == nil || p.RequestID != input.RequestID {
		problem(w, 409, "mutation identity does not match the current review")
		return
	}
	if p.State == "uncertain" || p.State == "submitting" {
		problem(w, 409, "an uncertain mutation must retain its identity for exact retry")
		return
	}
	sess.pending = nil
	writeJSON(w, 200, map[string]bool{"cleared": true})
}
