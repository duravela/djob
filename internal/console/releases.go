package console

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"io"
	"math"
	"net/http"
	"time"
)

type releaseStep struct {
	Kind            string          `json:"kind"`
	ID              string          `json:"id"`
	Expected        uint64          `json:"expected_revision,string"`
	Before          json.RawMessage `json:"before,omitempty"`
	Mutation        json.RawMessage `json:"mutation"`
	RequestID       string          `json:"request_id,omitempty"`
	Acknowledgement json.RawMessage `json:"acknowledgement,omitempty"`
	Confirmed       bool            `json:"confirmed"`
}
type releasePlan struct {
	ID               string              `json:"id"`
	Tenant           string              `json:"tenant_id"`
	Source           string              `json:"source"`
	Target           string              `json:"target"`
	Digest           string              `json:"digest"`
	TargetSnapshot   jp.WorkerDeployment `json:"target_snapshot"`
	Canary           string              `json:"canary_run,omitempty"`
	AllowUnqualified bool                `json:"allow_unqualified"`
	AllowUnobserved  bool                `json:"allow_unobserved"`
	Required         []json.RawMessage   `json:"required_definitions,omitempty"`
	Steps            []releaseStep       `json:"steps"`
}

func (p *releasePlan) digest() string {
	copy := *p
	copy.Digest = ""
	copy.Steps = append([]releaseStep(nil), p.Steps...)
	for i := range copy.Steps {
		copy.Steps[i].RequestID = ""
		copy.Steps[i].Confirmed = false
		copy.Steps[i].Acknowledgement = nil
	}
	raw, _ := json.Marshal(copy)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}
func (s *Server) releaseRead(ctx context.Context, sess *session, path string, q any, out any) error {
	var raw []byte
	var err error
	if message, ok := q.(proto.Message); ok {
		raw, err = (protojson.MarshalOptions{UseProtoNames: true}).Marshal(message)
	} else {
		raw, err = json.Marshal(q)
	}
	if err != nil {
		return err
	}
	response, _, err := s.request(ctx, sess.credential(), 0, path, http.MethodPost, raw, true)
	if err != nil {
		return err
	}
	if response.status != 200 {
		return fmt.Errorf("release inspection returned HTTP %d: %s", response.status, response.body)
	}
	if message, ok := out.(proto.Message); ok {
		return protojson.Unmarshal(response.body, message)
	}
	return json.Unmarshal(response.body, out)
}
func (s *Server) releaseRecord(ctx context.Context, sess *session, tenant string, kind jp.Kind, id string) (*jp.Record, error) {
	out := new(jp.Record)
	err := s.releaseRead(ctx, sess, "get", &jp.GetRequest{Reference: &jp.Reference{Kind: kind, TenantId: tenant, Id: id}}, out)
	if err == nil && (out.Reference == nil || out.Reference.Kind != kind || out.Reference.TenantId != tenant || out.Reference.Id != id) {
		err = fmt.Errorf("record scope mismatch")
	}
	return out, err
}
func (s *Server) releaseDeployment(ctx context.Context, sess *session, tenant, id string) (jp.WorkerDeployment, error) {
	var out jp.WorkerDeployment
	err := s.releaseRead(ctx, sess, "worker-deployments/get", jp.WorkerDeploymentQuery{TenantID: tenant, ID: id}, &out)
	if err == nil && (!out.Published || out.ID != id || out.TenantID != tenant) {
		err = fmt.Errorf("choose a published release in this tenant")
	}
	return out, err
}
func (s *Server) releaseRecords(ctx context.Context, sess *session, tenant string, kind jp.Kind) ([]*jp.Record, error) {
	var records []*jp.Record
	token := ""
	seen := map[string]bool{}
	for i := 0; i < 512; i++ {
		var page jp.ListResponse
		if err := s.releaseRead(ctx, sess, "list", &jp.ListRequest{Kind: kind, TenantId: tenant, PageSize: 32, PageToken: token}, &page); err != nil {
			return nil, err
		}
		for _, r := range page.Records {
			if r.Reference == nil || r.Reference.Kind != kind || r.Reference.TenantId != tenant {
				return nil, fmt.Errorf("list scope mismatch")
			}
			full, err := s.releaseRecord(ctx, sess, tenant, kind, r.Reference.Id)
			if err != nil {
				return nil, err
			}
			records = append(records, full)
		}
		token = page.NextPageToken
		if token == "" {
			return records, nil
		}
		if seen[token] {
			return nil, fmt.Errorf("repeated page token")
		}
		seen[token] = true
	}
	return nil, fmt.Errorf("release inspection exceeds 512 pages")
}
func referencesRelease(d *jp.Definition, id string) bool {
	return d != nil && (d.WorkerRelease == id || d.WorkerReleaseWeights[id] > 0)
}
func moveRelease(d *jp.Definition, source, target string) *jp.Definition {
	out := proto.Clone(d).(*jp.Definition)
	out.Revision = 0
	if len(out.WorkerReleaseWeights) > 0 {
		n := out.WorkerReleaseWeights[source]
		delete(out.WorkerReleaseWeights, source)
		out.WorkerReleaseWeights[target] += n
	} else {
		out.WorkerRelease = target
	}
	return out
}
func (p *releasePlan) add(kind, id string, expected uint64, before *jp.Record, q *jp.MutationRequest) error {
	if expected == math.MaxUint64 {
		return fmt.Errorf("revision exhausted")
	}
	q.TenantId = p.Tenant
	raw, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(q)
	if err != nil {
		return err
	}
	var old []byte
	if before != nil {
		old, err = (protojson.MarshalOptions{UseProtoNames: true}).Marshal(before)
		if err != nil {
			return err
		}
	}
	if put := q.GetPutDefinition(); put != nil {
		raw, _ := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(put.Definition)
		p.Required = append(p.Required, raw)
	}
	p.Steps = append(p.Steps, releaseStep{Kind: kind, ID: id, Expected: expected, Before: old, Mutation: raw})
	return nil
}
func (s *Server) buildReleasePlan(ctx context.Context, sess *session, p *releasePlan) error {
	if p.Source == p.Target {
		return fmt.Errorf("choose distinct releases")
	}
	if _, err := s.releaseDeployment(ctx, sess, p.Tenant, p.Source); err != nil {
		return err
	}
	target, err := s.releaseDeployment(ctx, sess, p.Tenant, p.Target)
	if err != nil {
		return err
	}
	p.TargetSnapshot = target
	if target.State != "active" || target.Spec.Replicas == 0 {
		return fmt.Errorf("target must admit new runs")
	}
	if !p.AllowUnqualified {
		if err = s.verifyReleaseCanary(ctx, sess, p); err != nil {
			return err
		}
	}
	definitions, err := s.releaseRecords(ctx, sess, p.Tenant, jp.Kind_DEFINITION)
	if err != nil {
		return err
	}
	planned := map[string]uint64{}
	for _, r := range definitions {
		d := r.GetDefinition()
		if referencesRelease(d, p.Source) {
			if err = p.add("DEFINITION", d.Id, d.Revision, r, &jp.MutationRequest{Operation: &jp.MutationRequest_PutDefinition{PutDefinition: &jp.PutDefinition{ExpectedRevision: d.Revision, Definition: moveRelease(d, p.Source, p.Target)}}}); err != nil {
				return err
			}
			planned[d.Id] = d.Revision + 1
		}
	}
	triggers, err := s.releaseRecords(ctx, sess, p.Tenant, jp.Kind_TRIGGER)
	if err != nil {
		return err
	}
	for _, r := range triggers {
		t := r.GetTrigger()
		if t == nil || !referencesRelease(t.JobDefinition, p.Source) {
			continue
		}
		d := proto.Clone(t.Definition).(*jp.TriggerDefinition)
		if revision, ok := planned[d.DefinitionId]; ok {
			d.DefinitionRevision = revision
		} else {
			current, err := s.releaseRecord(ctx, sess, p.Tenant, jp.Kind_DEFINITION, d.DefinitionId)
			if err != nil {
				return err
			}
			if referencesRelease(current.GetDefinition(), p.Target) && !referencesRelease(current.GetDefinition(), p.Source) {
				d.DefinitionRevision = current.GetDefinition().Revision
				raw, _ := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(current.GetDefinition())
				p.Required = append(p.Required, raw)
			} else {
				clone := moveRelease(t.JobDefinition, p.Source, p.Target)
				sum := sha256.Sum256([]byte(d.Id))
				clone.Id = "migration-" + p.ID[:12] + "-" + hex.EncodeToString(sum[:6])
				d.DefinitionId = clone.Id
				d.DefinitionRevision = 1
				if err = p.add("DEFINITION", clone.Id, 0, nil, &jp.MutationRequest{Operation: &jp.MutationRequest_PutDefinition{PutDefinition: &jp.PutDefinition{Definition: clone}}}); err != nil {
					return err
				}
			}
		}
		if err = p.add("TRIGGER", d.Id, t.Revision, r, &jp.MutationRequest{Operation: &jp.MutationRequest_PutTrigger{PutTrigger: &jp.PutTrigger{ExpectedRevision: t.Revision, Definition: d}}}); err != nil {
			return err
		}
	}
	return nil
}
func (s *Server) verifyReleaseCanary(ctx context.Context, sess *session, p *releasePlan) error {
	if p.Canary == "" {
		return fmt.Errorf("select a successful dedicated target canary or explicitly allow an unqualified plan")
	}
	r, err := s.releaseRecord(ctx, sess, p.Tenant, jp.Kind_RUN, p.Canary)
	if err != nil {
		return err
	}
	run := r.GetRun()
	if run == nil || run.Status != "succeeded" || run.Definition == nil || run.Definition.WorkerRelease != p.Target || run.CreatedAt == nil || run.LastTransition == nil {
		return fmt.Errorf("canary must be a succeeded run pinned to the target release")
	}
	age := time.Since(run.LastTransition.AsTime())
	if age < -5*time.Second || age > 30*time.Minute || run.CreatedAt.AsTime().Before(p.TargetSnapshot.ChangedAt) {
		return fmt.Errorf("canary is stale for this target release")
	}
	return nil
}
func (s *Server) releasePlans(w http.ResponseWriter, r *http.Request, sess *session) {
	sess.releaseMu.Lock()
	defer sess.releaseMu.Unlock()
	if !platformSession(sess) {
		problem(w, 403, "release plans require a platform operator")
		return
	}
	var q struct {
		Tenant                           string `json:"tenant_id"`
		Source, Target, Approval, Canary string
		AllowUnqualified                 bool `json:"allow_unqualified"`
		AllowUnobserved                  bool `json:"allow_unobserved"`
	}
	if decode(w, r, 8192, &q) != nil {
		problem(w, 400, "invalid release plan request")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 45*time.Second)
	defer cancel()
	action := r.PathValue("method")
	if action == "build" || action == "rollback" {
		sess.mu.Lock()
		old := sess.releasePlan
		pending := sess.pending
		pendingBusy := pending != nil && (pending.State == "uncertain" || pending.State == "submitting" || pending.State == "prepared")
		sess.mu.Unlock()
		if pendingBusy {
			problem(w, 409, "finish the retained mutation before creating a release plan")
			return
		}
		id, err := randomID()
		if err != nil {
			problem(w, 500, "cannot allocate plan identity")
			return
		}
		p := &releasePlan{ID: id, Tenant: q.Tenant, Source: q.Source, Target: q.Target, Canary: q.Canary, AllowUnqualified: q.AllowUnqualified, AllowUnobserved: q.AllowUnobserved, Steps: []releaseStep{}}
		if action == "rollback" {
			err = s.rollbackReleasePlan(ctx, sess, old, p)
		} else {
			err = s.buildReleasePlan(ctx, sess, p)
		}
		if err != nil {
			problem(w, 400, err.Error())
			return
		}
		p.Digest = p.digest()
		raw, _ := json.Marshal(p)
		if len(raw) > 2<<20 {
			problem(w, 400, "release plan exceeds 2 MiB; use selective CLI plans")
			return
		}
		sess.mu.Lock()
		defer sess.mu.Unlock()
		if sess.releasePlan != old || sess.pending != pending {
			problem(w, 409, "review changed during planning")
			return
		}
		sess.releasePlan = p
		writeJSON(w, 200, p)
		return
	}
	sess.mu.Lock()
	p := sess.releasePlan
	sess.mu.Unlock()
	if p == nil {
		problem(w, 404, "no retained release plan")
		return
	}
	if action == "current" {
		sess.mu.Lock()
		defer sess.mu.Unlock()
		writeJSON(w, 200, p)
		return
	}
	if action != "next" {
		problem(w, 404, "unknown release plan operation")
		return
	}
	sess.mu.Lock()
	validApproval := q.Approval == p.Digest && p.Digest == p.digest()
	sess.mu.Unlock()
	if !validApproval {
		problem(w, 409, "approve the exact reviewed plan digest")
		return
	}
	sess.mu.Lock()
	if sess.pending != nil {
		if sess.pending.State != "confirmed" {
			sess.mu.Unlock()
			problem(w, 409, "finish the existing reviewed request")
			return
		}
	}
	previous := sess.pending
	var step *releaseStep
	for i := range p.Steps {
		if !p.Steps[i].Confirmed {
			step = &p.Steps[i]
			break
		}
	}
	sess.mu.Unlock()
	if step == nil {
		writeJSON(w, 200, map[string]bool{"complete": true})
		return
	}
	if step.Kind != "WORKER_DEPLOYMENT" {
		if err := s.releasePreflight(ctx, sess, p); err != nil {
			problem(w, 409, err.Error())
			return
		}
	}
	// Use the normal retained request preparation and exact-retry path.
	preparation := &releasePreparation{plan: p, step: step, previous: previous}
	clone := r.Clone(context.WithValue(ctx, releasePreparationKey{}, preparation))
	clone.Body = ioNop(step.Mutation)
	clone.ContentLength = int64(len(step.Mutation))
	captured := &bufferedResponse{header: make(http.Header)}
	if step.Kind == "WORKER_DEPLOYMENT" {
		s.prepareDeployment(captured, clone, sess)
	} else {
		s.prepare(captured, clone, sess)
	}
	if captured.status != 200 {
		problem(w, captured.status, captured.body.String())
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	w.Write(captured.body.Bytes())
}
func ioNop(raw []byte) io.ReadCloser { return io.NopCloser(bytes.NewReader(raw)) }

// releasePreparation is an internal handoff, never accepted from a browser.
// The ordinary preparation handlers publish the pending request and its plan
// identity together while holding the session mutex.
type releasePreparationKey struct{}
type releasePreparation struct {
	plan     *releasePlan
	step     *releaseStep
	previous *pendingMutation
}

func retainPreparedLocked(ctx context.Context, sess *session, pending *pendingMutation) bool {
	preparation, _ := ctx.Value(releasePreparationKey{}).(*releasePreparation)
	if preparation == nil {
		if sess.pending != nil {
			return false
		}
	} else {
		if sess.releasePlan != preparation.plan || sess.pending != preparation.previous || preparation.previous != nil && preparation.previous.State != "confirmed" || preparation.step.Confirmed {
			return false
		}
	}
	clearDraftLocked(sess)
	pending.DraftRevision = sess.draftRevision
	if preparation != nil {
		preparation.step.RequestID = pending.RequestID
	}
	sess.pending = pending
	return true
}
func releaseAcknowledgementMatches(plan *releasePlan, pending *pendingMutation) bool {
	for _, step := range plan.Steps {
		if step.RequestID != pending.RequestID {
			continue
		}
		// The deployment-specific matcher already checks its scope and revision.
		if step.Kind == "WORKER_DEPLOYMENT" {
			return true
		}
		var ack jp.MutationResponse
		return protojson.Unmarshal(pending.Response, &ack) == nil && ack.Reference != nil && ack.Reference.Kind.String() == step.Kind && ack.Reference.TenantId == plan.Tenant && ack.Reference.Id == step.ID && step.Expected < math.MaxUint64 && ack.Revision == step.Expected+1
	}
	return true // An ordinary review can coexist with a retained plan.
}
