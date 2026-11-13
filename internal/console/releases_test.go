package console

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestReleasePlanDigestBindsSnapshotsButAllowsRetainedProgress(t *testing.T) {
	p := &releasePlan{ID: "reviewed", Tenant: "alpha", Source: "v1", Target: "v2", Steps: []releaseStep{{ID: "job", Kind: "DEFINITION", Expected: 9007199254740993, Mutation: json.RawMessage(`{"put_definition":{"expected_revision":"9007199254740993"}}`)}}}
	digest := p.digest()
	p.Steps[0].RequestID = "exact-request"
	p.Steps[0].Confirmed = true
	p.Steps[0].Acknowledgement = json.RawMessage(`{"request_id":"exact-request"}`)
	require.Equal(t, digest, p.digest())
	p.Steps[0].Expected++
	require.NotEqual(t, digest, p.digest())
}
func TestConsoleCanaryQualificationRequiresRecentSuccessfulTargetExecution(t *testing.T) {
	now := time.Now().UTC()
	status, release := "succeeded", "v2"
	created, finished := now, now
	b := newBrowserAs(t, Identity{Subject: "ops", Role: "operator"}, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/jobs/v1/get", r.URL.Path)
		writeJSON(w, 200, map[string]any{"reference": map[string]any{"tenant_id": "alpha", "kind": "RUN", "id": "canary"}, "run": map[string]any{"id": "canary", "status": status, "definition": map[string]any{"id": "test", "worker_release": release}, "created_at": created.Format(time.RFC3339Nano), "last_transition": finished.Format(time.RFC3339Nano)}})
	})
	sess := b.server.sessions[b.cookie.Value]
	p := &releasePlan{Tenant: "alpha", Target: "v2", Canary: "canary", TargetSnapshot: jp.WorkerDeployment{ChangedAt: now.Add(-time.Minute)}}
	require.NoError(t, b.server.verifyReleaseCanary(context.Background(), sess, p))
	status = "failed"
	require.Error(t, b.server.verifyReleaseCanary(context.Background(), sess, p))
	status = "succeeded"
	release = "v1"
	require.Error(t, b.server.verifyReleaseCanary(context.Background(), sess, p))
	release = "v2"
	created = now.Add(-time.Hour)
	require.Error(t, b.server.verifyReleaseCanary(context.Background(), sess, p))
	created = now
	finished = now.Add(-time.Hour)
	require.Error(t, b.server.verifyReleaseCanary(context.Background(), sess, p))
}

func TestBlockedReleaseStepKeepsPreviousConfirmedReview(t *testing.T) {
	target := jp.WorkerDeployment{TenantID: "alpha", ID: "v2", WorkerID: "w2", Published: true, State: "active", Revision: 2, Generation: 1, Spec: jp.WorkerDeploymentSpec{Image: "registry.example/worker@sha256:" + strings.Repeat("a", 64), Replicas: 1}}
	b := newBrowserAs(t, Identity{Subject: "ops", Role: "operator"}, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/jobs/v1/worker-deployments/get":
			writeJSON(w, 200, target)
		case "/api/jobs/v1/worker-deployments/status":
			writeJSON(w, 200, map[string]any{"tenant_id": "alpha", "id": "v2", "observed": false})
		case "/api/jobs/v1/workers":
			writeJSON(w, 200, jp.WorkerInventory{TenantID: "alpha", ObservedAt: time.Now()})
		default:
			t.Errorf("unexpected controller request: %s", r.URL.Path)
			w.WriteHeader(500)
		}
	})
	sess := b.server.sessions[b.cookie.Value]
	p := &releasePlan{Tenant: "alpha", Source: "v1", Target: "v2", TargetSnapshot: target, AllowUnqualified: true, Steps: []releaseStep{{Kind: "DEFINITION", ID: "job", Expected: 1, Mutation: json.RawMessage(`{"tenant_id":"alpha","put_definition":{"expected_revision":"1","definition":{"id":"job","runtime":"steps/v1","handler":"test","handler_version":"1","worker_release":"v2"}}}`)}}}
	p.Digest = p.digest()
	sess.releasePlan = p
	previous := &pendingMutation{RequestID: "previous-confirmed-request", State: "confirmed", Response: json.RawMessage(`{"request_id":"previous-confirmed-request"}`)}
	sess.pending = previous
	response := b.call("POST", "release-plans/next", map[string]string{"approval": p.Digest})
	require.Equal(t, 409, response.Code, response.Body.String())
	require.Same(t, previous, sess.pending)
	require.Empty(t, p.Steps[0].RequestID)
	info := b.call("GET", "session", nil)
	require.Contains(t, info.Body.String(), previous.RequestID)
}

func TestReleaseStepRejectsForeignOrWrongRevisionReceiptsAndRetriesExactRequest(t *testing.T) {
	target := jp.WorkerDeployment{TenantID: "alpha", ID: "v2", WorkerID: "w2", Published: true, State: "active", Revision: 2, Generation: 1, Spec: jp.WorkerDeploymentSpec{Image: "registry.example/worker@sha256:" + strings.Repeat("a", 64), Replicas: 1}}
	var sent [][]byte
	reply := 0
	b := newBrowserAs(t, Identity{Subject: "ops", Role: "operator"}, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/jobs/v1/worker-deployments/get" {
			writeJSON(w, 200, target)
			return
		}
		require.Equal(t, "/api/jobs/v1/mutate", r.URL.Path)
		raw, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		sent = append(sent, raw)
		var request jp.MutationRequest
		require.NoError(t, protojson.Unmarshal(raw, &request))
		ack := &jp.MutationResponse{RequestId: request.Command.RequestId, Reference: &jp.Reference{Kind: jp.Kind_DEFINITION, TenantId: "alpha", Id: "job"}, Revision: 2}
		switch reply {
		case 0:
			ack.Revision = 3
		case 1:
			ack.Reference.TenantId = "beta"
		case 2:
			ack.Reference.Kind = jp.Kind_TRIGGER
		case 3:
			ack.Reference.Id = "other-job"
		}
		reply++
		data, err := protojson.Marshal(ack)
		require.NoError(t, err)
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	})
	sess := b.server.sessions[b.cookie.Value]
	p := &releasePlan{Tenant: "alpha", Source: "v1", Target: "v2", TargetSnapshot: target, AllowUnqualified: true, AllowUnobserved: true, Steps: []releaseStep{{Kind: "DEFINITION", ID: "job", Expected: 1, Mutation: json.RawMessage(`{"tenant_id":"alpha","put_definition":{"expected_revision":"1","definition":{"id":"job","runtime":"steps/v1","handler":"test","handler_version":"1","worker_release":"v2"}}}`)}}}
	p.Digest = p.digest()
	sess.releasePlan = p
	next := b.call("POST", "release-plans/next", map[string]string{"approval": p.Digest})
	require.Equal(t, 200, next.Code, next.Body.String())
	require.Equal(t, sess.pending.RequestID, p.Steps[0].RequestID)
	for i := 0; i < 4; i++ {
		ack := b.commit(sess.pending.RequestID)
		require.Equal(t, "uncertain", ack.State)
		require.False(t, p.Steps[0].Confirmed)
	}
	ack := b.commit(sess.pending.RequestID)
	require.Equal(t, "confirmed", ack.State)
	require.True(t, p.Steps[0].Confirmed)
	require.JSONEq(t, string(ack.Response), string(p.Steps[0].Acknowledgement))
	for _, raw := range sent {
		require.Equal(t, sent[0], raw)
	}
}

type publicationObserver struct {
	*httptest.ResponseRecorder
	observe func()
}

func (w *publicationObserver) WriteHeader(code int) {
	if code == 200 {
		w.observe()
	}
	w.ResponseRecorder.WriteHeader(code)
}
func TestReleaseStepIdentityIsBoundBeforePreparationBecomesVisible(t *testing.T) {
	for _, deployment := range []bool{false, true} {
		t.Run(fmt.Sprint("deployment=", deployment), func(t *testing.T) {
			b := newBrowserAs(t, Identity{Subject: "ops", Role: "operator"}, func(http.ResponseWriter, *http.Request) { t.Error("preparation sent a controller mutation") })
			sess := b.server.sessions[b.cookie.Value]
			mutation := json.RawMessage(`{"tenant_id":"alpha","put_definition":{"expected_revision":"1","definition":{"id":"job","runtime":"steps/v1","handler":"test","handler_version":"1"}}}`)
			if deployment {
				mutation = json.RawMessage(`{"tenant_id":"alpha","id":"v1","worker_id":"w1","action":"scale","expected_revision":"2","replicas":1,"reason":"Resume reviewed rollback"}`)
			}
			p := &releasePlan{Steps: []releaseStep{{Mutation: mutation}}}
			sess.releasePlan = p
			previous := &pendingMutation{State: "confirmed", RequestID: "old"}
			sess.pending = previous
			context := context.WithValue(context.Background(), releasePreparationKey{}, &releasePreparation{plan: p, step: &p.Steps[0], previous: previous})
			request := httptest.NewRequest("POST", testOrigin+api+"mutations", bytes.NewReader(mutation)).WithContext(context)
			request.Header.Set("Content-Type", "application/json")
			observed := false
			writer := &publicationObserver{ResponseRecorder: httptest.NewRecorder(), observe: func() {
				observed = true
				require.NotEmpty(t, p.Steps[0].RequestID)
				require.Equal(t, sess.pending.RequestID, p.Steps[0].RequestID)
				require.NotSame(t, previous, sess.pending)
			}}
			if deployment {
				b.server.prepareDeployment(writer, request, sess)
			} else {
				b.server.prepare(writer, request, sess)
			}
			require.Equal(t, 200, writer.Code, writer.Body.String())
			require.True(t, observed)
		})
	}
}

func TestReleasePreparationDoesNotReplaceAReviewChangedDuringHealthCheck(t *testing.T) {
	entered, resume := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	defer unblock()
	target := jp.WorkerDeployment{TenantID: "alpha", ID: "v2", WorkerID: "w2", Published: true, State: "active", Revision: 2, Generation: 1, Spec: jp.WorkerDeploymentSpec{Image: "registry.example/worker@sha256:" + strings.Repeat("a", 64), Replicas: 1}}
	b := newBrowserAs(t, Identity{Subject: "ops", Role: "operator"}, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/jobs/v1/worker-deployments/get", r.URL.Path)
		close(entered)
		<-resume
		writeJSON(w, 200, target)
	})
	sess := b.server.sessions[b.cookie.Value]
	p := &releasePlan{Tenant: "alpha", Source: "v1", Target: "v2", TargetSnapshot: target, AllowUnqualified: true, AllowUnobserved: true, Steps: []releaseStep{{Kind: "DEFINITION", ID: "job", Expected: 1, Mutation: json.RawMessage(`{"tenant_id":"alpha","put_definition":{"expected_revision":"1","definition":{"id":"job","runtime":"steps/v1","handler":"test","handler_version":"1","worker_release":"v2"}}}`)}}}
	p.Digest = p.digest()
	sess.releasePlan = p
	sess.pending = &pendingMutation{RequestID: "previous", State: "confirmed"}
	result := make(chan *httptest.ResponseRecorder, 1)
	go func() { result <- b.call("POST", "release-plans/next", map[string]string{"approval": p.Digest}) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("health inspection did not start")
	}
	discarded := b.call("DELETE", "mutations/current", map[string]string{"request_id": "previous"})
	require.Equal(t, 200, discarded.Code, discarded.Body.String())
	manual := b.prepared()
	unblock()
	select {
	case response := <-result:
		require.Equal(t, 409, response.Code, response.Body.String())
	case <-time.After(3 * time.Second):
		t.Fatal("preparation did not finish")
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	require.Equal(t, manual.RequestID, sess.pending.RequestID)
	require.Empty(t, p.Steps[0].RequestID)
}
