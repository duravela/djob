package console

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"github.com/duravela/djob/sdk/go/worker"
	"github.com/stretchr/testify/require"
)

func TestConsoleEnrollmentSecretIsSeparateFromExactRetryAndPublicSession(t *testing.T) {
	var submitted [][]byte
	b := newBrowserAs(t, Identity{Subject: "platform", Role: "operator"}, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/jobs/v1/worker-enrollments/mutate", r.URL.Path)
		data, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		submitted = append(submitted, data)
		if len(submitted) == 1 {
			problem(w, 503, "receipt unavailable")
			return
		}
		var q jp.WorkerEnrollmentMutation
		require.NoError(t, json.Unmarshal(data, &q))
		writeJSON(w, 200, jp.WorkerEnrollmentAcknowledgement{RequestID: q.Command.RequestID, Enrollment: jp.WorkerEnrollment{TenantID: q.TenantID, WorkerID: q.WorkerID, Revision: 1, State: "enabled"}})
	})
	input := map[string]string{"tenant_id": "alpha", "worker_id": "company-worker", "action": "enroll", "subject": "acme/deployment", "reason": "first deployment", "expected_revision": "0"}
	response := b.call(http.MethodPost, "mutations/worker-enrollment", input)
	require.Equal(t, 200, response.Code, response.Body.String())
	var pending pendingMutation
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &pending))
	require.True(t, pending.WorkerConfigAvailable)
	requestID := map[string]string{"request_id": pending.RequestID}
	require.Equal(t, 409, b.call(http.MethodPost, "mutations/worker-config", requestID).Code)
	require.Equal(t, "uncertain", b.commit(pending.RequestID).State)
	require.Equal(t, 409, b.call(http.MethodDelete, "mutations/current", requestID).Code)
	require.Equal(t, "confirmed", b.commit(pending.RequestID).State)
	config := b.call(http.MethodPost, "mutations/worker-config", requestID)
	require.Equal(t, 200, config.Code)
	var cfg struct{ Token, TenantID, WorkerID string }
	var value map[string]any
	require.NoError(t, json.Unmarshal(config.Body.Bytes(), &value))
	cfg.Token = value["token"].(string)
	require.True(t, strings.HasPrefix(cfg.Token, "djobw1."))
	require.Len(t, cfg.Token, 71)
	require.Equal(t, "alpha", value["tenant_id"])
	require.Equal(t, "company-worker", value["worker_id"])
	require.Equal(t, "grpc", value["transport"])
	path := filepath.Join(t.TempDir(), "worker.json")
	require.NoError(t, os.WriteFile(path, config.Body.Bytes(), 0600))
	loaded, err := worker.LoadConfig(path)
	require.NoError(t, err)
	require.Equal(t, worker.TransportGRPC, loaded.Transport)
	require.Equal(t, cfg.Token, loaded.Token)
	var wire jp.WorkerEnrollmentMutation
	require.NoError(t, json.Unmarshal(pending.Request, &wire))
	hash := sha256.Sum256([]byte(cfg.Token))
	require.Equal(t, hex.EncodeToString(hash[:]), wire.CredentialHash)
	require.Len(t, submitted, 2)
	require.Equal(t, submitted[0], submitted[1])
	require.NotContains(t, string(submitted[0]), cfg.Token)
	info := b.call(http.MethodGet, "session", nil)
	require.Equal(t, 200, info.Code)
	require.NotContains(t, info.Body.String(), cfg.Token)
	require.NotContains(t, response.Body.String(), cfg.Token)
	require.Equal(t, "no-store", config.Header().Get("Cache-Control"))
	require.Equal(t, 200, b.call(http.MethodDelete, "mutations/current", requestID).Code)
	require.Equal(t, 409, b.call(http.MethodPost, "mutations/worker-config", requestID).Code)
}

func TestConsoleEnrollmentRejectsInvalidReviewsWithoutRetainingCredentials(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*jp.WorkerEnrollmentMutation)
	}{
		{"worker path", func(q *jp.WorkerEnrollmentMutation) { q.WorkerID = "company/worker" }},
		{"worker Unicode", func(q *jp.WorkerEnrollmentMutation) { q.WorkerID = "wörker" }},
		{"missing audit subject", func(q *jp.WorkerEnrollmentMutation) { q.Subject = "" }},
		{"audit subject whitespace", func(q *jp.WorkerEnrollmentMutation) { q.Subject = "company worker" }},
		{"audit subject Unicode", func(q *jp.WorkerEnrollmentMutation) { q.Subject = "compañy" }},
		{"audit subject too long", func(q *jp.WorkerEnrollmentMutation) { q.Subject = strings.Repeat("a", 129) }},
		{"enroll existing revision", func(q *jp.WorkerEnrollmentMutation) { q.ExpectedRevision = 1 }},
		{"rotate missing revision", func(q *jp.WorkerEnrollmentMutation) { q.Action, q.Subject = "rotate", "" }},
		{"rotate replaces audit subject", func(q *jp.WorkerEnrollmentMutation) { q.Action, q.ExpectedRevision = "rotate", 1 }},
		{"drain missing revision", func(q *jp.WorkerEnrollmentMutation) { q.Action, q.Subject = "drain", "" }},
		{"resume replaces audit subject", func(q *jp.WorkerEnrollmentMutation) { q.Action, q.ExpectedRevision = "resume", 1 }},
		{"revoke replaces audit subject", func(q *jp.WorkerEnrollmentMutation) { q.Action, q.ExpectedRevision = "revoke", 1 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			b := newBrowserAs(t, Identity{Subject: "platform", Role: "operator"}, func(http.ResponseWriter, *http.Request) { t.Error("preparing an enrollment must not submit it") })
			valid := jp.WorkerEnrollmentMutation{TenantID: "alpha", WorkerID: "company-worker", Action: "enroll", Subject: "company/worker", Reason: "configure company worker"}
			invalid := valid
			test.edit(&invalid)
			reply := b.call(http.MethodPost, "mutations/worker-enrollment", invalid)
			require.Equal(t, 400, reply.Code, reply.Body.String())
			require.NotContains(t, reply.Body.String(), "djobw1.")
			reply = b.call(http.MethodPost, "mutations/worker-enrollment", valid)
			require.Equal(t, 200, reply.Code, "an invalid review must not retain pending mutation state: %s", reply.Body.String())
		})
	}
}

func TestConsoleEnrollmentRejectsScopedWritersAndForeignQueries(t *testing.T) {
	b := newBrowser(t, func(http.ResponseWriter, *http.Request) { t.Error("unauthorized enrollment reached a controller") })
	require.Equal(t, 403, b.call(http.MethodPost, "mutations/worker-enrollment", map[string]string{"tenant_id": "alpha", "worker_id": "one", "action": "enroll"}).Code)
	require.Equal(t, 403, b.call(http.MethodPost, "worker-enrollments/list", map[string]string{"tenant_id": "beta"}).Code)
}

func TestWorkerConfigurationUsesBoundedDistinctBootstrapOrigins(t *testing.T) {
	origins := []string{"https://Controller.example/", "https://controller.example?"}
	for i := 0; i < 14; i++ {
		origins = append(origins, fmt.Sprintf("https://member-%d.example", i))
	}
	s, err := New(Config{Origin: testOrigin, Controllers: origins})
	require.NoError(t, err)
	defer s.Close()
	bootstrap := s.workerOrigins()
	require.Len(t, bootstrap, 8)
	require.Equal(t, "https://controller.example", bootstrap[0])
	require.Equal(t, "https://member-6.example", bootstrap[7])
	require.Equal(t, "https://Controller.example", s.controllers[0].String(), "discovery origins retain their configured identity")
}
