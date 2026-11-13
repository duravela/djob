package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/consolesession"
	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func createControllerSession(t *testing.T, s *Server, ttl time.Duration) string {
	t.Helper()
	token, err := consolesession.NewCredential()
	require.NoError(t, err)
	id, _, err := consolesession.ParseCredential(token)
	require.NoError(t, err)
	at := time.Now().UTC()
	creation := consolesession.Creation{Binding: consolesession.Binding{ID: id, CredentialHash: sha256.Sum256([]byte(token)), ClusterID: s.application.Raft.ClusterID()}, CreatedAt: at, ExpiresAt: at.Add(ttl), Ciphertext: []byte("opaque encrypted review")}
	response := sessionHTTP(t, s, genericToken("alpha/writer"), "create", db.ConsoleSessionCommand{Creation: &creation})
	data, _ := io.ReadAll(response.Body)
	response.Body.Close()
	require.Equal(t, 200, response.StatusCode, string(data))
	return token
}

func activateControllerSessions(t *testing.T, s *Server) {
	t.Helper()
	// Start may return before a follower has learned its leader's read path.
	// Activation requires quorum, so establish readiness instead of racing it.
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_, err := s.application.Raft.ReadBarrier(ctx)
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
	policy := &db.ConsoleSessionPolicy{MaxRecords: 128, MaxCiphertextBytes: 64 << 20, Limits: consolesession.Limits{MaxCiphertextBytes: 4 << 20, MaxLifetime: 24 * time.Hour}}
	response := sessionHTTP(t, s, genericToken("ops/alice"), "activate", db.ConsoleSessionCommand{Policy: policy})
	data, _ := io.ReadAll(response.Body)
	response.Body.Close()
	require.Equal(t, 200, response.StatusCode, string(data))
}

func TestDelegatedSessionScopeSourceBindingAndQuorumLoss(t *testing.T) {
	members := startReadinessServers(t, func(c *Config) {
		c.ConsoleSessions = true
		c.Admission.Disabled = true
		if *c.NodeID == 1 {
			// Same source token with changed privilege must invalidate its old binding.
			for i := range c.APIAuth.Credentials {
				if c.APIAuth.Credentials[i].Subject == "alpha/writer" {
					c.APIAuth.Credentials[i].Role = APIReader
				}
			}
		}
	})
	var changed, normal *Server
	for _, s := range members {
		if s.nodeID == 1 {
			changed = s
		} else {
			normal = s
		}
	}
	require.NotNil(t, changed)
	require.NotNil(t, normal)
	activateControllerSessions(t, normal)
	token := createControllerSession(t, normal, time.Hour)
	response := sessionHTTP(t, normal, token, "read", db.ConsoleSessionCommand{})
	response.Body.Close()
	require.Equal(t, 200, response.StatusCode)
	request, err := http.NewRequest(http.MethodPost, "http://"+normal.HTTPAddr()+"/api/jobs/v1/get", bytes.NewBufferString(`{"reference":{"kind":"TENANT","tenant_id":"beta"}}`))
	require.NoError(t, err)
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	foreign, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	foreign.Body.Close()
	require.Equal(t, 403, foreign.StatusCode, "delegation must preserve tenant scope")
	response = sessionHTTP(t, changed, token, "read", db.ConsoleSessionCommand{})
	response.Body.Close()
	require.Equal(t, 401, response.StatusCode, "source role change must invalidate a session")
	response = sessionHTTP(t, normal, token, "revoke", db.ConsoleSessionCommand{ID: [32]byte{99}})
	response.Body.Close()
	require.Equal(t, 403, response.StatusCode)
	response = sessionHTTP(t, normal, token, "activate", db.ConsoleSessionCommand{})
	response.Body.Close()
	require.Equal(t, 401, response.StatusCode)
	// No quorum must return unavailability, rather than an expired/absent session.
	for _, s := range members {
		if s != normal {
			require.NoError(t, s.Close(context.Background()))
		}
	}
	response = sessionHTTP(t, normal, token, "read", db.ConsoleSessionCommand{})
	response.Body.Close()
	require.Equal(t, 503, response.StatusCode)
}

type unreadSessionBody struct{ read bool }

func (b *unreadSessionBody) Read(p []byte) (int, error) { b.read = true; return 0, io.EOF }
func (b *unreadSessionBody) Close() error               { return nil }

func TestConsoleSessionByteAdmissionPrecedesBodyRead(t *testing.T) {
	for _, length := range []int64{-1, 7 << 20, 128} {
		h := newConsoleSessions(nil)
		h.byteBudget = rate.NewLimiter(0, 0)
		body := &unreadSessionBody{}
		request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/api/jobs/v1/console-sessions/read", nil)
		request.Body = body
		request.ContentLength = length
		request.SetPathValue("action", "read")
		response := httptest.NewRecorder()
		h.http(response, request)
		require.Equal(t, 429, response.Code)
		require.False(t, body.read)
	}
}

func TestConsoleSessionReadsDoNotAppendLogEntries(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	cfg.ConsoleSessions = true
	s := startAPI(t, cfg)
	activateControllerSessions(t, s)
	token := createControllerSession(t, s, time.Hour)
	before := s.application.Store.LastAppliedIndex()
	for range 5 {
		response := sessionHTTP(t, s, token, "read", db.ConsoleSessionCommand{})
		_, err := io.Copy(io.Discard, response.Body)
		response.Body.Close()
		require.NoError(t, err)
		require.Equal(t, 200, response.StatusCode)
	}
	require.Equal(t, before, s.application.Store.LastAppliedIndex())
}

func TestDelegatedSessionExpiryAndWriteAdmission(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	cfg.ConsoleSessions = true
	cfg.Admission.Disabled = true
	s := startAPI(t, cfg)
	activateControllerSessions(t, s)
	token := createControllerSession(t, s, time.Second)
	time.Sleep(1100 * time.Millisecond)
	response := sessionHTTP(t, s, token, "read", db.ConsoleSessionCommand{})
	response.Body.Close()
	require.Equal(t, 401, response.StatusCode)
	before := s.application.Store.LastAppliedIndex()
	s.jobs.sessions.writes = rate.NewLimiter(0, 0)
	response = sessionHTTP(t, s, genericToken("alpha/writer"), "create", db.ConsoleSessionCommand{})
	var body map[string]any
	_ = json.NewDecoder(response.Body).Decode(&body)
	response.Body.Close()
	require.Equal(t, 429, response.StatusCode)
	require.Equal(t, before, s.application.Store.LastAppliedIndex(), "rejected requests must not reach Raft")
}
