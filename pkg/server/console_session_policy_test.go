package server

import (
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
)

func TestRuntimeSessionPolicyRequiresOriginalOperatorAndCurrentRevision(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	cfg.ConsoleSessions = true
	s := startAPI(t, cfg)
	activateControllerSessions(t, s)
	token := createControllerSession(t, s, time.Hour)
	_ = createControllerSession(t, s, time.Hour)
	policy, err := s.application.Store.ConsoleSessionPolicy()
	require.NoError(t, err)
	require.Equal(t, uint64(1), policy.Revision)
	policy.Revision = 0
	policy.Cutoff = time.Time{}
	policy.MaxRecords = 256
	change := db.ConsoleSessionCommand{Policy: policy, ExpectedPolicyRevision: 1}
	response := sessionHTTP(t, s, genericToken("alpha/writer"), "configure", change)
	response.Body.Close()
	require.Equal(t, 403, response.StatusCode)
	response = sessionHTTP(t, s, genericToken("ops/alice"), "configure", change)
	response.Body.Close()
	require.Equal(t, 200, response.StatusCode)
	response = sessionHTTP(t, s, genericToken("ops/alice"), "configure", change)
	response.Body.Close()
	require.Equal(t, 200, response.StatusCode)
	response = sessionHTTP(t, s, token, "read", db.ConsoleSessionCommand{})
	response.Body.Close()
	require.Equal(t, 200, response.StatusCode)
	policy.MaxRecords = 1
	change.ExpectedPolicyRevision = 2
	response = sessionHTTP(t, s, genericToken("ops/alice"), "configure", change)
	response.Body.Close()
	require.Equal(t, 429, response.StatusCode, "lowering limits must not discard retained sessions")
	current, err := s.application.Store.ConsoleSessionPolicy()
	require.NoError(t, err)
	require.Equal(t, uint64(2), current.Revision)
	require.Equal(t, 256, current.MaxRecords)
}
