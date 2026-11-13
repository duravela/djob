package server

import (
	"context"
	"io"
	"path/filepath"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/consolesession"
	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/node"
	"github.com/stretchr/testify/require"
)

func TestSharedSessionsPreserveMemberRestartButRejectPreLogoutBackup(t *testing.T) {
	directory := t.TempDir()
	configuration := func(path string) Config {
		c := apiTestConfig(path)
		c.APIAuth = genericCredentials()
		c.ConsoleSessions = true
		return c
	}
	original := startAPI(t, configuration(directory))
	activateControllerSessions(t, original)
	token := createControllerSession(t, original, time.Hour)
	require.NoError(t, original.Close(context.Background()))
	artifact := filepath.Join(t.TempDir(), "backup")
	manifest, err := node.BackupMember(context.Background(), directory, artifact, 1)
	require.NoError(t, err)
	// An ordinary restart uses the same cluster identity and preserves the session.
	restarted := startAPI(t, configuration(directory))
	response := sessionHTTP(t, restarted, token, "read", db.ConsoleSessionCommand{})
	response.Body.Close()
	require.Equal(t, 200, response.StatusCode)
	id, _, err := consolesession.ParseCredential(token)
	require.NoError(t, err)
	response = sessionHTTP(t, restarted, token, "revoke", db.ConsoleSessionCommand{ID: id})
	response.Body.Close()
	require.Equal(t, 200, response.StatusCode)
	require.NoError(t, restarted.Close(context.Background()))
	// This artifact predates the acknowledged logout and therefore still has an
	// active record. Its old cluster binding must never authenticate after restore.
	recoveredPath := filepath.Join(t.TempDir(), "recovered")
	_, err = node.RecoverCluster(context.Background(), artifact, recoveredPath, 1, []string{"http://127.0.0.1:0"}, true)
	require.NoError(t, err)
	recovered := startAPI(t, configuration(recoveredPath))
	response = sessionHTTP(t, recovered, token, "read", db.ConsoleSessionCommand{})
	response.Body.Close()
	require.Equal(t, 401, response.StatusCode)
	policy := &db.ConsoleSessionPolicy{MaxRecords: 128, MaxCiphertextBytes: 64 << 20, Limits: consolesession.Limits{MaxCiphertextBytes: 4 << 20, MaxLifetime: 24 * time.Hour}}
	response = sessionHTTP(t, recovered, genericToken("ops/alice"), "recover", db.ConsoleSessionCommand{Policy: policy, PreviousClusterID: manifest.Identity.ClusterID})
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	require.Equal(t, 200, response.StatusCode, string(body))
	require.NoError(t, recovered.Close(context.Background()))
	require.NoError(t, node.ReleaseRecovery(context.Background(), recoveredPath, 1, "session recovery has no executable work; old cluster fenced"))
	ready := startAPI(t, configuration(recoveredPath))
	_ = createControllerSession(t, ready, time.Hour)
	response = sessionHTTP(t, ready, token, "read", db.ConsoleSessionCommand{})
	response.Body.Close()
	require.Equal(t, 401, response.StatusCode)
}
