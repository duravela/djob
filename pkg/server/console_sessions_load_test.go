package server

import (
	"crypto/sha256"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/consolesession"
	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
)

func TestConsoleSessionWriteFloodLeavesJobCoordinationAvailable(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	cfg.ConsoleSessions = true
	s := startAPI(t, cfg)
	activateControllerSessions(t, s)
	client := genericClient(t, s)
	tenantAPI(t, client, "alpha")
	definitionAPI(t, client, "alpha", "compute", "sha256", "", []byte(`{}`))
	var accepted, throttled atomic.Int32
	var workers sync.WaitGroup
	start := make(chan struct{})
	for range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for range 12 {
				token, err := consolesession.NewCredential()
				require.NoError(t, err)
				id, _, err := consolesession.ParseCredential(token)
				require.NoError(t, err)
				at := time.Now().UTC()
				c := consolesession.Creation{Binding: consolesession.Binding{ID: id, CredentialHash: sha256.Sum256([]byte(token)), ClusterID: s.application.Raft.ClusterID()}, CreatedAt: at, ExpiresAt: at.Add(time.Hour), Ciphertext: make([]byte, 1024)}
				response := sessionHTTP(t, s, genericToken("alpha/writer"), "create", db.ConsoleSessionCommand{Creation: &c})
				response.Body.Close()
				switch response.StatusCode {
				case 200:
					accepted.Add(1)
				case 429:
					throttled.Add(1)
				default:
					t.Errorf("unexpected flood response %d", response.StatusCode)
				}
			}
		}()
	}
	close(start)
	// Ordinary job authorization does not consume the session probe/write budget.
	before := time.Now()
	launchAPI(t, client, "alpha", "compute", "during-session-flood")
	require.Less(t, time.Since(before), 3*time.Second)
	workers.Wait()
	require.Positive(t, accepted.Load())
	require.Positive(t, throttled.Load())
}
