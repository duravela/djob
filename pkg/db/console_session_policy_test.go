package db

import (
	"testing"
	"time"

	"github.com/duravela/djob/pkg/commit"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func TestSessionPolicyUpdateIsVersionedBoundedAndPreservesExistingExpiry(t *testing.T) {
	q, cleanup := NewTestDB(t)
	defer cleanup()
	at := time.Now().UTC()
	require.Nil(t, sessionApply(t, q, ConsoleSessionCommand{Action: "activate", At: at, Policy: sessionPolicy()}).Rejection)
	c := sessionCreation(at, 1)
	require.Nil(t, sessionApply(t, q, ConsoleSessionCommand{Action: "create", At: at, Creation: &c}).Rejection)
	policy := sessionPolicy()
	policy.MaxRecords = 1
	policy.MaxCiphertextBytes = 10
	policy.Limits.MaxCiphertextBytes = 10
	policy.Limits.MaxLifetime = time.Minute
	change := ConsoleSessionCommand{Action: "configure", At: at, Policy: policy, ExpectedPolicyRevision: 1}
	failed := q.ApplyEntries([]commit.Entry{logEntry(t, q.LastAppliedIndex()+1, Command{Version: ConsoleSessionCommandVersion, Type: CmdConsoleSession, ConsoleSession: &change})})
	require.Error(t, failed.Error)
	require.Nil(t, sessionApply(t, q, change).Rejection)
	require.Nil(t, sessionApply(t, q, change).Rejection)
	current, err := q.ConsoleSessionPolicy()
	require.NoError(t, err)
	require.Equal(t, uint64(2), current.Revision)
	// A lower lifetime applies to new sessions; exact creation retries retain
	// the original expiry even after the policy changes.
	retried := sessionApply(t, q, ConsoleSessionCommand{Action: "create", At: at, Creation: &c})
	require.Nil(t, retried.Rejection)
	require.Equal(t, c.ExpiresAt, retried.Record.ExpiresAt)
	next := *policy
	next.MaxCiphertextBytes = 9
	next.Limits.MaxCiphertextBytes = 9
	require.Equal(t, "capacity", sessionApply(t, q, ConsoleSessionCommand{Action: "configure", At: at, Policy: &next, ExpectedPolicyRevision: 2}).Rejection.Code)
	require.NoError(t, q.db.View(func(tx *bbolt.Tx) error {
		require.Equal(t, []byte{16}, tx.Bucket(metadataBucket).Get(formatKey))
		return nil
	}))
}
