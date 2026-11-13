package db

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/commit"
	"github.com/duravela/djob/pkg/consolesession"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func sessionCreation(at time.Time, id byte) consolesession.Creation {
	return consolesession.Creation{Binding: consolesession.Binding{ID: [32]byte{id}, CredentialHash: [32]byte{2}, SourceHash: [32]byte{3}, ClusterID: 7, Subject: "alice", Role: "writer", TenantID: "alpha"}, CreatedAt: at, ExpiresAt: at.Add(time.Hour), Ciphertext: []byte("ciphertext")}
}

func sessionPolicy() *ConsoleSessionPolicy {
	return &ConsoleSessionPolicy{ClusterID: 7, MaxRecords: 80, MaxCiphertextBytes: 4096, Limits: consolesession.Limits{MaxCiphertextBytes: 1024, MaxLifetime: 24 * time.Hour}}
}

func sessionApply(t *testing.T, q *DB, c ConsoleSessionCommand) ConsoleSessionResult {
	t.Helper()
	version := uint32(ConsoleSessionCommandVersion)
	if c.Action == "configure" {
		version = ConsoleSessionPolicyCommandVersion
	}
	result := q.ApplyEntries([]commit.Entry{logEntry(t, q.LastAppliedIndex()+1, Command{Version: version, Type: CmdConsoleSession, ConsoleSession: &c})})
	require.NoError(t, result.Error)
	require.Len(t, result.ConsoleSessionResults, 1)
	return result.ConsoleSessionResults[0]
}

func TestConsoleSessionStorageActivationSnapshotsAndRevocation(t *testing.T) {
	q, cleanup := NewTestDB(t)
	defer cleanup()
	at := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	c := sessionCreation(at, 1)
	result := sessionApply(t, q, ConsoleSessionCommand{Action: "create", At: at, Creation: &c})
	require.Equal(t, "not_active", result.Rejection.Code)
	result = sessionApply(t, q, ConsoleSessionCommand{Action: "activate", At: at, Policy: sessionPolicy()})
	require.Nil(t, result.Rejection)
	result = sessionApply(t, q, ConsoleSessionCommand{Action: "create", At: at, Creation: &c})
	require.Nil(t, result.Rejection)
	write := ConsoleSessionCommand{Action: "replace", At: at, ID: c.Binding.ID, ExpectedVersion: 1, Mutation: [32]byte{4}, Ciphertext: []byte("submission marker")}
	result = sessionApply(t, q, write)
	require.Nil(t, result.Rejection)
	var snapshot bytes.Buffer
	require.NoError(t, q.Snapshot(&snapshot))
	path := filepath.Join(t.TempDir(), "restored.db")
	restored, err := NewDB(path)
	require.NoError(t, err)
	require.NoError(t, restored.RestoreAt(bytes.NewReader(snapshot.Bytes()), q.LastAppliedIndex()))
	require.NoError(t, restored.Close())
	compacted, err := CompactFile(context.Background(), path, FileCompactionOptions{})
	require.NoError(t, err)
	require.Equal(t, byte(15), compacted.Format)
	restored, err = NewDB(path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, restored.Close()) })
	retried := sessionApply(t, restored, write)
	require.Nil(t, retried.Rejection)
	require.Equal(t, result.Record, retried.Record)
	revoked := sessionApply(t, restored, ConsoleSessionCommand{Action: "revoke", At: at, ID: c.Binding.ID})
	require.Nil(t, revoked.Rejection)
	require.True(t, revoked.Record.Revoked)
	require.Empty(t, revoked.Record.Ciphertext)
	result = sessionApply(t, restored, ConsoleSessionCommand{Action: "create", At: at, Creation: &c})
	require.Equal(t, "revoked", result.Rejection.Code)
	got, err := restored.GetConsoleSession(c.Binding.ID)
	require.NoError(t, err)
	require.Equal(t, revoked.Record, got)
	require.NoError(t, restored.db.View(func(tx *bbolt.Tx) error {
		require.Equal(t, 0, tx.Bucket(requestsBucket).Stats().KeyN, "session edits must not create permanent request receipts")
		require.Equal(t, []byte{15}, tx.Bucket(metadataBucket).Get(formatKey))
		return nil
	}))
}

func TestConsoleSessionAggregateBytesAndImmutablePolicy(t *testing.T) {
	q, cleanup := NewTestDB(t)
	defer cleanup()
	at := time.Now().UTC()
	p := sessionPolicy()
	p.MaxCiphertextBytes, p.Limits.MaxCiphertextBytes = 20, 20
	require.Nil(t, sessionApply(t, q, ConsoleSessionCommand{Action: "activate", At: at, Policy: p}).Rejection)
	require.Nil(t, sessionApply(t, q, ConsoleSessionCommand{Action: "activate", At: at.Add(time.Second), Policy: p}).Rejection)
	changed := *p
	changed.ClusterID++
	require.Equal(t, "conflict", sessionApply(t, q, ConsoleSessionCommand{Action: "activate", At: at, Policy: &changed}).Rejection.Code)
	for id := byte(1); id <= 2; id++ {
		c := sessionCreation(at, id)
		require.Nil(t, sessionApply(t, q, ConsoleSessionCommand{Action: "create", At: at, Creation: &c}).Rejection)
	}
	write := ConsoleSessionCommand{Action: "replace", At: at, ID: [32]byte{1}, ExpectedVersion: 1, Mutation: [32]byte{4}, Ciphertext: []byte("fifteen letters")}
	require.Equal(t, "capacity", sessionApply(t, q, write).Rejection.Code)
	record, err := q.GetConsoleSession([32]byte{1})
	require.NoError(t, err)
	require.Equal(t, uint64(1), record.Version)
	require.Nil(t, sessionApply(t, q, ConsoleSessionCommand{Action: "revoke", At: at, ID: [32]byte{2}}).Rejection)
	require.Nil(t, sessionApply(t, q, write).Rejection)
}

func TestConsoleSessionActivatedFormatRequiresPolicyAndBucket(t *testing.T) {
	for _, missing := range []string{"policy", "bucket"} {
		t.Run(missing, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.db")
			q, err := NewDB(path)
			require.NoError(t, err)
			require.Nil(t, sessionApply(t, q, ConsoleSessionCommand{Action: "activate", At: time.Now().UTC(), Policy: sessionPolicy()}).Rejection)
			require.NoError(t, q.db.Update(func(tx *bbolt.Tx) error {
				if missing == "policy" {
					return tx.Bucket(metadataBucket).Delete(consoleSessionPolicyKey)
				}
				return tx.DeleteBucket(consoleSessionsBucket)
			}))
			require.NoError(t, q.Close())
			_, err = NewDB(path)
			require.ErrorContains(t, err, "missing console session")
		})
	}
}

func TestConsoleSessionCleanupIsBoundedAndRejectsStaleRecreation(t *testing.T) {
	q, cleanup := NewTestDB(t)
	defer cleanup()
	at := time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC)
	require.Nil(t, sessionApply(t, q, ConsoleSessionCommand{Action: "activate", At: at, Policy: sessionPolicy()}).Rejection)
	for id := byte(1); id <= 70; id++ {
		c := sessionCreation(at, id)
		require.Nil(t, sessionApply(t, q, ConsoleSessionCommand{Action: "create", At: at, Creation: &c}).Rejection)
	}
	clean := ConsoleSessionCommand{Action: "cleanup", At: at.Add(time.Hour)}
	require.Equal(t, 64, sessionApply(t, q, clean).Deleted)
	c := sessionCreation(at, 1)
	// Replay the original timestamp after cleanup, rather than giving the retry
	// a fresh timestamp. The durable cutoff must still reject it.
	result := sessionApply(t, q, ConsoleSessionCommand{Action: "create", At: at, Creation: &c})
	require.Equal(t, "expired", result.Rejection.Code)
	require.Equal(t, 6, sessionApply(t, q, clean).Deleted)
	require.Equal(t, 0, sessionApply(t, q, clean).Deleted)
}

func TestConsoleSessionCapacityStillAllowsRevocation(t *testing.T) {
	q, cleanup := NewTestDB(t)
	defer cleanup()
	at := time.Now().UTC()
	p := sessionPolicy()
	p.MaxRecords, p.MaxCiphertextBytes, p.Limits.MaxCiphertextBytes = 1, 10, 10
	require.Nil(t, sessionApply(t, q, ConsoleSessionCommand{Action: "activate", At: at, Policy: p}).Rejection)
	c := sessionCreation(at, 1)
	require.Nil(t, sessionApply(t, q, ConsoleSessionCommand{Action: "create", At: at, Creation: &c}).Rejection)
	other := sessionCreation(at, 2)
	require.Equal(t, "capacity", sessionApply(t, q, ConsoleSessionCommand{Action: "create", At: at, Creation: &other}).Rejection.Code)
	require.Nil(t, sessionApply(t, q, ConsoleSessionCommand{Action: "revoke", At: at, ID: c.Binding.ID}).Rejection)
	// Tombstones still count towards capacity until expiry.
	require.Equal(t, "capacity", sessionApply(t, q, ConsoleSessionCommand{Action: "create", At: at, Creation: &other}).Rejection.Code)
}

func TestConsoleSessionVersionGatingAndBatchRollback(t *testing.T) {
	q, cleanup := NewTestDB(t)
	defer cleanup()
	at := time.Now().UTC()
	c := ConsoleSessionCommand{Action: "activate", At: at, Policy: sessionPolicy()}
	result := q.ApplyEntries([]commit.Entry{logEntry(t, 1, Command{Version: WorkerDeploymentCommandVersion, Type: CmdConsoleSession, ConsoleSession: &c})})
	require.Error(t, result.Error)
	require.Zero(t, q.LastAppliedIndex())
	result = q.ApplyEntries([]commit.Entry{
		logEntry(t, 1, Command{Version: ConsoleSessionCommandVersion, Type: CmdConsoleSession, ConsoleSession: &c}),
		logEntry(t, 2, Command{Type: CmdState}),
	})
	require.Error(t, result.Error)
	require.Zero(t, q.LastAppliedIndex())
	require.NoError(t, q.db.View(func(tx *bbolt.Tx) error {
		require.Nil(t, tx.Bucket(consoleSessionsBucket))
		require.Nil(t, tx.Bucket(metadataBucket).Get(consoleSessionPolicyKey))
		require.NotEqual(t, []byte{15}, tx.Bucket(metadataBucket).Get(formatKey))
		return nil
	}))
}

func TestConsoleSessionRecoveryInvalidatesOldBindingsAndAllowsFreshLogin(t *testing.T) {
	q, cleanup := NewTestDB(t)
	defer cleanup()
	at := time.Now().UTC()
	policy := sessionPolicy()
	require.Nil(t, sessionApply(t, q, ConsoleSessionCommand{Action: "activate", At: at, Policy: policy}).Rejection)
	old := sessionCreation(at, 1)
	require.Nil(t, sessionApply(t, q, ConsoleSessionCommand{Action: "create", At: at, Creation: &old}).Rejection)
	next := *policy
	next.ClusterID = 8
	recover := ConsoleSessionCommand{Action: "recover", At: at, Policy: &next, PreviousClusterID: 7}
	require.Nil(t, sessionApply(t, q, recover).Rejection)
	require.Nil(t, sessionApply(t, q, recover).Rejection, "recovery retry must acknowledge the same policy")
	require.Equal(t, "invalid", sessionApply(t, q, ConsoleSessionCommand{Action: "replace", At: at, ID: old.Binding.ID, ExpectedVersion: 1, Mutation: [32]byte{5}, Ciphertext: []byte("new")}).Rejection.Code)
	fresh := sessionCreation(at, 2)
	fresh.Binding.ClusterID = 8
	require.Nil(t, sessionApply(t, q, ConsoleSessionCommand{Action: "create", At: at, Creation: &fresh}).Rejection)
	require.Equal(t, 1, sessionApply(t, q, ConsoleSessionCommand{Action: "cleanup", At: at}).Deleted)
	_, err := q.GetConsoleSession(old.Binding.ID)
	require.Error(t, err)
	_, err = q.GetConsoleSession(fresh.Binding.ID)
	require.NoError(t, err)
}
