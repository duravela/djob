package db

import (
	"context"
	"github.com/duravela/djob/pkg/commit"
	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
	"testing"
	"time"
)

func TestOIDCLogoutCutoffsPersistAndDoNotRevokeLaterLogin(t *testing.T) {
	h := newChannelHarness(t)
	c := ConsoleSessionCommand{Action: "oidc_logout", At: h.at, Logout: &OIDCLogout{Issuer: "https://id.example", Subject: "alice", IssuedAt: h.at}}
	require.NoError(t, h.db.db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(oidcLogoutBucket)
		if err != nil {
			return err
		}
		_, err = applyConsoleSession(tx, c)
		return err
	}))
	revoked, err := h.db.OIDCRevoked(context.Background(), c.Logout.Issuer, "alice", "", h.at)
	require.NoError(t, err)
	require.True(t, revoked)
	revoked, err = h.db.OIDCRevoked(context.Background(), c.Logout.Issuer, "alice", "", h.at.Add(time.Second))
	require.NoError(t, err)
	require.False(t, revoked)
	revoked, err = h.db.OIDCRevoked(context.Background(), c.Logout.Issuer, "bob", "", h.at)
	require.NoError(t, err)
	require.False(t, revoked)
}

func TestContinuityCommandsRequireNewVersionEvenWhenDurablyRejected(t *testing.T) {
	h := newChannelHarness(t)
	c := ConsoleSessionCommand{Action: "oidc_logout", At: h.at}
	result := h.db.ApplyEntries([]commit.Entry{logEntry(t, h.index+1, Command{Version: OIDCSessionCommandVersion, Type: CmdConsoleSession, ConsoleSession: &c})})
	require.ErrorContains(t, result.Error, "requires version 26")
	result = h.db.ApplyEntries([]commit.Entry{logEntry(t, h.index+1, Command{Version: OIDCContinuityCommandVersion, Type: CmdConsoleSession, ConsoleSession: &c})})
	require.NoError(t, result.Error)
	require.Len(t, result.ConsoleSessionResults, 1)
	require.Equal(t, "invalid", result.ConsoleSessionResults[0].Rejection.Code)
	require.NoError(t, h.db.db.View(func(tx *bbolt.Tx) error {
		require.Equal(t, []byte{19}, tx.Bucket(metadataBucket).Get(formatKey))
		require.NotNil(t, tx.Bucket(oidcLogoutBucket))
		return nil
	}))
}
