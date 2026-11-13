package store

import (
	"context"
	"github.com/duravela/djob/pkg/commit"
	"github.com/duravela/djob/pkg/consolesession"
	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestConsoleSessionWaitersMatchApplicationAndCleanUp(t *testing.T) {
	s, proposals, commits := newTestJobQueue(t)
	c := db.ConsoleSessionCommand{Action: "activate", At: time.Now().UTC(), Policy: &db.ConsoleSessionPolicy{ClusterID: 7, MaxRecords: 8, MaxCiphertextBytes: 4096, Limits: consolesession.Limits{MaxCiphertextBytes: 1024, MaxLifetime: time.Hour}}}
	replies := make(chan error, 1)
	go func() { _, err := s.ProposeConsoleSession(context.Background(), c); replies <- err }()
	data := receiveProposal(t, proposals)
	applyCommit(t, commits, &commit.Commit{Entries: []commit.Entry{{Index: 3, Data: data}}})
	require.NoError(t, <-replies)
	policy, err := s.ConsoleSessionPolicy()
	require.NoError(t, err)
	require.Equal(t, uint64(7), policy.ClusterID)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_, err := s.ProposeConsoleSession(ctx, db.ConsoleSessionCommand{Action: "cleanup", At: c.At})
		replies <- err
	}()
	_ = receiveProposal(t, proposals)
	cancel()
	require.ErrorIs(t, <-replies, context.Canceled)
	s.mu.RLock()
	require.Empty(t, s.pendingSessions)
	s.mu.RUnlock()
}
