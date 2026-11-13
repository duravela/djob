package node

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

func TestLocalStatusRemainsAvailableDuringApplicationStall(t *testing.T) {
	cfg := crashTestConfig(t.TempDir())
	cfg.ProposeC, cfg.ConfChangeC = make(chan []byte), make(chan raftpb.ConfChange)
	n := NewRaftNode(cfg)
	t.Cleanup(n.Stop)
	blocked := make(chan uint64, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	go func() {
		for c := range n.CommitC {
			for _, entry := range c.Entries {
				if len(entry.Data) != 0 {
					blocked <- entry.Index
					<-release
				}
			}
			close(c.ApplyDoneC)
		}
	}()
	_, err := n.WaitForLeader(3 * time.Second)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = n.ReadBarrier(ctx) // Apply the current-term no-op before the stall.
	require.NoError(t, err)
	require.NoError(t, n.node.Propose(ctx, []byte("hold durable application acknowledgement")))
	var index uint64
	select {
	case index = <-blocked:
	case <-ctx.Done():
		t.Fatal("application did not receive committed work")
	}
	for range 100 {
		observed := n.LocalStatus()
		require.True(t, observed.Running)
		require.True(t, observed.Leader)
		require.True(t, observed.LeaderKnown)
		require.Positive(t, observed.Term)
		require.GreaterOrEqual(t, observed.CommitIndex, index)
		require.Less(t, observed.AppliedIndex, index, "commit cannot masquerade as durable application")
	}
	releaseOnce.Do(func() { close(release) })
	require.Eventually(t, func() bool { return n.LocalStatus().AppliedIndex >= index }, time.Second, time.Millisecond)
	n.Stop()
	observed := n.LocalStatus()
	require.False(t, observed.Running)
	require.False(t, observed.Leader)
	require.False(t, observed.LeaderKnown)
	require.GreaterOrEqual(t, observed.CommitIndex, observed.AppliedIndex)
}

func TestLocalStatusIncludesAcknowledgedSnapshotsAfterRestart(t *testing.T) {
	dir := t.TempDir()
	n := startDurableNode(t, dir, 2)
	for i := range 5 {
		id := fmt.Sprintf("gateway/%d", i)
		proposeChannel(t, n, db.StateCommand{RequestID: fmt.Sprintf("status/%d", i), At: time.Now().UTC(),
			Configure: &db.ConfigureChannel{ID: id, GatewayID: id, Protocol: "fake", Endpoint: "private-endpoint:502"}})
	}
	index := n.store.LastAppliedIndex()
	require.Eventually(t, func() bool {
		s := n.raft.LocalStatus()
		return s.AppliedIndex >= index && s.SnapshotIndex > 0 && s.SnapshotIndex <= s.AppliedIndex
	}, 3*time.Second, time.Millisecond)
	n.close()
	restarted := startDurableNode(t, dir, 2)
	require.Eventually(t, func() bool {
		s := restarted.raft.LocalStatus()
		return s.Running && s.CommitIndex >= s.AppliedIndex && s.AppliedIndex >= index && s.SnapshotIndex > 0
	}, 3*time.Second, time.Millisecond)
}
