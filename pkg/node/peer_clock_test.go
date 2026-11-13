package node

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/base"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
)

func TestPeerClockRequiresQuorumIdentityAndBoundedUncertainty(t *testing.T) {
	var offsets [2]atomic.Int64
	var badIdentity atomic.Bool
	peers := []string{"http://local.invalid"}
	for i := range 2 {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := uint64(i + 2)
			if badIdentity.Load() {
				id = 99
			}
			_ = json.NewEncoder(w).Encode(peerClockReply{NodeID: id, ClusterID: 42, At: time.Now().Add(time.Duration(offsets[i].Load())).UTC()})
		}))
		t.Cleanup(server.Close)
		peers = append(peers, server.URL)
	}
	n := &RaftNode{raftCfg: &base.RaftConfig{Config: &raft.Config{ID: 1}, Peers: peers, ClusterID: 42}, clockNow: time.Now, peerClockClient: &http.Client{Timeout: time.Second}}
	require.True(t, n.PeerClockWithin(context.Background(), time.Second))
	offsets[0].Store(int64(time.Hour))
	require.True(t, n.PeerClockWithin(context.Background(), time.Second), "one healthy peer plus self establishes the majority")
	offsets[1].Store(int64(-time.Hour))
	require.False(t, n.PeerClockWithin(context.Background(), time.Second))
	offsets[0].Store(0)
	offsets[1].Store(0)
	badIdentity.Store(true)
	require.False(t, n.PeerClockWithin(context.Background(), time.Second))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.False(t, n.PeerClockWithin(ctx, time.Second))
}

func TestPeerClockHealthyMajorityCancelsAndJoinsBlackholedMinority(t *testing.T) {
	started, finished := make(chan struct{}), make(chan struct{})
	blocked := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(started); <-r.Context().Done(); close(finished) }))
	defer blocked.Close()
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-started
		_ = json.NewEncoder(w).Encode(peerClockReply{NodeID: 2, ClusterID: 42, At: time.Now().UTC()})
	}))
	defer healthy.Close()
	n := &RaftNode{raftCfg: &base.RaftConfig{Config: &raft.Config{ID: 1}, Peers: []string{"http://local.invalid", healthy.URL, blocked.URL}, ClusterID: 42}, clockNow: time.Now, peerClockClient: &http.Client{Timeout: 10 * time.Second}}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require.True(t, n.PeerClockWithin(ctx, time.Second), "an unreachable minority must not hold a confirmed healthy majority until its parent deadline")
	require.NoError(t, ctx.Err())
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("minority probe was not cancelled")
	}
}
