package node

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/base"
	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/node/config"
	"github.com/duravela/djob/pkg/node/util/testutil"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

func startReadBarrierCluster(t *testing.T, useTLS bool) ([]*durableNode, []config.Config) {
	t.Helper()
	peers := make([]string, 3)
	scheme := "http://"
	var ca *testutil.PeerCA
	if useTLS {
		scheme = "https://"
		ca = testutil.NewPeerCA(t)
	}
	for i := range peers {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		peers[i] = scheme + listener.Addr().String()
		require.NoError(t, listener.Close())
	}
	nodes := make([]*durableNode, 3)
	configs := make([]config.Config, 3)
	for i := range nodes {
		cfg := crashTestConfig(t.TempDir())
		cfg.ID = uint64(i + 1)
		cfg.Peers = peers
		cfg.SnapshotCount, cfg.CatchUpEntriesN = 2, 1
		cfg.ProposeC = make(chan []byte)
		cfg.ConfChangeC = make(chan raftpb.ConfChange)
		if ca != nil {
			cfg.PeerTLS = ca.Member(t, []string{"127.0.0.1"})
		}
		configs[i] = cfg
		nodes[i] = startRaftApplication(t, cfg)
	}
	for _, n := range nodes {
		_, err := n.raft.WaitForLeader(3 * time.Second)
		require.NoError(t, err)
	}
	return nodes, configs
}
func requireReadBarrier(t *testing.T, n *durableNode, minimum uint64) ReadBarrierResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var result ReadBarrierResult
	require.Eventually(t, func() bool { var err error; result, err = n.raft.ReadBarrier(ctx); return err == nil }, 3*time.Second, time.Millisecond)
	require.GreaterOrEqual(t, result.ConfirmedIndex, minimum)
	require.GreaterOrEqual(t, result.AppliedIndex, result.ConfirmedIndex)
	require.GreaterOrEqual(t, n.store.LastAppliedIndex(), result.AppliedIndex)
	return result
}
func TestReadBarriersConfirmLeaderAndFollowersAcrossTLSFailover(t *testing.T) {
	nodes, _ := startReadBarrierCluster(t, true)
	genericProposal(t, nodes[0], "readiness-tenant", db.JobCommand{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: "tenant"}}})
	minimum := nodes[0].store.LastAppliedIndex()
	for _, n := range nodes {
		requireReadBarrier(t, n, minimum)
	}
	before := nodes[0].store.LastAppliedIndex()
	for range 5 {
		for _, n := range nodes {
			requireReadBarrier(t, n, before)
		}
	}
	leader := -1
	for i, n := range nodes {
		if n.raft.IsLeader() {
			leader = i
		}
	}
	require.NotEqual(t, -1, leader)
	nodes[leader].close()
	require.Eventually(t, func() bool {
		for i, n := range nodes {
			if i != leader && n.raft.IsLeader() {
				return true
			}
		}
		return false
	}, 3*time.Second, 5*time.Millisecond)
	for i, n := range nodes {
		if i != leader {
			requireReadBarrier(t, n, before)
		}
	}
}

func TestReadBarrierOutageCapacityStaysBoundedAndRecovers(t *testing.T) {
	nodes, configs := startReadBarrierCluster(t, false)
	leader := -1
	for i, n := range nodes {
		if n.raft.IsLeader() {
			leader = i
		}
	}
	require.NotEqual(t, -1, leader)
	n := nodes[leader]
	requireReadBarrier(t, n, 1)
	for i, node := range nodes {
		if i != leader {
			node.close()
		}
	}
	require.True(t, n.raft.IsLeader(), "the stale local role alone cannot prove quorum")
	var wg sync.WaitGroup
	errs := make([]error, MaxPendingReadBarriers)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()
			_, errs[i] = n.raft.ReadBarrier(ctx)
		}(i)
	}
	wg.Wait()
	for _, err := range errs {
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}
	n.raft.readBarriers.mu.Lock()
	pending := len(n.raft.readBarriers.pending)
	n.raft.readBarriers.mu.Unlock()
	require.Equal(t, MaxPendingReadBarriers, pending)
	_, err := n.raft.ReadBarrier(context.Background())
	require.ErrorIs(t, err, ErrReadBarrierBusy)
	require.True(t, n.raft.IsLeader())
	for i := range nodes {
		if i != leader {
			cfg := configs[i]
			cfg.ProposeC = make(chan []byte)
			cfg.ConfChangeC = make(chan raftpb.ConfChange)
			nodes[i] = startRaftApplication(t, cfg)
		}
	}
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, err := n.raft.ReadBarrier(ctx)
		return err == nil
	}, 5*time.Second, 10*time.Millisecond)
	requireReadBarrier(t, n, 1)
}

func TestReadBarrierWaitsForApplicationAcknowledgement(t *testing.T) {
	cfg := crashTestConfig(t.TempDir())
	cfg.ProposeC = make(chan []byte)
	cfg.ConfChangeC = make(chan raftpb.ConfChange)
	n := NewRaftNode(cfg)
	t.Cleanup(n.Stop)
	blocked := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	go func() {
		for c := range n.CommitC {
			for _, entry := range c.Entries {
				if len(entry.Data) > 0 {
					close(blocked)
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
	require.NoError(t, n.node.Propose(ctx, []byte("held application commit")))
	select {
	case <-blocked:
	case <-ctx.Done():
		t.Fatal("application did not receive committed entry")
	}
	done := make(chan readBarrierReply, 1)
	go func() { result, err := n.ReadBarrier(ctx); done <- readBarrierReply{result: result, err: err} }()
	select {
	case result := <-done:
		t.Fatalf("read barrier passed before application acknowledgement: %+v", result)
	case <-time.After(30 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case result := <-done:
		require.NoError(t, result.err)
		require.GreaterOrEqual(t, result.result.AppliedIndex, result.result.ConfirmedIndex)
	case <-ctx.Done():
		t.Fatal("read barrier did not resume after application acknowledgement")
	}
}

func TestReadBarrierMatchesNonceAndWaitsForConfirmedIndex(t *testing.T) {
	a := &pendingReadBarrier{done: make(chan readBarrierReply, 1)}
	b := &pendingReadBarrier{done: make(chan readBarrierReply, 1)}
	n := &RaftNode{appliedIndex: 5, readBarriers: readBarrierState{pending: map[string]*pendingReadBarrier{"a": a, "b": b}}}
	n.completeReadBarriers([]raft.ReadState{{Index: 7, RequestCtx: []byte("a")}, {Index: 100, RequestCtx: []byte("unknown")}, {Index: 6, RequestCtx: []byte("a")}})
	select {
	case <-a.done:
		t.Fatal("confirmed index was not applied")
	default:
	}
	n.appliedIndex = 7
	n.completeReadBarriers([]raft.ReadState{{Index: 7, RequestCtx: []byte("b")}})
	for _, p := range []*pendingReadBarrier{a, b} {
		select {
		case reply := <-p.done:
			require.NoError(t, reply.err)
			require.Equal(t, ReadBarrierResult{ConfirmedIndex: 7, AppliedIndex: 7}, reply.result)
		default:
			t.Fatal("applied barrier did not complete")
		}
	}
	require.Empty(t, n.readBarriers.pending)
}

func TestReadBarrierCancellationStopAndUnsafeModeFailClosed(t *testing.T) {
	n := startDurableNode(t, t.TempDir(), 2)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := n.raft.ReadBarrier(ctx)
	require.ErrorIs(t, err, context.Canceled)
	n.close()
	_, err = n.raft.ReadBarrier(context.Background())
	require.ErrorIs(t, err, ErrReadBarrierUnavailable)
	require.Empty(t, n.raft.readBarriers.pending)
	cfg := crashTestConfig(filepath.Join(t.TempDir(), "unopened"))
	cfg.ReadOnlyOption = raft.ReadOnlyLeaseBased
	cfg.CheckQuorum = true
	require.ErrorContains(t, ValidateApplicationConfig(cfg), "ReadOnlySafe")
	unsafe := &RaftNode{raftCfg: &base.RaftConfig{Config: &raft.Config{ReadOnlyOption: raft.ReadOnlyLeaseBased}}}
	_, err = unsafe.ReadBarrier(context.Background())
	require.True(t, errors.Is(err, ErrReadBarrierUnsafe))
}

func TestReadBarriersDoNotAppendProbeEntriesOrReceipts(t *testing.T) {
	n := startDurableNode(t, t.TempDir(), 2)
	requireReadBarrier(t, n, 1) // Include the election no-op before measuring probe writes.
	before := n.store.LastAppliedIndex()
	for range 8 {
		requireReadBarrier(t, n, before)
	}
	require.Equal(t, before, n.store.LastAppliedIndex())
}
