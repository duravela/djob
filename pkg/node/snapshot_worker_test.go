package node

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/node/config"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/client/pkg/v3/types"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func preparedCheckpointCluster(t *testing.T, setup func(int, *config.Config) []func(uint64)) ([]*durableNode, []config.Config) {
	t.Helper()
	peers := make([]string, 3)
	for i := range peers {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		peers[i] = "http://" + ln.Addr().String()
		require.NoError(t, ln.Close())
	}
	configs := make([]config.Config, 3)
	nodes := make([]*durableNode, 3)
	for i := range nodes {
		cfg := config.Config{}
		cfg.SetDefaults()
		cfg.ID, cfg.Peers, cfg.DataDir = uint64(i+1), peers, t.TempDir()
		cfg.Logger = zap.NewNop()
		cfg.ProposeC = make(chan []byte)
		cfg.ConfChangeC = make(chan raftpb.ConfChange)
		cfg.BatchTimeout = time.Millisecond
		cfg.RaftEventLoopHeartbeatInterval = 100 * time.Millisecond
		cfg.SnapshotCount, cfg.CatchUpEntriesN = 8, 1
		var hooks []func(uint64)
		if setup != nil {
			hooks = setup(i, &cfg)
		}
		configs[i] = cfg
		nodes[i] = startRaftApplication(t, cfg, hooks...)
	}
	for _, n := range nodes {
		_, err := n.raft.WaitForLeader(5 * time.Second)
		require.NoError(t, err)
	}
	return nodes, configs
}

func TestPreparedCheckpointsKeepQuorumAndRecoverExactViewWithWALTail(t *testing.T) {
	resume := make(chan struct{})
	var release sync.Once
	unblock := func() { release.Do(func() { close(resume) }) }
	defer unblock() // Run before fixture cleanup, including Fatal paths.
	type capture struct {
		member int
		index  uint64
	}
	started := make(chan capture, 3)
	var encoders atomic.Int64
	nodes, configs := preparedCheckpointCluster(t, func(i int, _ *config.Config) []func(uint64) {
		var first sync.Once
		return []func(uint64){func(index uint64) {
			encoders.Add(1)
			first.Do(func() { started <- capture{i, index}; <-resume })
		}}
	})
	want := make(map[string]db.Job)
	for i := range 6 {
		job := db.Job{ID: fmt.Sprintf("before-%d", i), Data: "durable input"}
		want[job.ID] = job
		nodes[i%3].propose(t, db.Command{Type: db.CmdAddJobs, Jobs: []db.Job{job}})
	}
	captured := make(map[int]uint64)
	for range nodes {
		select {
		case c := <-started:
			captured[c.member] = c.index
		case <-time.After(5 * time.Second):
			t.Fatal("all members must capture a checkpoint")
		}
	}
	require.Len(t, captured, 3)
	term := nodes[0].raft.LocalStatus().Term
	job := db.Job{ID: "after-view", Data: "WAL tail survives exact-index snapshot"}
	want[job.ID] = job
	nodes[0].propose(t, db.Command{Type: db.CmdAddJobs, Jobs: []db.Job{job}})
	for _, n := range nodes {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, err := n.raft.ReadBarrier(ctx)
		cancel()
		require.NoError(t, err, "quorum reads must continue with every encoder paused")
		require.Equal(t, want, pendingJobs(t, n.store))
	}
	// Longer than multiple heartbeat/election windows, with all three captured
	// read views still pinned. Pausing an encoder must not pause Raft's loop.
	require.Never(t, func() bool {
		for _, n := range nodes {
			if n.raft.LocalStatus().Term != term {
				return true
			}
		}
		return false
	}, 2500*time.Millisecond, 20*time.Millisecond)
	require.Equal(t, int64(3), encoders.Load(), "at most one encoder per member")
	unblock()
	for i, n := range nodes {
		require.Eventually(t, func() bool { return n.raft.LocalStatus().SnapshotIndex >= captured[i] }, 5*time.Second, 10*time.Millisecond)
		snapshot, err := n.raft.Snapshotter.Load()
		require.NoError(t, err)
		require.Equal(t, captured[i], snapshot.Metadata.Index)
		var payload db.Snapshot
		require.NoError(t, gob.NewDecoder(bytes.NewReader(snapshot.Data)).Decode(&payload))
		require.Equal(t, snapshot.Metadata.Index, payload.LastIndex)
		for _, b := range payload.Buckets {
			for _, r := range b.Records {
				require.NotEqual(t, "after-view", string(r.Key))
			}
		}
	}
	for _, n := range nodes {
		n.close()
	}
	require.NoError(t, os.RemoveAll(filepath.Join(configs[1].DataDir, "store")))
	for i := range nodes {
		nodes[i] = startRaftApplication(t, configs[i])
	}
	for _, n := range nodes {
		_, err := n.raft.WaitForLeader(5 * time.Second)
		require.NoError(t, err)
		require.Eventually(t, func() bool { return len(pendingJobs(t, n.store)) == len(want) }, 5*time.Second, 10*time.Millisecond)
		require.Equal(t, want, pendingJobs(t, n.store))
	}
}

func TestPreparedCheckpointShutdownAndPublicationFailureKeepWALRecoverable(t *testing.T) {
	for _, mode := range []string{"shutdown", "publication failure", "orphaned publication"} {
		t.Run(mode, func(t *testing.T) {
			cfg := config.Config{}
			cfg.SetDefaults()
			cfg.DataDir, cfg.Peers = t.TempDir(), []string{"http://127.0.0.1:0"}
			cfg.Logger = zap.NewNop()
			cfg.ProposeC = make(chan []byte)
			cfg.ConfChangeC = make(chan raftpb.ConfChange)
			cfg.BatchTimeout = time.Millisecond
			cfg.RaftEventLoopHeartbeatInterval = 10 * time.Millisecond
			cfg.SnapshotCount, cfg.CatchUpEntriesN = 8, 1
			started, resume := make(chan struct{}), make(chan struct{})
			var release, first sync.Once
			unblock := func() { release.Do(func() { close(resume) }) }
			defer unblock()
			n := startRaftApplication(t, cfg, func(uint64) { first.Do(func() { close(started); <-resume }) })
			_, err := n.raft.WaitForLeader(3 * time.Second)
			require.NoError(t, err)
			want := make(map[string]db.Job)
			for i := range 9 {
				job := db.Job{ID: fmt.Sprintf("job-%d", i)}
				want[job.ID] = job
				n.propose(t, db.Command{Type: db.CmdAddJobs, Jobs: []db.Job{job}})
			}
			select {
			case <-started:
			case <-time.After(3 * time.Second):
				t.Fatal("encoder did not pin its view")
			}
			if mode == "shutdown" {
				stopped := make(chan struct{})
				go func() { n.raft.Stop(); close(stopped) }()
				select {
				case <-stopped:
					t.Fatal("Raft released resources before its encoder finished")
				case <-time.After(30 * time.Millisecond):
				}
				unblock()
				select {
				case <-stopped:
				case <-time.After(3 * time.Second):
					t.Fatal("shutdown leaked the captured view")
				}
			} else {
				failure := errors.New("injected snapshot publication failure")
				phase := "snapshot_file"
				if mode == "orphaned publication" {
					phase = "snapshot_marker"
				}
				n.raft.persistFault.Store(&persistenceFault{before: func(stage string) error {
					if stage == phase {
						return failure
					}
					return nil
				}})
				unblock()
				select {
				case err := <-n.raft.ErrorC:
					require.ErrorIs(t, err, failure)
				case <-time.After(3 * time.Second):
					t.Fatal("publication failure was not reported")
				}
				select {
				case <-n.raft.Done():
				case <-time.After(3 * time.Second):
					t.Fatal("Raft continued after checkpoint persistence failed")
				}
			}
			n.close()
			if mode == "orphaned publication" {
				_, err := n.raft.Snapshotter.Load()
				require.NoError(t, err, "the complete file must exist before the failed WAL marker")
				selected, _, _, err := readMemberLog(cfg.DataDir, cfg.ID)
				require.NoError(t, err)
				require.True(t, raft.IsEmptySnap(*selected), "recovery must ignore a file without a durable WAL marker")
			}
			// No checkpoint WAL marker was published. A clean application DB must
			// recover every acknowledged command from the untouched durable tail.
			require.NoError(t, os.RemoveAll(filepath.Join(cfg.DataDir, "store")))
			cfg.ProposeC = make(chan []byte)
			cfg.ConfChangeC = make(chan raftpb.ConfChange)
			cfg.SnapshotCount = 100000
			n = startRaftApplication(t, cfg)
			_, err = n.raft.WaitForLeader(3 * time.Second)
			require.NoError(t, err)
			require.Equal(t, want, pendingJobs(t, n.store))
		})
	}
}

func TestReceivedCheckpointSupersedesPausedLocalEncoder(t *testing.T) {
	resume := make(chan struct{})
	var release sync.Once
	unblock := func() { release.Do(func() { close(resume) }) }
	defer unblock()
	started := make(chan uint64, 1)
	var pausedMember atomic.Int64
	pausedMember.Store(-1)
	core, observed := observer.New(zap.InfoLevel)
	nodes, configs := preparedCheckpointCluster(t, func(i int, cfg *config.Config) []func(uint64) {
		cfg.Logger = zap.New(core).With(zap.Int("member", i+1))
		var first sync.Once
		return []func(uint64){func(index uint64) {
			if pausedMember.Load() == int64(i) {
				first.Do(func() { started <- index; <-resume })
			}
		}}
	})
	isolated := 0
	if nodes[isolated].raft.IsLeader() {
		isolated = 1
	}
	pausedMember.Store(int64(isolated))
	want := make(map[string]db.Job)
	for i := range 6 {
		job := db.Job{ID: fmt.Sprintf("initial-%d", i)}
		want[job.ID] = job
		nodes[(isolated+1)%3].propose(t, db.Command{Type: db.CmdAddJobs, Jobs: []db.Job{job}})
	}
	var captured uint64
	select {
	case captured = <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("follower did not capture its local view")
	}
	for i, n := range nodes {
		if i == isolated {
			continue
		}
		nodes[isolated].raft.transport.CutPeer(types.ID(i + 1))
		n.raft.transport.CutPeer(types.ID(isolated + 1))
	}
	oldApplied := nodes[isolated].store.LastAppliedIndex()
	leader := (isolated + 1) % 3
	require.Eventually(t, func() bool {
		for i, n := range nodes {
			if i != isolated && n.raft.IsLeader() {
				leader = i
				return true
			}
		}
		return false
	}, 5*time.Second, 20*time.Millisecond)
	for i := range 24 {
		job := db.Job{ID: fmt.Sprintf("majority-%d", i)}
		want[job.ID] = job
		nodes[leader].propose(t, db.Command{Type: db.CmdAddJobs, Jobs: []db.Job{job}})
	}
	target := nodes[leader].store.LastAppliedIndex()
	for i, n := range nodes {
		if i == isolated {
			continue
		}
		require.Eventually(t, func() bool { return n.raft.LocalStatus().SnapshotIndex > oldApplied+1 }, 5*time.Second, 10*time.Millisecond)
	}
	for i, n := range nodes {
		if i == isolated {
			continue
		}
		nodes[isolated].raft.transport.MendPeer(types.ID(i + 1))
		n.raft.transport.MendPeer(types.ID(isolated + 1))
	}
	require.Eventually(t, func() bool {
		status := nodes[isolated].raft.LocalStatus()
		return status.SnapshotIndex > captured && status.AppliedIndex >= target
	}, 8*time.Second, 20*time.Millisecond)
	received := nodes[isolated].raft.LocalStatus().SnapshotIndex
	require.Equal(t, want, pendingJobs(t, nodes[isolated].store))
	unblock()
	// The log is an acknowledgement from the serialized loop that it handled
	// the old worker result, rather than merely finishing its encoder.
	require.Eventually(t, func() bool { return observed.FilterMessage("discard superseded prepared snapshot").Len() == 1 }, 5*time.Second, 10*time.Millisecond)
	require.GreaterOrEqual(t, nodes[isolated].raft.LocalStatus().SnapshotIndex, received)
	for _, n := range nodes {
		n.close()
	}
	require.NoError(t, os.RemoveAll(filepath.Join(configs[isolated].DataDir, "store")))
	for i := range nodes {
		nodes[i] = startRaftApplication(t, configs[i])
	}
	for _, n := range nodes {
		_, err := n.raft.WaitForLeader(5 * time.Second)
		require.NoError(t, err)
		require.Eventually(t, func() bool { return len(pendingJobs(t, n.store)) == len(want) }, 5*time.Second, 10*time.Millisecond)
		require.Equal(t, want, pendingJobs(t, n.store))
	}
}

func TestByteBudgetTriggersCheckpointBeforeEntryLimit(t *testing.T) {
	nodes, _ := preparedCheckpointCluster(t, func(_ int, cfg *config.Config) []func(uint64) {
		cfg.SnapshotCount = 100000
		cfg.SnapshotBytes = 16 << 10
		cfg.CatchUpBytes = 1024
		cfg.CatchUpEntriesN = 10000
		return nil
	})
	nodes[0].propose(t, db.Command{Type: db.CmdAddJobs, Jobs: []db.Job{{ID: "large", Data: string(make([]byte, 32<<10))}}})
	for _, n := range nodes {
		require.Eventually(t, func() bool {
			status := n.raft.LocalStatus()
			return status.SnapshotIndex > 3 && status.SnapshotBytes > 0 && status.LogBytes < 1024
		}, 5*time.Second, 10*time.Millisecond)
		require.Contains(t, pendingJobs(t, n.store), "large")
	}
}
