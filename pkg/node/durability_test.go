package node

import (
	"bytes"
	"context"
	"encoding/gob"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/node/config"
	"github.com/duravela/djob/pkg/store"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

type durableNode struct {
	raft  *RaftNode
	store *store.Store
}

func startDurableNode(t *testing.T, dataDir string, snapshotCount uint64) *durableNode {
	t.Helper()
	cfg := config.Config{}
	cfg.SetDefaults()
	cfg.Logger = zap.NewNop()
	cfg.DataDir = dataDir
	cfg.Peers = []string{"http://127.0.0.1:0"}
	cfg.ProposeC = make(chan []byte)
	cfg.ConfChangeC = make(chan raftpb.ConfChange)
	cfg.BatchTimeout = time.Millisecond
	cfg.RaftEventLoopHeartbeatInterval = 10 * time.Millisecond
	cfg.SnapshotCount = snapshotCount
	cfg.CatchUpEntriesN = 1
	n := startRaftApplication(t, cfg)
	_, err := n.raft.WaitForLeader(3 * time.Second)
	require.NoError(t, err)
	// Leader discovery can precede consuming the local Ready notification.
	// Callers of this single-member fixture use the applied leadership guard.
	require.Eventually(t, n.raft.IsLeader, 3*time.Second, time.Millisecond)
	return n
}

func startRaftApplication(t *testing.T, cfg config.Config, snapshotHooks ...func(uint64)) *durableNode {
	t.Helper()
	var application *store.Store
	ready := make(chan struct{})
	cfg.GetSnapshot = func(index uint64) ([]byte, error) {
		<-ready
		return application.SnapshotAt(index)
	}
	cfg.PrepareSnapshot = func(index uint64) (func() ([]byte, error), error) {
		<-ready
		encode, err := application.PrepareSnapshotAt(index)
		if err != nil {
			return nil, err
		}
		return func() ([]byte, error) {
			for _, hook := range snapshotHooks {
				hook(index)
			}
			return encode()
		}, nil
	}
	n := &durableNode{raft: NewRaftNode(cfg)}
	t.Cleanup(n.close)
	select {
	case <-n.raft.SnapshotterReady:
	case <-time.After(3 * time.Second):
		t.Fatal("Raft did not initialize")
	}
	var err error
	application, err = store.NewStore(store.StoreConfig{
		DataDir: cfg.DataDir, ProposalTimeout: 3 * time.Second, Logger: cfg.Logger,
	}, cfg.ProposeC, n.raft.CommitC, n.raft.Snapshotter)
	require.NoError(t, err)
	n.store = application
	require.NoError(t, application.Start())
	close(ready)
	return n
}

func (n *durableNode) close() {
	if n.store != nil {
		n.store.Stop()
	}
	n.raft.Stop()
}

func (n *durableNode) propose(t *testing.T, command db.Command) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	results, err := n.store.ProposeCommand(ctx, command)
	require.NoError(t, err)
	require.Len(t, results, len(command.Jobs))
	for id, err := range results {
		require.NoError(t, err, "job %s", id)
	}
}

func pendingJobs(t *testing.T, application *store.Store) map[string]db.Job {
	t.Helper()
	data, err := application.Snapshot()
	require.NoError(t, err)
	var snapshot db.Snapshot
	require.NoError(t, gob.NewDecoder(bytes.NewReader(data)).Decode(&snapshot))
	jobs := make(map[string]db.Job)
	for _, bucket := range snapshot.Buckets {
		if string(bucket.Name) != "jobs_pending" && string(bucket.Name) != "jobs_high_pri" {
			continue
		}
		for _, record := range bucket.Records {
			var job db.Job
			require.NoError(t, gob.NewDecoder(bytes.NewReader(record.Value)).Decode(&job))
			jobs[job.ID] = job
		}
	}
	return jobs
}

func TestRaftRestartFromWAL(t *testing.T) {
	dir := t.TempDir()
	n := startDurableNode(t, dir, 100000)
	n.propose(t, db.Command{Type: db.CmdAddJobs, Jobs: []db.Job{
		{ID: "keep", Data: "original payload"}, {ID: "deleted"},
	}})
	n.propose(t, db.Command{Type: db.CmdDeleteJob, Jobs: []db.Job{{ID: "deleted"}}})
	index := n.store.LastAppliedIndex()
	_, err := n.raft.Snapshotter.Load()
	require.ErrorIs(t, err, snap.ErrNoSnapshot)
	n.close()

	n = startDurableNode(t, dir, 100000)
	require.GreaterOrEqual(t, n.store.LastAppliedIndex(), index)
	require.Equal(t, map[string]db.Job{"keep": {ID: "keep", Data: "original payload"}}, pendingJobs(t, n.store))
	n.propose(t, db.Command{Type: db.CmdAddJobs, Jobs: []db.Job{{ID: "after restart"}}})
	require.Greater(t, n.store.LastAppliedIndex(), index)
}

func TestRaftRestartFromSnapshotAndWALTail(t *testing.T) {
	dir := t.TempDir()
	n := startDurableNode(t, dir, 3)
	n.propose(t, db.Command{Type: db.CmdAddJobs, Jobs: []db.Job{{ID: "deleted", Data: "in snapshot"}}})
	n.propose(t, db.Command{Type: db.CmdAddJobs, Jobs: []db.Job{{ID: "keep", Data: "survives snapshot"}}})
	var snapshot *raftpb.Snapshot
	require.Eventually(t, func() bool {
		var err error
		snapshot, err = n.raft.Snapshotter.Load()
		return err == nil && snapshot.Metadata.Index == n.store.LastAppliedIndex()
	}, 3*time.Second, 10*time.Millisecond)
	var payload db.Snapshot
	require.NoError(t, gob.NewDecoder(bytes.NewReader(snapshot.Data)).Decode(&payload))
	require.Equal(t, snapshot.Metadata.Index, payload.LastIndex)
	require.Equal(t, db.CurrentSnapshotVersion, payload.Version)
	n.propose(t, db.Command{Type: db.CmdDeleteJob, Jobs: []db.Job{{ID: "deleted"}}})
	n.propose(t, db.Command{Type: db.CmdAddJobs, Jobs: []db.Job{{ID: "tail", Data: "from WAL"}}})
	index := n.store.LastAppliedIndex()
	require.Greater(t, index, snapshot.Metadata.Index)
	n.close()

	// A database newer than the snapshot must keep its post-snapshot changes.
	n = startDurableNode(t, dir, 3)
	want := map[string]db.Job{
		"keep": {ID: "keep", Data: "survives snapshot"},
		"tail": {ID: "tail", Data: "from WAL"},
	}
	require.Equal(t, want, pendingJobs(t, n.store))
	require.GreaterOrEqual(t, n.store.LastAppliedIndex(), index)
	n.close()

	// An empty application database must rebuild from the selected snapshot and WAL.
	require.NoError(t, os.RemoveAll(filepath.Join(dir, "store")))
	n = startDurableNode(t, dir, 3)
	require.Equal(t, want, pendingJobs(t, n.store))
	require.GreaterOrEqual(t, n.store.LastAppliedIndex(), index)
	n.propose(t, db.Command{Type: db.CmdAddJobs, Jobs: []db.Job{{ID: "after recovery"}}})
}

func TestRaftStopsAfterApplicationFailure(t *testing.T) {
	n := startDurableNode(t, t.TempDir(), 100000)
	require.Eventually(t, func() bool { return n.store.LastAppliedIndex() >= 2 }, time.Second, time.Millisecond)
	index := n.store.LastAppliedIndex()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := n.store.ProposeCommand(ctx, db.Command{Type: db.CmdAssignJobs, Jobs: []db.Job{{ID: "unsupported"}}})
	require.ErrorContains(t, err, "unknown command type")
	select {
	case err := <-n.raft.ErrorC:
		require.ErrorContains(t, err, "apply Raft state")
	case <-time.After(3 * time.Second):
		t.Fatal("Raft did not report application failure")
	}
	select {
	case <-n.raft.donec:
	case <-time.After(3 * time.Second):
		t.Fatal("Raft continued after application failure")
	}
	require.Equal(t, index, n.store.LastAppliedIndex())
}
