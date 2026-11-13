package node

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/node/config"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

func crashTestConfig(dir string) config.Config {
	cfg := config.Config{}
	cfg.SetDefaults()
	cfg.ID, cfg.DataDir, cfg.Peers, cfg.Logger = 1, dir, []string{"http://127.0.0.1:0"}, zap.NewNop()
	cfg.BatchTimeout, cfg.RaftEventLoopHeartbeatInterval = time.Millisecond, 10*time.Millisecond
	cfg.SnapshotCount = 100000 // WAL.Close and a later snapshot must not hide a buffered commit boundary.
	return cfg
}

func TestRaftCommitBoundaryIsReadableBeforeApplication(t *testing.T) {
	cfg := crashTestConfig(t.TempDir())
	cfg.ProposeC, cfg.ConfChangeC = make(chan []byte), make(chan raftpb.ConfChange)
	n := NewRaftNode(cfg)
	t.Cleanup(n.Stop)
	checked := make(chan error, 1)
	go func() {
		for c := range n.CommitC {
			applicationEntry := false
			for _, e := range c.Entries {
				applicationEntry = applicationEntry || len(e.Data) != 0
			}
			if !applicationEntry {
				close(c.ApplyDoneC)
				continue
			}
			// Raft is blocked waiting for application here. Read the physical WAL
			// without closing or flushing the writer, exactly as recovery would.
			reader, err := wal.OpenForRead(zap.NewNop(), n.waldir, walpb.Snapshot{})
			if err == nil {
				_, state, _, readErr := reader.ReadAll() // ReadAll closes read-only file descriptors.
				err = readErr
				if err == nil && state.Commit < c.Entries[len(c.Entries)-1].Index {
					err = fmt.Errorf("WAL commit boundary %d is behind application index %d", state.Commit, c.Entries[len(c.Entries)-1].Index)
				}
			}
			checked <- err
			if err != nil {
				c.ApplyErrorC <- err
				return
			}
			close(c.ApplyDoneC)
		}
	}()
	_, err := n.WaitForLeader(3 * time.Second)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, n.node.Propose(ctx, []byte("durable application command")))
	select {
	case err := <-checked:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("application never received the committed command")
	}
}

func TestRaftAppliedCommandSurvivesImmediateProcessExit(t *testing.T) {
	const helperEnv = "DJOB_TEST_WAL_EXIT_AFTER_APPLY"
	if dir := os.Getenv(helperEnv); dir != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		app, err := OpenApplication(ctx, crashTestConfig(dir))
		require.NoError(t, err)
		t.Cleanup(app.Close)
		_, err = app.Raft.WaitForLeaderContext(ctx)
		require.NoError(t, err)
		_, err = app.Store.ProposeStateCommand(ctx, db.StateCommand{RequestID: "before-exit", At: time.Now().UTC(),
			Configure: &db.ConfigureChannel{ID: "crash-proof", GatewayID: "crash-proof", Protocol: "fake", Endpoint: "fake:502"}})
		require.NoError(t, err)
		// Exit immediately after acknowledgement, deliberately bypassing Close
		// and all cleanup functions that would otherwise flush the WAL buffer.
		os.Exit(0)
	}
	dir := t.TempDir()
	command := exec.Command(os.Args[0], "-test.run=^TestRaftAppliedCommandSurvivesImmediateProcessExit$", "-test.timeout=15s")
	command.Env = append(os.Environ(), helperEnv+"="+dir)
	output, err := command.CombinedOutput()
	require.NoError(t, err, "%s", output)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	app, err := OpenApplication(ctx, crashTestConfig(dir))
	require.NoError(t, err, "acknowledged state must recover without a graceful WAL close")
	defer app.Close()
	channel, err := app.Store.GetChannel("crash-proof")
	require.NoError(t, err)
	require.Equal(t, "fake:502", channel.Endpoint)
	require.Equal(t, db.ChannelAvailable, channel.State)
}
