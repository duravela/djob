package systemtest

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/client"
	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/node"
	"github.com/duravela/djob/pkg/node/config"
	"github.com/duravela/djob/pkg/node/util/testutil"
	"github.com/duravela/djob/pkg/proto/command/proto"
	"github.com/duravela/djob/pkg/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type testNode struct {
	node     *node.RaftNode
	jobQueue *store.Store
}

func newNode(t *testing.T) *testNode {
	os.RemoveAll(fmt.Sprintf("wal-%d", 1))
	os.RemoveAll(fmt.Sprintf("snap-%d", 1))

	proposec := make(chan []byte)
	confc := make(chan raftpb.ConfChange)

	var jq *store.Store
	getSnapshot := func(index uint64) ([]byte, error) { return jq.SnapshotAt(index) }
	cfg := *testutil.NewTestConfig(t, func(c *testutil.Config) {
		c.RaftConfig.Peers = []string{"http://127.0.0.1:0"}
		c.RaftConfig.ProposeC = proposec
		c.RaftConfig.ConfChangeC = confc
		c.GetSnapshot = getSnapshot

	})
	node := node.NewRaftNode(cfg.Config)

	// Verify snapshotter readiness
	select {
	case <-node.SnapshotterReady:
	case <-time.After(5 * time.Second):
		t.Fatal("snapshotter not ready")
	}

	jq = newJobQueue(t, proposec, node.CommitC, node.Snapshotter)

	_, err := node.WaitForLeader(leaderTimeout)
	require.NoError(t, err)

	return &testNode{
		node:     node,
		jobQueue: jq,
	}
}

func (n *testNode) Close() {
	n.jobQueue.Stop()
	n.node.Stop()
}

func Test_SingleNodeWaitForLeader(t *testing.T) {
	node := newNode(t)
	defer node.Close()

	_, err := node.node.WaitForLeader(leaderTimeout)
	require.NoError(t, err)
}

func Test_SingleNodeAddJobAndAquireJob(t *testing.T) {
	node := newNode(t)
	defer node.Close()

	ctx, cancelFn := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFn()
	_, err := node.jobQueue.ProposeCommand(ctx, db.Command{
		Type: db.CmdAddJobs,
		Jobs: []db.Job{{
			ID:   "job-1",
			Data: "Hello, World!",
			TS:   time.Now(),
		}}})
	require.NoError(t, err)

	assert.Equal(t, 1, node.jobQueue.BucketStats().Pending)
	err = node.jobQueue.AssignJobs([]string{"job-1"})
	require.NoError(t, err)
}

func Test_NewNode(t *testing.T) {
	cfg := new(config.Config).
		WithDefaults().
		Customize(func(c *config.Config) {
			c.DataDir = t.TempDir()
			c.RaftConfig.Peers = []string{"http://127.0.0.1:19009"}
		})
	n, err := node.NewNode(*cfg)
	if err != nil {
		t.Fatalf("error when starting node: %v", err)
	}
	defer n.Close()

	err = n.Open()
	if err != nil {
		t.Fatalf("failed to open connection to node: %v", err)
	}

	client := client.NewClient(&simpleDialer{}, 0)
	_, err = client.GetAllJobReports("127.0.0.1:8080", time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
}

func Test_AddJob(t *testing.T) {
	cfg := new(config.Config).
		WithDefaults().
		Customize(func(c *config.Config) {
			c.DataDir = t.TempDir()
			c.RaftConfig.Peers = []string{"http://127.0.0.1:19009"}
		})
	n, err := node.NewNode(*cfg)
	if err != nil {
		t.Fatalf("error when starting node: %v", err)
	}
	defer n.Close()

	err = n.Open()
	if err != nil {
		t.Fatalf("failed to open connection to node: %v", err)
	}

	priority := int32(0)
	client := client.NewClient(&simpleDialer{}, 0)
	_, err = client.AddJobs(&proto.AddJobsRequest{
		Jobs: []*proto.Job{{
			Id:       "job-1",
			Data:     "no data",
			Deadline: timestamppb.New(time.Now().Add(time.Second)),
			Priority: &priority,
		},
		},
	}, "127.0.0.1:8080", 30*time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.AssignJobs(&proto.AssignJobsRequest{
		WorkerId: "1",
		JobIds:   []string{"job-1"},
	}, "127.0.0.1:8080", 30*time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(5 * time.Second)

	resp, err := client.GetAllJobReports("127.0.0.1:8080", time.Second, 1)
	require.NoError(t, err)
	require.Equal(t, 1, len(resp.JobReports))
}

func Test_AddJobs_TriggerSnapshot(t *testing.T) {
	cfg := new(config.Config).
		WithDefaults().
		Customize(func(c *config.Config) {
			c.DataDir = t.TempDir()
			c.RaftConfig.Peers = []string{"http://127.0.0.1:0"}
			c.SnapshotCount = 1
			c.CatchUpEntriesN = 2
		})
	n, err := node.NewNode(*cfg)
	if err != nil {
		t.Fatalf("error when starting node: %v", err)
	}
	defer n.Close()

	err = n.Open()
	if err != nil {
		t.Fatalf("failed to open connection to node: %v", err)
	}

	priority := int32(0)
	client := client.NewClient(&simpleDialer{}, 0)
	_, err = client.AddJobs(&proto.AddJobsRequest{
		Jobs: []*proto.Job{
			{
				Id:       "job-1",
				Data:     "no data",
				Deadline: timestamppb.New(time.Now().Add(time.Second)),
				Priority: &priority,
			},
			{
				Id:       "job-2",
				Data:     "no data",
				Deadline: timestamppb.New(time.Now().Add(time.Second)),
				Priority: &priority,
			},
		},
	}, "127.0.0.1:8080", 30*time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.AddJobs(&proto.AddJobsRequest{
		Jobs: []*proto.Job{
			{
				Id:       "job-3",
				Data:     "no data",
				Deadline: timestamppb.New(time.Now().Add(time.Second)),
				Priority: &priority,
			},
			{
				Id:       "job-4",
				Data:     "no data",
				Deadline: timestamppb.New(time.Now().Add(time.Second)),
				Priority: &priority,
			},
		},
	}, "127.0.0.1:8080", 30*time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.AssignJobs(&proto.AssignJobsRequest{
		WorkerId: "1",
		JobIds:   []string{"job-1"},
	}, "127.0.0.1:8080", 30*time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(5 * time.Second)

	resp, err := client.GetAllJobReports("127.0.0.1:8080", time.Second, 1)
	require.NoError(t, err)
	require.Equal(t, 1, len(resp.JobReports))
}

type simpleDialer struct {
}

func (s *simpleDialer) Dial(address string, timeout time.Duration) (net.Conn, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
