package systemtest

import (
	"fmt"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/commit"
	"github.com/duravela/djob/pkg/node"
	"github.com/duravela/djob/pkg/node/util/testutil"
	"github.com/duravela/djob/pkg/store"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	leaderTimeout = 50 * time.Second
)

type clusterConfig = testutil.Config

func NewClusterConfig(t *testing.T, customizerFns ...func(c *clusterConfig)) *clusterConfig {
	return testutil.NewTestConfig(t, customizerFns...)
}

type cluster struct {
	leader                *node.RaftNode
	leaderID              uint64
	nodes                 []*node.RaftNode
	jobQueues             []*store.Store
	peers                 []string
	commitC               []<-chan *commit.Commit
	errorC                []<-chan error
	proposeC              []chan []byte
	confChangeC           []chan raftpb.ConfChange
	snapshotTriggeredC    []<-chan struct{}
	configurationChangeCh []<-chan struct{}
	snapshotterReadyC     []<-chan *snap.Snapshotter
}

func (clus *cluster) WaitForLeader(timeout time.Duration) (uint64, error) {
	var leaderID uint64
	for i := range clus.peers {
		nodeID, err := clus.nodes[i].WaitForLeader(timeout)
		if err != nil {
			return 0, err
		}

		leaderID = nodeID
	}

	return leaderID, nil
}

// Close closes all cluster nodes and returns an error if any failed.
func (clus *cluster) Close() (err error) {
	for i := range clus.peers {
		go func(i int) {
			for range clus.commitC[i] { //revive:disable-line:empty-block
				// drain pending commits
			}
		}(i)
		close(clus.proposeC[i])
		clus.jobQueues[i].Stop()
		// wait for channel to close
		if erri := <-clus.errorC[i]; erri != nil {
			err = erri
		}
	}
	return err
}

func (clus *cluster) closeNoErrors(t *testing.T) {
	t.Log("closing cluster...")
	err := clus.Close()
	require.NoError(t, err)
	t.Log("closing cluster [done]")
}

func newCluster(t *testing.T, n int) *cluster {
	peers := make([]string, n)
	for i := range peers {
		peers[i] = fmt.Sprintf("http://127.0.0.1:%d", 10000+i)
	}
	clus := &cluster{
		nodes:                 make([]*node.RaftNode, len(peers)),
		jobQueues:             make([]*store.Store, len(peers)),
		peers:                 peers,
		commitC:               make([]<-chan *commit.Commit, len(peers)),
		errorC:                make([]<-chan error, len(peers)),
		proposeC:              make([]chan []byte, len(peers)),
		confChangeC:           make([]chan raftpb.ConfChange, len(peers)),
		snapshotTriggeredC:    make([]<-chan struct{}, len(peers)),
		snapshotterReadyC:     make([]<-chan *snap.Snapshotter, len(peers)),
		configurationChangeCh: make([]<-chan struct{}, len(peers)),
	}

	for i := range clus.peers {
		clus.proposeC[i] = make(chan []byte)
		clus.confChangeC[i] = make(chan raftpb.ConfChange)

		getSnapshot := func(index uint64) ([]byte, error) { return clus.jobQueues[i].SnapshotAt(index) }

		cfg := *NewClusterConfig(t, func(c *clusterConfig) {
			c.ID = uint64(i + 1)
			c.RaftConfig.Peers = clus.peers
			c.RaftConfig.ProposeC = clus.proposeC[i]
			c.RaftConfig.ConfChangeC = clus.confChangeC[i]
			c.GetSnapshot = getSnapshot

		})

		clus.nodes[i] = node.NewRaftNode(cfg.Config)
		clus.commitC[i] = clus.nodes[i].CommitC
		clus.errorC[i] = clus.nodes[i].ErrorC
		clus.snapshotterReadyC[i] = clus.nodes[i].SnapshotterReady

		select {
		case <-clus.snapshotterReadyC[i]:
		case <-time.After(5 * time.Second):
			panic("snapshotter not ready")
		}

		clus.jobQueues[i] = newJobQueue(t, clus.proposeC[i], clus.commitC[i], clus.nodes[i].Snapshotter)
	}

	leaderId, err := clus.WaitForLeader(leaderTimeout)
	clus.leaderID = leaderId
	require.NoError(t, err)

	for i, n := range clus.nodes {
		if i+1 == int(leaderId) {
			clus.leader = n
			break
		}
	}

	return clus
}

/////// cluster helpers

func jobQueues(clus *cluster) []*store.Store {
	return clus.jobQueues
}

// predicate helpers

func hasPendingJobs(expected int) func(*store.Store) bool {
	return func(jq *store.Store) bool {
		return jq.BucketStats().Pending == expected
	}
}

// assertion helpers

func assertCluster[T any](
	t *testing.T,
	clus *cluster,
	selectFn func(*cluster) []T,
	predicate func(T) bool,
	timeout time.Duration,
) {

	start := time.Now()
	for {
		items := selectFn(clus)
		allOk := true

		for i, item := range items {
			if !predicate(item) {
				allOk = false
				if time.Since(start) > timeout {
					t.Fatalf("Timeout: item %d did not satisfy predicate", i)
				}
				break
			}
		}
		if allOk {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}
