package node

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/commit"
	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/node/util/testutil"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	testTimeout = 60 * time.Second
)

type clusterConfig = testutil.Config

func NewClusterConfig(t *testing.T, customizerFns ...func(c *clusterConfig)) *clusterConfig {
	return testutil.NewTestConfig(t, customizerFns...)
}

type cluster struct {
	nodes                 []*RaftNode
	peers                 []string
	commitC               []<-chan *commit.Commit
	errorC                []<-chan error
	proposeC              []chan []byte
	confChangeC           []chan raftpb.ConfChange
	snapshotTriggeredC    []<-chan struct{}
	configurationChangeCh []<-chan struct{}
	snapshotterReadyC     []<-chan *snap.Snapshotter
}

// newCluster creates a cluster of n nodes
func newCluster(t *testing.T, n int, basePort int) *cluster {
	peers := make([]string, n)
	for i := range peers {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		peers[i] = "http://" + ln.Addr().String()
		require.NoError(t, ln.Close())
	}

	clus := &cluster{
		nodes:                 make([]*RaftNode, len(peers)),
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
		clus.proposeC[i] = make(chan []byte, 1)
		clus.confChangeC[i] = make(chan raftpb.ConfChange, 1)

		cfg := *NewClusterConfig(t, func(c *clusterConfig) {
			c.ID = uint64(i + 1)
			c.RaftConfig.Peers = clus.peers
			c.RaftConfig.ProposeC = clus.proposeC[i]
			c.RaftConfig.ConfChangeC = clus.confChangeC[i]
		})
		clus.snapshotTriggeredC[i] = cfg.SnapshotTriggeredC

		node := NewRaftNode(cfg.Config)
		clus.nodes[i] = node
		applicationCommits := make(chan *commit.Commit)
		clus.commitC[i] = applicationCommits
		go func() {
			defer close(applicationCommits)
			for c := range node.CommitC {
				application := false
				for _, entry := range c.Entries {
					if len(entry.Data) > 0 {
						application = true
					}
				}
				if !application {
					close(c.ApplyDoneC)
					continue
				}
				applicationCommits <- c
			}
		}()
		clus.errorC[i] = node.ErrorC
		clus.snapshotterReadyC[i] = node.SnapshotterReady
		clus.configurationChangeCh[i] = node.configurationChangeCh
	}

	return clus
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

func Test_TestConfig(t *testing.T) {
	cfg := NewClusterConfig(t)
	require.NotNil(t, cfg)
}

// TestCloseProposerBeforeReplay tests closing the producer before raft starts.
func TestCloseProposerBeforeReplay(t *testing.T) {
	clus := newCluster(t, 1, 10_000)
	// close before replay so raft never starts
	defer clus.closeNoErrors(t)
}

// Verify that a node initializes correctly, creating necessary WAL and snapshot directories.
func TestRaftNode_Initialization(t *testing.T) {
	peers := []string{"http://127.0.0.1:0"}
	proposeC := make(chan []byte)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(proposeC)
	defer close(confChangeC)

	cfg := *NewClusterConfig(t, func(c *clusterConfig) {
		c.RaftConfig.Peers = peers
		c.RaftConfig.ProposeC = proposeC
		c.RaftConfig.ConfChangeC = confChangeC
	})

	node := NewRaftNode(cfg.Config)
	go func() {
		for c := range node.CommitC {
			close(c.ApplyDoneC)
		}
	}()
	t.Cleanup(node.Stop)

	// Verify snapshotter readiness
	select {
	case <-node.SnapshotterReady:
	case <-time.After(testTimeout):
		t.Fatal("snapshotter not ready")
	}

	// Check directories
	require.DirExists(t, node.waldir)
	require.DirExists(t, node.snapdir)

	_, err := node.WaitForLeader(3 * time.Second)
	require.NoError(t, err)
}

// TestProposeOnCommit starts three nodes and feeds commits back into the proposal
// channel. The intent is to ensure blocking on a proposal won't block raft progress.
func TestProposeOnCommit(t *testing.T) {
	clus := newCluster(t, 3, 11_000)
	defer clus.closeNoErrors(t)
	if _, err := clus.WaitForLeader(3 * time.Second); err != nil {
		require.NoError(t, err)
	}
	donec := make(chan struct{})
	for i := range clus.peers {
		// feedback for "n" committed entries, then update donec
		go func(pC chan<- []byte, cC <-chan *commit.Commit, eC <-chan error) {
			c, ok := <-cC
			if !ok {
				return
			}
			close(c.ApplyDoneC)
			var payload []byte
			for _, entry := range c.Entries {
				if len(entry.Data) > 0 {
					payload = entry.Data
					break
				}
			}
			var buf bytes.Buffer
			if err := gob.NewEncoder(&buf).Encode(payload); err != nil {
				t.Errorf("encoding failed: %v", err)
			}
			select {
			case pC <- buf.Bytes():
				// expected
			case err := <-eC:
				t.Errorf("eC message (%v)", err)
			}
			donec <- struct{}{}
			for c := range cC {
				close(c.ApplyDoneC)
				// acknowledge the commits from other nodes so
				// raft continues to make progress
			}
		}(clus.proposeC[i], clus.commitC[i], clus.errorC[i])

		// one message feedback per node
		go func(i int) {
			cmd := db.Command{
				Type: db.CmdAddJobs,
				Jobs: []db.Job{
					{
						ID:   fmt.Sprintf("job-%d", i),
						Data: "test msg",
						TS:   time.Now(),
					},
				},
			}

			var buf bytes.Buffer
			if err := gob.NewEncoder(&buf).Encode(cmd); err != nil {
				t.Errorf("encoding failed: %v", err)
			}
			clus.proposeC[i] <- buf.Bytes()
		}(i)
	}

	for range clus.peers {
		<-donec
	}
}
