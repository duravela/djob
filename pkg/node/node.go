package node

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/duravela/djob/pkg/apiserver"
	"github.com/duravela/djob/pkg/commit"
	"github.com/duravela/djob/pkg/node/config"
	"github.com/duravela/djob/pkg/store"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/raft/v3/raftpb"
)

type Node struct {
	service   *apiserver.Service
	raft      *RaftNode
	store     *store.Store
	closeOnce sync.Once

	proposeC    chan []byte
	confChangeC chan raftpb.ConfChange
}

func NewNode(cfg config.Config) (*Node, error) {
	proposeC := make(chan []byte)
	confChangeC := make(chan raftpb.ConfChange)

	var js *store.Store
	storeReady := make(chan struct{})
	getSnapshot := func(index uint64) ([]byte, error) { <-storeReady; return js.SnapshotAt(index) }

	cfg.GetSnapshot = getSnapshot
	cfg.PrepareSnapshot = func(index uint64) (func() ([]byte, error), error) {
		<-storeReady
		return js.PrepareSnapshotAt(index)
	}
	cfg.ProposeC = proposeC
	cfg.ConfChangeC = confChangeC

	node := NewRaftNode(cfg)
	select {
	case <-node.SnapshotterReady:
	case <-time.After(5 * time.Second):
		node.Stop()
		return nil, fmt.Errorf("snapshotter not ready")
	}

	var err error
	js, err = createStore(cfg, node.CommitC, node.Snapshotter)
	if err != nil {
		node.Stop()
		return nil, err
	}
	close(storeReady)
	adapter := &apiserver.JobStoreAdapter{
		Store: js,
	}

	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", cfg.Server.Port))
	if err != nil {
		js.Stop()
		node.Stop()
		return nil, err
	}
	service := apiserver.New(l, adapter, adapter)
	if _, err = node.WaitForLeader(10 * time.Second); err != nil {
		_ = service.Close()
		js.Stop()
		node.Stop()
		return nil, err
	}
	return &Node{
		service:     service,
		raft:        node,
		store:       js,
		proposeC:    proposeC,
		confChangeC: confChangeC,
	}, nil
}

func (n *Node) Open() error {
	return n.service.Open()
}

func (n *Node) Close() {
	n.closeOnce.Do(func() {
		_ = n.service.Close()
		n.store.Stop()
		n.raft.Stop()
	})
}

func createStore(
	cfg config.Config,
	commitc <-chan *commit.Commit,
	snapshotter *snap.Snapshotter,
) (*store.Store, error) {
	jq, err := store.NewStore(
		store.StoreConfig{
			DataDir:         cfg.DataDir,
			ProposalTimeout: 30 * time.Second,
			Logger:          cfg.Logger,
		},
		cfg.ProposeC,
		commitc,
		snapshotter,
	)
	if err != nil {
		return nil, fmt.Errorf("initialize job store: %w", err)
	}
	if err := jq.Start(); err != nil {
		return nil, err
	}
	return jq, nil
}
