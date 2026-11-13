package node

import (
	"context"
	"fmt"
	"sync"

	"github.com/duravela/djob/pkg/node/config"
	"github.com/duravela/djob/pkg/store"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

// Application owns Raft and its durable store without the legacy TCP API or
// simulated worker pool. Start independent cluster members concurrently.
type Application struct {
	Raft      *RaftNode
	Store     *store.Store
	closeOnce sync.Once
	cancel    context.CancelFunc
}

func ValidateApplicationConfig(cfg config.Config) error {
	if cfg.DataDir == "" {
		return fmt.Errorf("an explicit data directory is required")
	}
	if cfg.Config == nil || cfg.ID == 0 || cfg.ID > uint64(len(cfg.Peers)) {
		return fmt.Errorf("node ID must select an entry in the ordered peer list")
	}
	if cfg.ReadOnlyOption != raft.ReadOnlySafe {
		return fmt.Errorf("application readiness requires Raft ReadOnlySafe")
	}
	if cfg.Join && !cfg.DynamicMembership {
		return fmt.Errorf("dynamic joining is not supported by the application runtime")
	}
	if err := validateMembershipConfig(cfg.RaftConfig); err != nil {
		return err
	}
	if cfg.RetainSnapshots < 1 || cfg.RetainSnapshots > 100 {
		return fmt.Errorf("retained snapshots must be in 1..100")
	}
	if err := validateRaftPeerURLs(cfg.RaftConfig); err != nil {
		return err
	}
	if cfg.BatchSize <= 0 || cfg.BatchTimeout <= 0 || cfg.RaftEventLoopHeartbeatInterval <= 0 {
		return fmt.Errorf("Raft batching and timer settings must be positive")
	}
	if cfg.HeartbeatTick <= 0 || cfg.ElectionTick <= cfg.HeartbeatTick || cfg.MaxInflightMsgs <= 0 ||
		(cfg.MaxInflightBytes != 0 && cfg.MaxInflightBytes < cfg.MaxSizePerMsg) {
		return fmt.Errorf("invalid Raft election or flow-control settings")
	}
	_, _, err := loadRaftPeerTLS(cfg.RaftConfig)
	return err
}

func OpenApplication(ctx context.Context, cfg config.Config) (*Application, error) {
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("an explicit data directory is required")
	}
	if cfg.Config != nil {
		v := *cfg.Config
		cfg.Config = &v
	}
	if cfg.SnapshotConfig != nil {
		v := *cfg.SnapshotConfig
		cfg.SnapshotConfig = &v
	}
	cfg.Peers = append([]string(nil), cfg.Peers...)
	cfg.InitialMembers = append([]uint64(nil), cfg.InitialMembers...)
	if cfg.PeerTLS != nil {
		v := *cfg.PeerTLS
		cfg.PeerTLS = &v
	}
	cfg.SetDefaults()
	if err := ValidateApplicationConfig(cfg); err != nil {
		return nil, err
	}
	cfg.ProposeC = make(chan []byte)
	cfg.ConfChangeC = make(chan raftpb.ConfChange)
	var jobStore *store.Store
	storeReady := make(chan struct{})
	snapshotCtx, snapshotCancel := context.WithCancel(context.Background())
	cfg.GetSnapshot = func(index uint64) ([]byte, error) {
		select {
		case <-storeReady:
			return jobStore.SnapshotAt(index)
		case <-snapshotCtx.Done():
			return nil, snapshotCtx.Err()
		}
	}
	cfg.PrepareSnapshot = func(index uint64) (func() ([]byte, error), error) {
		select {
		case <-storeReady:
			return jobStore.PrepareSnapshotAt(index)
		case <-snapshotCtx.Done():
			return nil, snapshotCtx.Err()
		}
	}
	application := &Application{Raft: NewRaftNode(cfg), cancel: snapshotCancel}
	select {
	case <-application.Raft.SnapshotterReady:
	case err := <-application.Raft.ErrorC:
		snapshotCancel()
		application.Raft.Stop()
		return nil, fmt.Errorf("start Raft: %w", err)
	case <-ctx.Done():
		snapshotCancel()
		application.Raft.Stop()
		return nil, ctx.Err()
	}
	var err error
	jobStore, err = store.NewStore(store.StoreConfig{DataDir: cfg.DataDir, ProposalTimeout: cfg.ProposalTimeout,
		Logger: cfg.Logger, DisableLegacyWorkers: true}, cfg.ProposeC, application.Raft.CommitC, application.Raft.Snapshotter)
	if err == nil {
		err = jobStore.Start()
	}
	if err != nil {
		snapshotCancel()
		if jobStore != nil {
			jobStore.Stop()
		}
		application.Raft.Stop()
		return nil, err
	}
	application.Store = jobStore
	close(storeReady)
	if err := jobStore.WaitForRecovery(ctx); err != nil {
		application.Close()
		return nil, err
	}
	return application, nil
}

// Close must follow dispatcher shutdown, so cleanup can still commit results.
func (a *Application) Close() {
	a.closeOnce.Do(func() {
		a.cancel()
		if a.Store != nil {
			a.Store.Stop()
		}
		a.Raft.Stop()
	})
}
