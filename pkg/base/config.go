// Portions Copyright 2015 The Cockroach Authors.
// Older configuration/server interfaces derive from CockroachDB 23.1.0.
// The applicable Apache-2.0 grant and original conversion terms are recorded in
// licenses/Apache-2.0.txt and licenses/CockroachDB-23.1-BSL.txt.
// Modified for djob; new engine implementation is governed by the root LICENSE.

package base

import (
	"net/url"
	"time"

	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
)

const (
	defaultSnapshotCount   uint64 = 10000
	defaultCatchUpEntriesN uint64 = 10000

	defaultBatchSize         = 1000
	defaultBatchTimeout      = 1 * time.Second
	defaultElectionTimeout   = 1000 * time.Millisecond
	defaultHeartbeatInterval = 100 * time.Millisecond

	defaultRaftNodeID                    = 1
	defaultRaftElectionTick              = int(defaultElectionTimeout / defaultHeartbeatInterval)
	defaultRaftHeartbeatTick             = 1
	defaultRaftMaxSizePerMsg             = 1024 * 1024
	defaultRaftMaxInflightMsgs           = 256
	defaultRaftMaxUncommittedEntriesSize = 32 << 20
)

const (
	httpScheme  = "http"
	httpsScheme = "https"

	DefaultPort = "10987"

	DefaultHTTPPort = "8080"

	defaultAddr     = ":" + DefaultPort
	defaultHTTPAddr = ":" + DefaultHTTPPort
)

// Config selects the controller's public RPC and HTTP listener addresses.
type Config struct {
	Addr     string
	HTTPAddr string
}

func (cfg *Config) InitDefaults() {
	*cfg = Config{Addr: defaultAddr, HTTPAddr: defaultHTTPAddr}
}

func (cfg *Config) SchedulerURL() *url.URL {
	return &url.URL{Scheme: httpScheme, Host: cfg.HTTPAddr}
}

type SnapshotConfig struct {
	// number of entries to apply before taking a snapshot
	SnapshotCount uint64

	CatchUpEntriesN uint64
	// Serialized log budgets; checkpoint encoding may temporarily exceed them.
	SnapshotBytes   uint64
	CatchUpBytes    uint64
	RetainSnapshots int // Complete recoverable checkpoints retained on disk; default 3.

	// Internal
	GetSnapshot func(index uint64) ([]byte, error)
	// PrepareSnapshot captures an exact-index view synchronously. Raft invokes
	// its encoder exactly once on a bounded background worker, including during
	// shutdown, so it must release the captured view on every return path.
	// When unset, GetSnapshot retains its synchronous compatibility behavior.
	PrepareSnapshot func(index uint64) (func() ([]byte, error), error)
}

func (cfg *SnapshotConfig) SetDefaults() {
	if cfg.SnapshotBytes == 0 {
		cfg.SnapshotBytes = 32 << 20
	}
	if cfg.CatchUpBytes == 0 {
		cfg.CatchUpBytes = 32 << 20
	}
	if cfg.RetainSnapshots == 0 {
		cfg.RetainSnapshots = 3
	}
	if cfg.SnapshotCount == 0 {
		cfg.SnapshotCount = defaultSnapshotCount
	}

	if cfg.CatchUpEntriesN == 0 {
		cfg.CatchUpEntriesN = defaultCatchUpEntriesN
	}
}

// RaftTLSConfig uses a cluster-only CA and one member certificate/key pair.
// The leaf certificate must cover the local peer URL and support both TLS roles.
type RaftTLSConfig struct {
	CAFile   string
	CertFile string
	KeyFile  string
}

type RaftConfig struct {
	*raft.Config
	*SnapshotConfig

	// How much time passes before advancing the internal Raft timers
	// (e.g., for elections and heartbeats).
	RaftEventLoopHeartbeatInterval time.Duration

	Peers     []string
	PeerTLS   *RaftTLSConfig
	ClusterID uint64 // Recovery can establish a fresh transport identity.

	// node is joining an existing cluster
	Join bool
	// DynamicMembership permits guarded learner-first changes within the peer
	// catalog. InitialMembers are immutable bootstrap voters, not current voters.
	DynamicMembership bool
	InitialMembers    []uint64
	Bootstrap         bool // Explicit initial provisioning only; ordinary services must leave false.

	BatchSize    int
	BatchTimeout time.Duration
	ProposeC     chan []byte //
	ConfChangeC  <-chan raftpb.ConfChange
}

// SetDefaults initializes unset fields.
func (cfg *RaftConfig) SetDefaults() {
	if cfg.ClusterID == 0 {
		cfg.ClusterID = 0x1000
	}
	if cfg.Config == nil {
		cfg.Config = &raft.Config{}
	}
	if cfg.SnapshotConfig == nil {
		cfg.SnapshotConfig = &SnapshotConfig{}
	}
	if cfg.RaftEventLoopHeartbeatInterval == 0 {
		cfg.RaftEventLoopHeartbeatInterval = defaultHeartbeatInterval
	}
	if cfg.ID == 0 {
		cfg.ID = defaultRaftNodeID
	}
	if cfg.ElectionTick == 0 {
		cfg.ElectionTick = defaultRaftElectionTick
	}
	if cfg.HeartbeatTick == 0 {
		cfg.HeartbeatTick = defaultRaftHeartbeatTick
	}
	if cfg.MaxSizePerMsg == 0 {
		cfg.MaxSizePerMsg = defaultRaftMaxSizePerMsg
	}
	if cfg.MaxInflightMsgs == 0 {
		cfg.MaxInflightMsgs = defaultRaftMaxInflightMsgs
	}
	if cfg.MaxUncommittedEntriesSize == 0 {
		cfg.MaxUncommittedEntriesSize = defaultRaftMaxUncommittedEntriesSize
	}

	if cfg.BatchSize == 0 {
		cfg.BatchSize = defaultBatchSize
	}
	if cfg.BatchTimeout == 0 {
		cfg.BatchTimeout = defaultBatchTimeout
	}
}

// Returns a new raft.Config
func (rc *RaftConfig) ToRaftConfig() *raft.Config {
	if rc.Config == nil {
		return &raft.Config{}
	}
	cpy := *rc.Config
	return &cpy
}

// Returns a new SnapshotConfig
func (rc *RaftConfig) ToSnapshotConfig() *SnapshotConfig {
	if rc.SnapshotConfig == nil {
		return &SnapshotConfig{}
	}
	cpy := *rc.SnapshotConfig
	return &cpy
}
