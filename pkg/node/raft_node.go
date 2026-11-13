// Portions Copyright 2015 The etcd Authors.
// WAL, snapshot, transport and Ready-loop scaffolding adapted from etcd's
// contrib/raftexample/raft.go under Apache-2.0; see licenses/Apache-2.0.txt.
// Extensively modified for djob; new engine code is governed by root LICENSE.

package node

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/fileutil"
	"go.etcd.io/etcd/client/pkg/v3/types"

	"go.etcd.io/etcd/server/v3/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	stats "go.etcd.io/etcd/server/v3/etcdserver/api/v2stats"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"

	"go.uber.org/zap"

	"github.com/duravela/djob/pkg/base"
	"github.com/duravela/djob/pkg/commit"
	"github.com/duravela/djob/pkg/node/config"
)

const (
	defaultRaftDir = "raft"
)

// Is responsible for setting up the Raft node, handling Raft's Ready states, proposing entries, managing snapshots, and communicating with other nodes.
// A key-value stream backed by raft
type RaftNode struct {
	isLeader                atomic.Bool
	readBarriers            readBarrierState
	localStatus             atomic.Pointer[LocalRaftStatus]
	membership              atomic.Pointer[MembershipStatus]
	membershipRequests      chan membershipRequest
	membershipPendingUntil  time.Time
	membershipSnapshotIndex uint64

	raftCfg           *base.RaftConfig
	waldir            string // path to WAL directory
	snapdir           string // path to snapshot directory
	storage           *accountedStorage
	Snapshotter       *snap.Snapshotter
	transport         *rafthttp.Transport
	transportStarted  bool
	hasRaftState      bool
	dataDirLock       *fileutil.LockedFile
	confState         raftpb.ConfState
	node              raft.Node
	wal               *wal.WAL
	snapshotIndex     uint64
	appliedIndex      uint64
	replayCommitIndex uint64
	httpServer        *http.Server
	httpListener      net.Listener
	peerClockClient   *http.Client
	clockNow          func() time.Time
	persistFault      atomic.Pointer[persistenceFault]
	stopOnce          sync.Once
	preparedSnapshots sync.WaitGroup
	preparedSnapshotC <-chan preparedSnapshotResult
	donec             chan struct{}
	raftReady         chan struct{}
	proposalsDone     chan struct{}
	ctx               context.Context
	cancel            context.CancelFunc

	batchTimer *time.Timer

	// Channels
	proposeC              <-chan []byte            // proposed messages (jobID,jobData)
	confChangeC           <-chan raftpb.ConfChange // proposed cluster config changes
	CommitC               chan *commit.Commit      // entries committed to log (k,v)
	ErrorC                chan error               // errors from raft session
	stopc                 chan struct{}            // signals proposal channel closed
	httpstopc             chan struct{}            // signals http server to shutdown
	httpdonec             chan struct{}            // signals http server shutdown complete
	SnapshotterReady      chan *snap.Snapshotter   // signals when snapshotter is ready
	configurationChangeCh chan struct{}

	logger *zap.Logger
}

func NewRaftNode(cfg config.Config) *RaftNode {
	cfg.RaftConfig.SetDefaults()
	cfg.SnapshotConfig.SetDefaults()
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}
	raftCfg := cfg.RaftConfig
	raftConfig := *raftCfg.Config
	snapshotConfig := *raftCfg.SnapshotConfig
	raftCfg.Config, raftCfg.SnapshotConfig = &raftConfig, &snapshotConfig
	raftCfg.Peers = append([]string(nil), raftCfg.Peers...)
	raftCfg.InitialMembers = append([]uint64(nil), raftCfg.InitialMembers...)
	if raftCfg.PeerTLS != nil {
		v := *raftCfg.PeerTLS
		raftCfg.PeerTLS = &v
	}
	ctx, cancel := context.WithCancel(context.Background())

	raftBaseDir := filepath.Join(cfg.DataDir, defaultRaftDir)

	rc := &RaftNode{
		raftCfg:               &raftCfg,
		waldir:                filepath.Join(raftBaseDir, fmt.Sprintf("wal-%d", cfg.ID)),
		snapdir:               filepath.Join(raftBaseDir, fmt.Sprintf("snap-%d", cfg.ID)),
		proposeC:              cfg.ProposeC,
		confChangeC:           cfg.ConfChangeC,
		CommitC:               make(chan *commit.Commit),
		ErrorC:                make(chan error, 1),
		stopc:                 make(chan struct{}),
		httpstopc:             make(chan struct{}),
		httpdonec:             make(chan struct{}),
		SnapshotterReady:      make(chan *snap.Snapshotter, 1),
		logger:                cfg.Logger.Named("raft"),
		batchTimer:            time.NewTimer(cfg.BatchTimeout),
		configurationChangeCh: make(chan struct{}),
		donec:                 make(chan struct{}),
		raftReady:             make(chan struct{}),
		proposalsDone:         make(chan struct{}),
		ctx:                   ctx,
		cancel:                cancel,
		clockNow:              time.Now,
		membershipRequests:    make(chan membershipRequest),
	}
	go rc.startRaft()
	return rc
}

func (rc *RaftNode) startRaft() {
	if err := rc.initializeRaft(); err != nil {
		rc.writeError(fmt.Errorf("initialize Raft: %w", err))
		rc.requestStop()
		rc.batchTimer.Stop()
		if rc.httpListener != nil {
			_ = rc.httpListener.Close()
		}
		if rc.transportStarted {
			rc.transport.Stop()
		}
		if rc.node != nil {
			rc.node.Stop()
		}
		if rc.wal != nil {
			_ = rc.wal.Close()
		}
		if rc.dataDirLock != nil {
			_ = rc.dataDirLock.Close()
		}
		close(rc.CommitC)
		close(rc.ErrorC)
		close(rc.donec)
	}
}

func (rc *RaftNode) initializeRaft() error {
	if err := validateMembershipConfig(*rc.raftCfg); err != nil {
		return err
	}
	serverTLS, peerTLSInfo, tlsErr := loadRaftPeerTLS(*rc.raftCfg)
	if tlsErr != nil {
		return tlsErr
	}
	clockTransport := &http.Transport{ResponseHeaderTimeout: time.Second}
	if rc.raftCfg.PeerTLS != nil {
		clockTransport.TLSClientConfig, tlsErr = peerTLSInfo.ClientConfig()
		if tlsErr != nil {
			return tlsErr
		}
	}
	rc.peerClockClient = &http.Client{Transport: clockTransport, Timeout: time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}

	var err error
	directory := filepath.Dir(filepath.Dir(rc.waldir))
	if err := os.MkdirAll(directory, 0o750); err != nil {
		return fmt.Errorf("create member directory: %w", err)
	}
	rc.dataDirLock, err = lockNodeDataDir(directory)
	if err != nil {
		return err
	}
	// Claim the listener before initializing persistent identity/WAL. A failed
	// initial bind must not pin a never-started member to an unusable peer URL.
	peerURL, err := url.Parse(rc.raftCfg.Peers[rc.raftCfg.ID-1])
	if err != nil {
		return fmt.Errorf("parse peer URL: %w", err)
	}
	rc.httpListener, err = net.Listen("tcp", peerURL.Host)
	if err != nil {
		return fmt.Errorf("listen for Raft: %w", err)
	}
	if err := rc.validateMemberIdentity(directory); err != nil {
		return err
	}
	if err := os.MkdirAll(rc.snapdir, 0o750); err != nil {
		return fmt.Errorf("create snapshot directory: %w", err)
	}
	rc.Snapshotter = snap.New(zap.NewExample(), rc.snapdir)

	rc.wal, err = rc.replayWAL()
	if err != nil {
		return err
	}
	if rc.raftCfg.Join && !rc.hasRaftState {
		if err := rc.validateFreshJoin(); err != nil {
			return err
		}
	}
	if rc.raftCfg.DynamicMembership && !rc.hasRaftState && !rc.raftCfg.Join && !rc.raftCfg.Bootstrap {
		return fmt.Errorf("initial member has no durable Raft state; initialize a new cohort explicitly or replace a lost voter under a fresh learner ID")
	}
	ids := bootstrapVoters(*rc.raftCfg)
	rpeers := make([]raft.Peer, len(ids))
	for i, id := range ids {
		rpeers[i] = raft.Peer{ID: id}
		if rc.raftCfg.DynamicMembership {
			raw, err := json.Marshal(rc.membershipScope())
			if err != nil {
				return err
			}
			rpeers[i].Context = append([]byte(bootstrapPrefix), raw...)
		}
	}

	if rc.hasRaftState || rc.raftCfg.Join {
		rc.node = raft.RestartNode(rc.raftCfg.Config)
	} else {
		rc.node = raft.StartNode(rc.raftCfg.Config, rpeers)
	}

	rc.transport = &rafthttp.Transport{
		Logger:      rc.logger.Named("http transport"),
		TLSInfo:     peerTLSInfo,
		ID:          types.ID(rc.raftCfg.ID),
		ClusterID:   types.ID(rc.raftCfg.ClusterID),
		Raft:        rc,
		Snapshotter: rc.Snapshotter,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(zap.NewExample(), fmt.Sprint(rc.raftCfg.ID)),
		ErrorC:      make(chan error),
	}

	if err := rc.transport.Start(); err != nil {
		return fmt.Errorf("start Raft transport: %w", err)
	}
	rc.transportStarted = true

	for i := range rc.raftCfg.Peers {
		if i+1 != int(rc.raftCfg.ID) {
			rc.transport.AddPeer(types.ID(i+1), []string{rc.raftCfg.Peers[i]})
		}
	}
	raftHandler := rc.transport.Handler()
	handler := http.Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/djob/membership" {
			rc.membershipHTTP(w, r)
			return
		}
		if r.URL.Path == "/djob/clock" {
			if r.Method != http.MethodGet || r.URL.RawQuery != "" || r.URL.ForceQuery {
				http.Error(w, "invalid clock probe", http.StatusBadRequest)
				return
			}
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(peerClockReply{NodeID: rc.raftCfg.ID, ClusterID: rc.raftCfg.ClusterID, At: rc.clockNow().UTC()})
			return
		}
		raftHandler.ServeHTTP(w, r)
	}))
	if serverTLS != nil {
		rc.httpListener = tls.NewListener(rc.httpListener, serverTLS)
		handler = secureRaftPeerHandler(handler)
	}
	rc.httpServer = &http.Server{Handler: handler, TLSConfig: serverTLS, ReadHeaderTimeout: 5 * time.Second}
	close(rc.raftReady)
	rc.SnapshotterReady <- rc.Snapshotter

	go rc.serveRaft()
	go rc.serveChannels()
	return nil
}

// replayWAL replays WAL entries into the raft instance.
func (rc *RaftNode) replayWAL() (*wal.WAL, error) {
	rc.logger.Info("replaying WAL of member", zap.Int("id", int(rc.raftCfg.ID)))
	snapshot, err := rc.loadSnapshot()
	if err != nil {
		return nil, err
	}
	w, err := rc.openWAL(snapshot)
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			_ = w.Close()
		}
	}()
	_, st, ents, err := w.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("read WAL: %w", err)
	}

	// Initialize raft storage
	rc.storage = newAccountedStorage()
	rc.raftCfg.Storage = rc.storage
	if snapshot != nil && !raft.IsEmptySnap(*snapshot) {
		if err := rc.storage.ApplySnapshot(*snapshot); err != nil {
			return nil, fmt.Errorf("apply replay snapshot: %w", err)
		}
		rc.logger.Info("applied snapshot",
			zap.Uint64("index", snapshot.Metadata.Index),
			zap.Uint64("term", snapshot.Metadata.Term),
		)
	}
	rc.replayCommitIndex = st.Commit
	rc.hasRaftState = !raft.IsEmptyHardState(st) || len(ents) > 0 || (snapshot != nil && !raft.IsEmptySnap(*snapshot))
	if err := rc.storage.SetHardState(st); err != nil {
		return nil, fmt.Errorf("set replay hard state: %w", err)
	}
	// append to storage so raft starts at the right place in log
	if err := rc.storage.Append(ents); err != nil {
		return nil, fmt.Errorf("append replay entries: %w", err)
	}

	rc.logger.Info("WAL replay completed",
		zap.Int("entries", len(ents)),
		zap.Uint64("last_index", st.Commit),
	)

	ok = true
	return w, nil
}

func (rc *RaftNode) loadSnapshot() (*raftpb.Snapshot, error) {
	if wal.Exist(rc.waldir) {
		walSnaps, err := wal.ValidSnapshotEntries(rc.logger, rc.waldir)
		if err != nil {
			return nil, fmt.Errorf("list snapshots: %w", err)
		}
		snapshot, err := rc.Snapshotter.LoadNewestAvailable(walSnaps)
		if err != nil && !errors.Is(err, snap.ErrNoSnapshot) {
			return nil, fmt.Errorf("load snapshot: %w", err)
		}

		return snapshot, nil
	}
	return nil, nil
}

// openWAL returns a WAL ready for reading.
func (rc *RaftNode) openWAL(snapshot *raftpb.Snapshot) (*wal.WAL, error) {
	if !wal.Exist(rc.waldir) {
		rc.logger.Debug("creating new WAL", zap.String("path", rc.waldir))
		if err := os.MkdirAll(rc.waldir, 0o750); err != nil {
			return nil, fmt.Errorf("create WAL directory: %w", err)
		}

		w, err := wal.Create(rc.logger, rc.waldir, nil)
		if err != nil {
			return nil, fmt.Errorf("create WAL: %w", err)
		}
		if err := w.Close(); err != nil {
			return nil, err
		}
	}

	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index = snapshot.Metadata.Index
		walsnap.Term = snapshot.Metadata.Term
		walsnap.ConfState = &snapshot.Metadata.ConfState
	}

	rc.logger.Info("opening WAL",
		zap.Uint64("snap_index", walsnap.Index),
		zap.Uint64("snap_term", walsnap.Term),
	)

	w, err := wal.Open(rc.logger, rc.waldir, walsnap)
	if err != nil {
		return nil, fmt.Errorf("open WAL: %w", err)
	}

	return w, nil
}

// Done closes only after all Raft resources, including the WAL, are released.
func (rc *RaftNode) Done() <-chan struct{} { return rc.donec }

func (rc *RaftNode) serveChannels() {
	go rc.processProposals()
	defer func() {
		rc.requestStop()
		<-rc.proposalsDone
		rc.preparedSnapshots.Wait()
		_ = rc.httpServer.Close()
		<-rc.httpdonec
		rc.transport.Stop()
		if rc.peerClockClient != nil {
			rc.peerClockClient.CloseIdleConnections()
		}
		rc.node.Stop()
		_ = rc.wal.Close()
		if rc.dataDirLock != nil {
			_ = rc.dataDirLock.Close()
		}
		close(rc.CommitC)
		close(rc.ErrorC)
		close(rc.donec)
	}()
	snap, err := rc.storage.Snapshot()
	if err != nil {
		rc.writeError(err)
		return
	}
	rc.confState = snap.Metadata.ConfState
	rc.snapshotIndex = snap.Metadata.Index
	rc.appliedIndex = snap.Metadata.Index

	recovery := &commit.Recovery{CommittedIndex: rc.replayCommitIndex}
	if !raft.IsEmptySnap(snap) {
		recovery.Snapshot = &commit.Snapshot{Index: snap.Metadata.Index, Data: snap.Data}
	}
	if !rc.publishCommit(&commit.Commit{Recovery: recovery}) {
		return
	}
	rc.publishLocalStatus(rc.replayCommitIndex)

	ticker := time.NewTicker(rc.raftCfg.RaftEventLoopHeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rc.node.Tick()

		case rd := <-rc.node.Ready():
			if !rc.handleReady(rd) {
				return
			}
		case request := <-rc.membershipRequests:
			rc.proposeMembership(request)

		case result := <-rc.preparedSnapshotC:
			if !rc.finishPreparedSnapshot(result) {
				return
			}

		case err := <-rc.transport.ErrorC:
			rc.writeError(err)
			return

		case <-rc.stopc:
			return
		}
	}
}

func (rc *RaftNode) processProposals() {
	defer close(rc.proposalsDone)
	defer rc.requestStop()
	confChangeCount := uint64(0)
	batch := make([][]byte, 0, rc.raftCfg.BatchSize)
	var batchBytes int
	flush := func() { rc.flushBatch(batch); clear(batch); batch = batch[:0]; batchBytes = 0 }
	defer rc.batchTimer.Stop()

	for rc.proposeC != nil {
		select {
		case <-rc.stopc:
			return
		case prop, ok := <-rc.proposeC:
			if !ok {
				rc.proposeC = nil
				continue
			}
			if batchBytes+len(prop) > 8<<20 {
				flush()
			}
			batch = append(batch, prop)
			batchBytes += len(prop)
			if len(batch) >= rc.raftCfg.BatchSize {
				flush()
				rc.batchTimer.Reset(rc.raftCfg.BatchTimeout)
			}

		case <-rc.batchTimer.C:
			if len(batch) > 0 {
				flush()
			}
			rc.batchTimer.Reset(rc.raftCfg.BatchTimeout)

		case cc, ok := <-rc.confChangeC:
			if !ok {
				rc.confChangeC = nil
				continue
			}
			if len(batch) > 0 {
				flush()
				rc.batchTimer.Reset(rc.raftCfg.BatchTimeout)
			}
			rc.handleConfChange(&confChangeCount, cc)
		}
	}
	// client closed channel; shutdown raft if not already
}

// Helper to flush batched proposals
func (rc *RaftNode) flushBatch(batch [][]byte) {
	for _, data := range batch {
		ctx, cancel := context.WithTimeout(rc.ctx, 5*time.Second)
		err := rc.node.Propose(ctx, data)
		cancel()
		if err != nil {
			rc.logger.Error("failed to propose batch", zap.Error(err))
		}
	}
}

func (rc *RaftNode) handleReady(rd raft.Ready) bool {
	rc.updateReadBarrierLeadership(rd)
	if rd.SoftState != nil {
		rc.isLeader.Store(rd.SoftState.RaftState == raft.StateLeader)
	}

	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := rc.saveSnap(rd.Snapshot); err != nil {
			rc.writeError(err)
			return false
		}
	}

	if len(rd.Entries) > 0 || len(rd.CommittedEntries) > 0 {
		if fault := rc.persistFault.Load(); fault != nil {
			if err := fault.before("wal_save"); err != nil {
				rc.writeError(fmt.Errorf("save WAL: %w", err))
				return false
			}
		}
	}
	if err := rc.wal.Save(rd.HardState, rd.Entries); err != nil {
		rc.writeError(fmt.Errorf("save WAL: %w", err))
		return false
	}
	if len(rd.Entries) > 0 || len(rd.CommittedEntries) > 0 {
		if fault := rc.persistFault.Load(); fault != nil {
			if err := fault.before("wal_sync"); err != nil {
				rc.writeError(fmt.Errorf("sync WAL before application: %w", err))
				return false
			}
		}
	}
	// WAL.Save may buffer a commit-only HardState update when MustSync is
	// false. Our durable state machine must never apply or acknowledge beyond
	// the persisted commit boundary, including when installing a snapshot.
	if !rd.MustSync && (len(rd.CommittedEntries) > 0 || !raft.IsEmptySnap(rd.Snapshot)) {
		if err := rc.wal.Sync(); err != nil {
			rc.writeError(fmt.Errorf("sync WAL before application: %w", err))
			return false
		}
	}
	rc.publishLocalStatus(rd.HardState.Commit)
	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := rc.storage.ApplySnapshot(rd.Snapshot); err != nil {
			rc.writeError(err)
			return false
		}
		if !rc.publishSnapshot(rd.Snapshot) {
			return false
		}
	}

	if err := rc.storage.Append(rd.Entries); err != nil {
		rc.writeError(err)
		return false
	}
	rc.transport.Send(rc.processMessages(rd.Messages))
	if !rc.publishEntries(rc.entriesToApply(rd.CommittedEntries)) {
		return false
	}
	rc.publishLocalStatus(0)
	if !rc.maybeTriggerSnapshot() {
		return false
	}
	rc.publishLocalStatus(0)
	rc.completeReadBarriers(rd.ReadStates)
	rc.node.Advance()
	return true
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (rc *RaftNode) publishEntries(ents []raftpb.Entry) bool {
	if len(ents) == 0 {
		return true
	}
	entries := make([]commit.Entry, 0, len(ents))
	removed := false
	for i := range ents {
		entry := commit.Entry{Index: ents[i].Index}
		switch ents[i].Type {
		case raftpb.EntryNormal:
			entry.Data = ents[i].Data

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			if err := cc.Unmarshal(ents[i].Data); err != nil {
				rc.writeError(fmt.Errorf("decode configuration change: %w", err))
				return false
			}
			if err := validateRaftTLSConfChange(*rc.raftCfg, cc); err != nil {
				rc.writeError(err)
				return false
			}
			allowed, err := rc.membershipEntryAllowed(cc)
			if err != nil {
				rc.writeError(err)
				return false
			}
			if !allowed {
				cc.NodeID = 0 // Apply the committed entry as a no-op.
			}
			rc.confState = *rc.node.ApplyConfChange(cc)
			rc.membershipPendingUntil = time.Time{}
			if cc.NodeID == 0 {
				break
			}
			if rc.raftCfg.DynamicMembership {
				rc.membershipSnapshotIndex = ents[i].Index
			}
			switch cc.Type {
			case raftpb.ConfChangeAddNode, raftpb.ConfChangeAddLearnerNode:
				if len(cc.Context) > 0 {
					peer := string(cc.Context)
					if isMembershipContext(cc.Context) || isBootstrapContext(cc.Context) {
						peer = rc.raftCfg.Peers[cc.NodeID-1]
					}
					if cc.NodeID != rc.raftCfg.ID {
						rc.transport.AddPeer(types.ID(cc.NodeID), []string{peer})
					}
					select {
					case rc.configurationChangeCh <- struct{}{}:
					default: // prevent blocking
					}
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rc.raftCfg.ID) {
					rc.logger.Info("shutting down removed node")
					removed = true
					break
				}
				rc.transport.RemovePeer(types.ID(cc.NodeID))
			}
		default:
			rc.writeError(fmt.Errorf("unsupported Raft entry type %v", ents[i].Type))
			return false
		}
		entries = append(entries, entry)
	}
	if !rc.publishCommit(&commit.Commit{Entries: entries}) {
		return false
	}
	rc.appliedIndex = ents[len(ents)-1].Index
	if removed {
		rc.requestStop()
		return false
	}
	return true
}

func (rc *RaftNode) publishCommit(c *commit.Commit) bool {
	done := make(chan struct{})
	errors := make(chan error, 1)
	c.ApplyDoneC, c.ApplyErrorC = done, errors
	select {
	case rc.CommitC <- c:
	case <-rc.stopc:
		return false
	}
	select {
	case <-done:
		return true
	case err := <-errors:
		rc.writeError(fmt.Errorf("apply Raft state: %w", err))
		return false
	case <-rc.stopc:
		return false
	}
}

func (rc *RaftNode) maybeTriggerSnapshot() bool {
	if rc.preparedSnapshotC != nil || (rc.membershipSnapshotIndex <= rc.snapshotIndex && rc.appliedIndex-rc.snapshotIndex <= rc.raftCfg.SnapshotCount && rc.storage.bytesAfter(rc.snapshotIndex, rc.appliedIndex) < rc.raftCfg.SnapshotBytes) {
		return true
	}
	if rc.raftCfg.PrepareSnapshot != nil {
		return rc.startPreparedSnapshot()
	}

	rc.logger.Info(fmt.Sprintf("start snapshot [applied index: %d | last snapshot index: %d]", rc.appliedIndex, rc.snapshotIndex))
	snapshotData, err := rc.raftCfg.GetSnapshot(rc.appliedIndex)
	if err != nil {
		rc.logger.Error("failed to get job queue snapshot", zap.Error(err))
		return true
	}

	snapshot, err := rc.storage.CreateSnapshot(rc.appliedIndex, &rc.confState, snapshotData)
	if err != nil {
		rc.logger.Error("failed to create snapshot", zap.Error(err))
		return true
	}

	if err := rc.saveSnap(snapshot); err != nil {
		rc.writeError(fmt.Errorf("save snapshot: %w", err))
		return false
	}

	rc.compactSnapshot(rc.appliedIndex)
	return true
}

func (rc *RaftNode) saveSnap(snapshot raftpb.Snapshot) error {
	// Save the complete file before a WAL marker can reference it. Orphaned
	// files are safe: recovery selects only checkpoints with valid WAL markers.
	if err := rc.saveSnapshotAtomically(snapshot); err != nil {
		return err
	}
	return rc.recordSnapshot(snapshot)
}

// recordSnapshot runs only on the serialized Ready loop; WAL is not concurrent.
func (rc *RaftNode) recordSnapshot(snapshot raftpb.Snapshot) error {
	walSnap := walpb.Snapshot{
		Index: snapshot.Metadata.Index, Term: snapshot.Metadata.Term,
		ConfState: &snapshot.Metadata.ConfState,
	}
	if fault := rc.persistFault.Load(); fault != nil {
		if err := fault.before("snapshot_marker"); err != nil {
			return err
		}
	}
	if err := rc.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	return rc.wal.ReleaseLockTo(snapshot.Metadata.Index)
}

func (rc *RaftNode) requestStop() {
	rc.stopOnce.Do(func() { close(rc.stopc); close(rc.httpstopc); rc.cancel(); rc.stopReadBarriers() })
}

// Stop waits until the proposal loop, HTTP server, Raft, and WAL have stopped.
func (rc *RaftNode) Stop() {
	rc.requestStop()
	<-rc.donec
}

func (rn *RaftNode) IsLeader() bool {
	return rn.ctx.Err() == nil && rn.isLeader.Load()
}

func (rc *RaftNode) serveRaft() {
	defer close(rc.httpdonec)
	err := rc.httpServer.Serve(rc.httpListener)
	select {
	case <-rc.httpstopc:
	default:
		rc.writeError(fmt.Errorf("serve Raft: %w", err))
	}
}

// handleConfChange processes configuration changes
func (rc *RaftNode) handleConfChange(confChangeCount *uint64, cc raftpb.ConfChange) {
	if err := validateRaftTLSConfChange(*rc.raftCfg, cc); err != nil {
		rc.logger.Error("rejected Raft peer TLS configuration change", zap.Error(err))
		return
	}
	// Generate unique ID for the configuration change
	*confChangeCount++
	cc.ID = *confChangeCount

	ctx, cancel := context.WithTimeout(rc.ctx, 5*time.Second)
	defer cancel()

	// Propose configuration change to raft
	if err := rc.node.ProposeConfChange(ctx, cc); err != nil {
		rc.logger.Error("failed to propose configuration change",
			zap.Error(err),
			zap.String("type", cc.Type.String()),
			zap.Uint64("nodeID", cc.NodeID),
		)
		return
	}

	rc.logger.Info("proposed configuration change",
		zap.String("type", cc.Type.String()),
		zap.Uint64("nodeID", cc.NodeID),
	)
}

func (rc *RaftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if len(ents) == 0 {
		return nil
	}
	firstIdx := ents[0].Index
	if firstIdx > rc.appliedIndex+1 {
		rc.logger.Fatal(fmt.Sprintf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, rc.appliedIndex))
	}
	// Return entries starting from the first unapplied index
	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		return ents[rc.appliedIndex-firstIdx+1:]
	}
	return nil
}

// writeError handles transport errors
func (rc *RaftNode) writeError(err error) {
	rc.logger.Error("raft stopped with error", zap.Error(err))
	select {
	case rc.ErrorC <- err:
	default:
		rc.logger.Warn("error channel full, dropping error", zap.Error(err))
	}

	rc.requestStop()
}

func (rc *RaftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) bool {
	if raft.IsEmptySnap(snapshotToSave) {
		return true
	}
	if snapshotToSave.Metadata.Index <= rc.appliedIndex {
		rc.writeError(fmt.Errorf("snapshot index %d does not advance applied index %d", snapshotToSave.Metadata.Index, rc.appliedIndex))
		return false
	}
	if !rc.publishCommit(&commit.Commit{Snapshot: &commit.Snapshot{Index: snapshotToSave.Metadata.Index, Data: snapshotToSave.Data}}) {
		return false
	}

	rc.confState = snapshotToSave.Metadata.ConfState
	rc.snapshotIndex = snapshotToSave.Metadata.Index
	rc.appliedIndex = snapshotToSave.Metadata.Index
	rc.publishLocalStatus(0)
	return true
}

// Snapshot membership belongs to the snapshot's application index. Later
// configuration changes must replay from its WAL tail, just like normal entries.
func (rc *RaftNode) processMessages(ms []raftpb.Message) []raftpb.Message {
	return ms
}

func (rc *RaftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}
func (rc *RaftNode) IsIDRemoved(id uint64) bool {
	m := rc.Membership()
	if !m.Enabled || id == 0 || id > m.MaxID {
		return false
	}
	for _, active := range append(m.Voters, m.Learners...) {
		if active == id {
			return false
		}
	}
	return true
}
func (rc *RaftNode) ReportUnreachable(id uint64) { rc.node.ReportUnreachable(id) }
func (rc *RaftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	rc.node.ReportSnapshot(id, status)
}

func (rn *RaftNode) WaitForLeader(timeout time.Duration) (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return rn.WaitForLeaderContext(ctx)
}

func (rn *RaftNode) WaitForLeaderContext(ctx context.Context) (uint64, error) {
	select {
	case <-rn.raftReady:
	case <-rn.donec:
		return 0, errors.New("Raft stopped before leader election")
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if status := rn.node.Status(); status.Lead != raft.None {
			return status.Lead, nil
		}
		select {
		case <-rn.donec:
			return 0, errors.New("Raft stopped before leader election")
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-ticker.C:
		}
	}
}

// ClusterID identifies the current Raft cluster, including disaster recovery.
func (rc *RaftNode) ClusterID() uint64 { return rc.raftCfg.ClusterID }
