package node

// LocalRaftStatus is an observation of this member, not quorum confirmation or
// execution authority. Indexes describe persisted commit/application progress.
type LocalRaftStatus struct {
	Running             bool
	Leader              bool
	LeaderKnown         bool
	Term                uint64
	CommitIndex         uint64
	AppliedIndex        uint64
	SnapshotIndex       uint64
	PendingReadBarriers int
	LogEntries          uint64
	LogBytes            uint64
	SnapshotBytes       uint64
	UncheckpointedBytes uint64
	SnapshotInFlight    bool
}

// LocalStatus reads a published observation without asking the Raft goroutine
// or taking a store lock. It remains available during stalled application and
// quorum loss. Individual fields may change immediately after the observation.
func (rc *RaftNode) LocalStatus() LocalRaftStatus {
	var result LocalRaftStatus
	if observed := rc.localStatus.Load(); observed != nil {
		result = *observed
	}
	result.Running = rc.ctx != nil && rc.ctx.Err() == nil
	result.Leader = result.Running && rc.isLeader.Load()
	result.LeaderKnown = result.Running && result.LeaderKnown
	rc.readBarriers.mu.Lock()
	result.PendingReadBarriers = len(rc.readBarriers.pending)
	rc.readBarriers.mu.Unlock()
	return result
}

// Only the application goroutine publishes progress. Call after WAL persistence
// to expose committed work, and again after application acknowledgement. Never
// read the application goroutine's mutable indexes from an HTTP scrape.
func (rc *RaftNode) publishLocalStatus(commitIndex uint64) {
	rc.publishMembership()
	if previous := rc.localStatus.Load(); previous != nil && commitIndex < previous.CommitIndex {
		commitIndex = previous.CommitIndex
	}
	if commitIndex < rc.appliedIndex {
		commitIndex = rc.appliedIndex
	}
	rc.readBarriers.mu.Lock()
	term, leaderKnown := rc.readBarriers.term, rc.readBarriers.leader != 0
	rc.readBarriers.mu.Unlock()
	entries, logBytes, snapshotBytes := rc.storage.stats()
	rc.localStatus.Store(&LocalRaftStatus{
		LogEntries: entries, LogBytes: logBytes, SnapshotBytes: snapshotBytes,
		UncheckpointedBytes: rc.storage.bytesAfter(rc.snapshotIndex, rc.appliedIndex), SnapshotInFlight: rc.preparedSnapshotC != nil,
		Term: term, LeaderKnown: leaderKnown, CommitIndex: commitIndex,
		AppliedIndex: rc.appliedIndex, SnapshotIndex: rc.snapshotIndex,
	})
}
