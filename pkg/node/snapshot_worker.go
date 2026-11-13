package node

import (
	"fmt"

	"github.com/gogo/protobuf/proto"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

type preparedSnapshotResult struct {
	snapshot          raftpb.Snapshot
	err               error
	persistenceFailed bool
}

// Capture the exact-index database view before allowing the next Ready to apply.
// Only one encoder is in flight. Its CPU work and file sync must not stop Raft
// ticks, message delivery, read barriers or application acknowledgements.
func (rc *RaftNode) startPreparedSnapshot() bool {
	index := rc.appliedIndex
	term, err := rc.storage.Term(index)
	if err != nil {
		rc.logger.Error("failed to find snapshot term", zap.Error(err))
		return true
	}
	metadata := raftpb.SnapshotMetadata{
		Index: index, Term: term,
		ConfState: *proto.Clone(&rc.confState).(*raftpb.ConfState),
	}
	encode, err := rc.raftCfg.PrepareSnapshot(index)
	if err != nil {
		rc.logger.Error("failed to prepare snapshot", zap.Error(err))
		return true
	}
	rc.logger.Info("start prepared snapshot", zap.Uint64("index", index))
	resultC := make(chan preparedSnapshotResult, 1)
	rc.preparedSnapshotC = resultC
	rc.preparedSnapshots.Add(1)
	go func() {
		defer rc.preparedSnapshots.Done()
		// Invoke even during shutdown: the encoder owns the captured read view.
		data, err := encode()
		result := preparedSnapshotResult{snapshot: raftpb.Snapshot{Metadata: metadata, Data: data}, err: err}
		if err == nil {
			if err = rc.ctx.Err(); err == nil {
				err = rc.saveSnapshotAtomically(result.snapshot)
				result.persistenceFailed = err != nil
			}
			result.err = err
		}
		resultC <- result
	}()
	return true
}

func (rc *RaftNode) finishPreparedSnapshot(result preparedSnapshotResult) bool {
	rc.preparedSnapshotC = nil
	if result.err != nil {
		if result.persistenceFailed {
			rc.writeError(fmt.Errorf("save prepared snapshot: %w", result.err))
			return false
		}
		rc.logger.Error("failed to encode prepared snapshot", zap.Error(result.err))
		return true
	}
	index := result.snapshot.Metadata.Index
	// Installing a received snapshot can supersede a background encoder. Its
	// unreferenced file must never move the WAL marker or local status backward.
	if index <= rc.snapshotIndex {
		rc.logger.Info("discard superseded prepared snapshot", zap.Uint64("index", index), zap.Uint64("snapshot_index", rc.snapshotIndex))
		return true
	}
	if _, err := rc.storage.CreateSnapshot(index, &result.snapshot.Metadata.ConfState, result.snapshot.Data); err != nil {
		rc.logger.Error("failed to create prepared snapshot", zap.Error(err))
		return true
	}
	if err := rc.recordSnapshot(result.snapshot); err != nil {
		rc.writeError(fmt.Errorf("record prepared snapshot: %w", err))
		return false
	}
	rc.compactSnapshot(index)
	rc.publishLocalStatus(0)
	rc.logger.Info("completed prepared snapshot", zap.Uint64("index", index))
	return rc.maybeTriggerSnapshot()
}

func (rc *RaftNode) compactSnapshot(index uint64) {
	compactIndex := rc.storage.compactIndex(index, rc.raftCfg.CatchUpEntriesN, rc.raftCfg.CatchUpBytes)
	if first, _ := rc.storage.FirstIndex(); compactIndex >= first {
		if err := rc.storage.Compact(compactIndex); err != nil {
			rc.logger.Error("failed to compact log", zap.Error(err))
		}
	}
	rc.snapshotIndex = index
	if _, err := retainRaftFiles(rc.ctx, rc.waldir, rc.snapdir, rc.raftCfg.RetainSnapshots); err != nil && rc.ctx.Err() == nil {
		rc.logger.Warn("Raft disk retention deferred", zap.Error(err))
	}
}
