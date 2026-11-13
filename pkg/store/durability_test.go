package store

import (
	"bytes"
	"encoding/gob"
	"errors"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/commit"
	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
)

func storeEntry(t *testing.T, index uint64, cmd db.Command) commit.Entry {
	t.Helper()
	var data bytes.Buffer
	require.NoError(t, gob.NewEncoder(&data).Encode(cmd))
	return commit.Entry{Index: index, Data: data.Bytes()}
}

func applyCommit(t *testing.T, commits chan<- *commit.Commit, c *commit.Commit) {
	t.Helper()
	done := make(chan struct{})
	failures := make(chan error, 1)
	c.ApplyDoneC, c.ApplyErrorC = done, failures
	commits <- c
	select {
	case <-done:
	case err := <-failures:
		t.Fatalf("apply failed: %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("application was not acknowledged")
	}
}

func TestCommitsPreserveRaftIndexesAndReplayPrefixes(t *testing.T) {
	s, _, commits := newTestJobQueue(t)
	first := storeEntry(t, 7, db.Command{Type: db.CmdAddJobs, Jobs: []db.Job{{ID: "first"}}})
	applyCommit(t, commits, &commit.Commit{Entries: []commit.Entry{first, {Index: 8}}})
	require.Equal(t, uint64(8), s.LastAppliedIndex())
	second := storeEntry(t, 10, db.Command{Type: db.CmdAddJobs, Jobs: []db.Job{{ID: "second"}}})
	applyCommit(t, commits, &commit.Commit{Entries: []commit.Entry{first, {Index: 8}, {Index: 9}, second}})
	require.Equal(t, uint64(10), s.LastAppliedIndex())
	require.Equal(t, 2, s.BucketStats().Pending)
	_, err := s.SnapshotAt(9)
	require.Error(t, err)
}

func TestSnapshotRestoreUsesExistingDatabaseHandle(t *testing.T) {
	source, _, commits := newTestJobQueue(t)
	applyCommit(t, commits, &commit.Commit{Entries: []commit.Entry{storeEntry(t, 12, db.Command{Type: db.CmdAddJobs, Jobs: []db.Job{{ID: "job", Data: "payload"}}})}})
	ok, err := source.db.Acquire("job", "owner", time.Hour)
	require.NoError(t, err)
	require.True(t, ok)
	data, err := source.SnapshotAt(12)
	require.NoError(t, err)
	target, _, targetCommits := newTestJobQueue(t)
	handle := target.db
	require.NoError(t, target.RecoverFromSnapshot(data))
	require.Same(t, handle, target.db)
	require.Equal(t, uint64(12), target.LastAppliedIndex())
	require.Equal(t, 1, target.BucketStats().Progress)
	require.NoError(t, target.db.Release("job", "owner"))
	applyCommit(t, targetCommits, &commit.Commit{Entries: []commit.Entry{{Index: 13}}})
	require.Equal(t, uint64(13), target.LastAppliedIndex())
}

func TestRecoveryPreservesDatabaseAheadOfSnapshot(t *testing.T) {
	s, _, commits := newTestJobQueue(t)
	add := storeEntry(t, 4, db.Command{Type: db.CmdAddJobs, Jobs: []db.Job{{ID: "deleted"}}})
	applyCommit(t, commits, &commit.Commit{Entries: []commit.Entry{add}})
	data, err := s.SnapshotAt(4)
	require.NoError(t, err)
	remove := storeEntry(t, 6, db.Command{Type: db.CmdDeleteJob, Jobs: []db.Job{{ID: "deleted"}}})
	applyCommit(t, commits, &commit.Commit{Entries: []commit.Entry{{Index: 5}, remove}})
	applyCommit(t, commits, &commit.Commit{Recovery: &commit.Recovery{
		CommittedIndex: 6, Snapshot: &commit.Snapshot{Index: 4, Data: data},
	}})
	require.Equal(t, uint64(6), s.LastAppliedIndex())
	require.False(t, s.db.HasPendingJob("deleted"))
	applyCommit(t, commits, &commit.Commit{Entries: []commit.Entry{add, {Index: 5}, remove, {Index: 7}}})
	require.Equal(t, uint64(7), s.LastAppliedIndex())
	require.False(t, s.db.HasPendingJob("deleted"))
}

func TestApplicationFailureStopsWithoutAcknowledging(t *testing.T) {
	for _, failure := range []string{"command", "snapshot", "database ahead of WAL"} {
		t.Run(failure, func(t *testing.T) {
			s, _, commits := newTestJobQueue(t)
			done := make(chan struct{})
			failed := make(chan error, 1)
			bad := &commit.Commit{ApplyDoneC: done, ApplyErrorC: failed}
			switch failure {
			case "command":
				bad.Entries = []commit.Entry{{Index: 4, Data: []byte("invalid gob")}}
			case "snapshot":
				bad.Snapshot = &commit.Snapshot{Index: 4, Data: []byte("invalid snapshot")}
			case "database ahead of WAL":
				applyCommit(t, commits, &commit.Commit{Entries: []commit.Entry{{Index: 8}}})
				bad.Recovery = &commit.Recovery{CommittedIndex: 7}
			}
			commits <- bad
			select {
			case err := <-failed:
				require.Error(t, err)
			case <-time.After(3 * time.Second):
				t.Fatal("failure was not reported")
			}
			select {
			case <-done:
				t.Fatal("failed application was acknowledged")
			default:
			}
			select {
			case <-s.stopC:
			case <-time.After(time.Second):
				t.Fatal("store continued after failure")
			}
			before := s.LastAppliedIndex()
			later := make(chan struct{})
			commits <- &commit.Commit{Entries: []commit.Entry{{Index: 9}}, ApplyDoneC: later}
			s.Stop()
			require.Equal(t, before, s.LastAppliedIndex())
			select {
			case <-later:
				t.Fatal("later commit applied after failure")
			default:
			}
			require.False(t, errors.Is(s.stopError(), ErrProposalTimeout))
		})
	}
}
