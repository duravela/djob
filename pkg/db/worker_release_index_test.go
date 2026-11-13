package db

import (
	"bytes"
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func TestWorkerReleaseRunPagesTrackOutstandingStatesAndScope(t *testing.T) {
	h := newChannelHarness(t)
	h.job("alpha", "", "legacy", "")
	h.publishedRelease("v1", "worker-v1")
	h.publishedRelease("v2", "worker-v2")
	h.ok(releaseDefinition("work-v1", "v1"))
	h.ok(releaseDefinition("work-v2", "v2"))
	for _, id := range []string{"ready-a", "ready-b", "waiting", "unknown", "success", "cancelled"} {
		h.launch("alpha", "work-v1", id)
	}
	h.launch("alpha", "work-v2", "other-release")
	h.launch("alpha", "legacy", "unpinned")
	for _, id := range []string{"waiting", "unknown", "success"} {
		claim := releaseClaim(id, "worker-v1", "v1", 1, h.at)
		h.ok(claim)
		advance := &AdvanceJob{JobOwner: claim.Job.Claim.JobOwner, Kind: JobComplete, Status: JobSucceeded, Quiescent: true}
		if id == "waiting" {
			advance.Kind = JobWait
			advance.Status = ""
			advance.WaitKey = "continue"
		}
		if id == "unknown" {
			advance.Status = JobUnknown
		}
		h.ok(jc(JobCommand{Advance: advance}))
	}
	h.ok(jc(JobCommand{Cancel: &CancelJob{TenantID: "alpha", RunID: "cancelled", Reason: "withdraw queued work"}}))
	before := h.db.LastAppliedIndex()
	query := JobRecordQuery{Kind: JobRunRecord, TenantID: "alpha", WorkerRelease: "v1"}
	var ids []string
	var cursor, firstCursor []byte
	for {
		page, err := h.db.ListJobRecords(query, cursor, 2)
		require.NoError(t, err)
		require.LessOrEqual(t, len(page.Entries), 2)
		for _, entry := range page.Entries {
			require.Equal(t, "v1", outstandingWorkerRelease(entry.Record.Run))
			ids = append(ids, entry.Record.Run.ID)
		}
		cursor = page.NextCursor
		if firstCursor == nil {
			firstCursor = bytes.Clone(cursor)
		}
		if len(cursor) == 0 {
			break
		}
	}
	require.ElementsMatch(t, []string{"ready-a", "ready-b", "waiting", "unknown"}, ids)
	require.Equal(t, before, h.db.LastAppliedIndex(), "inspection must not append receipts or entries")
	query.WorkerRelease = "v2"
	_, err := h.db.ListJobRecords(query, firstCursor, 2)
	require.ErrorContains(t, err, "another namespace")
	for _, invalid := range []JobRecordQuery{{Kind: JobRunRecord, TenantID: "alpha", WorkerRelease: "v1", ReadyOnly: true}, {Kind: JobDefinitionRecord, TenantID: "alpha", WorkerRelease: "v1"}} {
		_, err := h.db.ListJobRecords(invalid, nil, 2)
		require.ErrorContains(t, err, "outstanding run lists only")
	}
	query.WorkerRelease = "v1"
	query.TenantID = "beta"
	_, err = h.db.ListJobRecords(query, nil, 2)
	require.ErrorContains(t, err, "not_found")
	// Completing a formerly unknown outcome removes it from inspection and usage.
	h.ok(jc(JobCommand{Resolve: &ResolveJob{TenantID: "alpha", RunID: "unknown", ExpectedStep: 1, Status: JobFailed, Actor: "operator", Reason: "confirmed outcome"}}))
	query.TenantID = "alpha"
	page, err := h.db.ListJobRecords(query, nil, 32)
	require.NoError(t, err)
	require.Len(t, page.Entries, 3)
	v, err := h.db.GetWorkerDeployment("alpha", "v1")
	require.NoError(t, err)
	require.EqualValues(t, 3, v.OutstandingRuns)
}

func TestWorkerReleaseRunIndexRebuildsFromSnapshotsWithoutChangingAuthority(t *testing.T) {
	for _, mode := range []string{"missing_bucket", "old_writer_watermark", "different_layout"} {
		t.Run(mode, func(t *testing.T) {
			h := newChannelHarness(t)
			h.job("alpha", "", "legacy", "")
			h.publishedRelease("v1", "worker-v1")
			h.ok(releaseDefinition("work", "v1"))
			h.launch("alpha", "work", "retained")
			require.NoError(t, h.db.db.Update(func(tx *bbolt.Tx) error {
				if mode == "missing_bucket" {
					return tx.DeleteBucket(workerReleaseRunsBucket)
				}
				require.NoError(t, tx.Bucket(workerReleaseRunsBucket).Delete([]byte(jobKey("alpha", "v1", "retained"))))
				if mode == "different_layout" {
					return tx.Bucket(metadataBucket).Put(workerReleaseRunsVersionKey, []byte{2})
				}
				// A previous writer advances the durable applied index without this cache's watermark.
				return tx.Bucket(metadataBucket).Put(appliedKey, binary.BigEndian.AppendUint64(nil, h.index+1))
			}))
			index := h.index
			if mode == "old_writer_watermark" {
				index++
			}
			var snapshot bytes.Buffer
			require.NoError(t, h.db.Snapshot(&snapshot))
			restored, err := NewDB(filepath.Join(t.TempDir(), "restored.db"))
			require.NoError(t, err)
			defer restored.Close()
			require.NoError(t, restored.RestoreAt(bytes.NewReader(snapshot.Bytes()), index))
			page, err := restored.ListJobRecords(JobRecordQuery{Kind: JobRunRecord, TenantID: "alpha", WorkerRelease: "v1"}, nil, 32)
			require.NoError(t, err)
			require.Len(t, page.Entries, 1)
			require.Equal(t, "retained", page.Entries[0].Record.Run.ID)
			release, err := restored.GetWorkerDeployment("alpha", "v1")
			require.NoError(t, err)
			require.EqualValues(t, 1, release.OutstandingRuns)
			require.Equal(t, index, restored.LastAppliedIndex())
			require.NoError(t, restored.db.View(func(tx *bbolt.Tx) error {
				require.Equal(t, []byte{17}, tx.Bucket(metadataBucket).Get(formatKey))
				require.Equal(t, index, binary.BigEndian.Uint64(tx.Bucket(metadataBucket).Get(workerReleaseRunsAppliedKey)))
				return nil
			}))
		})
	}
}
