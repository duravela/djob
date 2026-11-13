package node

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

// This opt-in diagnostic uses real WAL persistence, Raft snapshots and receipt
// cleanup, but accelerates completed pure jobs without timers or external I/O.
// Heap profiles contain allocation stacks; the database contains synthetic data.
// It deliberately makes no leak assertion from process RSS or one heap sample.
func TestControllerRetentionProfile(t *testing.T) {
	output := os.Getenv("DJOB_RETENTION_PROFILE_DIR")
	if output == "" {
		t.Skip("set DJOB_RETENTION_PROFILE_DIR for the bounded retention diagnostic")
	}
	require.NoError(t, os.Mkdir(output, 0700), "use a new diagnostic output directory")
	cfg := crashTestConfig(t.TempDir())
	cfg.ProposeC, cfg.ConfChangeC = make(chan []byte), make(chan raftpb.ConfChange)
	cfg.SnapshotCount = 10000
	if value := os.Getenv("DJOB_RETENTION_PROFILE_CATCHUP"); value != "" {
		count, err := strconv.ParseUint(value, 10, 64)
		require.NoError(t, err)
		require.Positive(t, count)
		cfg.CatchUpEntriesN = count
	}
	n := startRaftApplication(t, cfg)
	require.Eventually(t, n.raft.IsLeader, 3*time.Second, time.Millisecond)
	propose := func(id string, c db.JobCommand) db.StateResult {
		return genericProposal(t, n, id, c)
	}
	propose("tenant", db.JobCommand{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: "profile"}}})
	propose("definition", db.JobCommand{PutDefinition: &db.PutJobDefinition{Definition: db.JobDefinition{TenantID: "profile", ID: "pure", Runtime: db.StepRuntime, Handler: "hash", HandlerVersion: "1", Input: []byte("synthetic")}}})
	var samples []map[string]any
	capture := func(runs int) {
		last, err := n.raft.storage.LastIndex()
		require.NoError(t, err)
		// A proposal acknowledgement can precede background checkpoint
		// installation. Compare the same completed checkpoint phase, rather
		// than accidentally holding the old entry tail alive during profiling.
		checkpoint := last / (cfg.SnapshotCount + 1) * (cfg.SnapshotCount + 1)
		if checkpoint > 0 {
			require.Eventually(t, func() bool {
				return n.raft.LocalStatus().SnapshotIndex >= checkpoint
			}, 30*time.Second, time.Millisecond)
		}
		first, err := n.raft.storage.FirstIndex()
		require.NoError(t, err)
		last, err = n.raft.storage.LastIndex()
		require.NoError(t, err)
		entries, err := n.raft.storage.Entries(first, last+1, math.MaxUint64)
		require.NoError(t, err)
		var logBytes uint64
		for _, entry := range entries {
			logBytes += uint64(len(entry.Data))
		}
		snapshot, err := n.raft.storage.Snapshot()
		require.NoError(t, err)
		var stats db.StorageStats
		require.Eventually(t, func() bool {
			stats, err = n.store.StorageStats()
			return err == nil
		}, time.Second, time.Millisecond)
		info, err := os.Stat(filepath.Join(cfg.DataDir, "store", "jobs.snap"))
		require.NoError(t, err)
		runtime.GC()
		var memory runtime.MemStats
		runtime.ReadMemStats(&memory)
		sample := map[string]any{"runs": runs, "heap_after_gc_bytes": memory.HeapAlloc, "heap_inuse_bytes": memory.HeapInuse, "goroutines": runtime.NumGoroutine(), "raft_first_index": first, "raft_last_index": last, "raft_entries": len(entries), "raft_entry_data_bytes": logBytes, "raft_snapshot_index": snapshot.Metadata.Index, "raft_snapshot_bytes": len(snapshot.Data), "database_file_bytes": info.Size(), "storage": stats}
		samples = append(samples, sample)
		profile, err := os.OpenFile(filepath.Join(output, fmt.Sprintf("heap-%05d.pprof", runs)), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		require.NoError(t, err)
		require.NoError(t, pprof.WriteHeapProfile(profile))
		require.NoError(t, profile.Close())
		raw, err := json.MarshalIndent(samples, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(output, "samples.json"), append(raw, '\n'), 0600))
		t.Logf("retention sample: %s", mustProfileJSON(t, sample))
	}
	capture(0)
	for i := 0; i < 4000; i++ {
		id := fmt.Sprintf("run-%05d", i)
		propose(id+"/launch", db.JobCommand{Launch: &db.LaunchJob{TenantID: "profile", DefinitionID: "pure", DefinitionRevision: 1, RunID: id}})
		owner := db.JobOwner{TenantID: "profile", RunID: id, AttemptID: "attempt", OwnerID: "synthetic-worker"}
		propose(id+"/claim", db.JobCommand{Claim: &db.ClaimJob{JobOwner: owner, LeaseUntil: time.Now().UTC().Add(time.Minute)}})
		finished := propose(id+"/complete", db.JobCommand{Advance: &db.AdvanceJob{JobOwner: owner, Kind: db.JobComplete, Status: db.JobSucceeded, Quiescent: true}}).Job.Run
		deletion := propose(id+"/purge", db.JobCommand{Purge: &db.PurgeJob{TenantID: "profile", RunID: id, ExpectedStep: finished.Step, FinishedBefore: time.Now().UTC(), BatchSize: 128, Actor: "profile", Reason: "synthetic retained metadata measurement"}}).Job.Deletion
		require.True(t, deletion.CleanupComplete)
		compacted := propose(id+"/compact", db.JobCommand{CompactReceipts: &db.CompactJobReceipts{TenantID: "profile", RunID: id, ExpectedStep: deletion.Step, BatchSize: 128, Actor: "profile", Reason: "synthetic receipt cleanup"}}).Job.ReceiptCompaction
		require.True(t, compacted.Complete)
		if (i+1)%500 == 0 {
			capture(i + 1)
		}
	}
	n.close()
	report, err := ReportStoreStorage(context.Background(), cfg.DataDir)
	require.NoError(t, err)
	raw, err := json.MarshalIndent(report, "", "  ")
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(output, "storage-report.json"), append(raw, '\n'), 0600))
}

func mustProfileJSON(t *testing.T, value any) string {
	t.Helper()
	raw, err := json.Marshal(value)
	require.NoError(t, err)
	return string(raw)
}
