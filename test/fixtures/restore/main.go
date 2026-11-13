// The test image uses this program to prepare native checkpoint/WAL fixtures
// and verify every restored value while a controller is stopped. It is never
// included in release archives.
package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/duravela/djob/pkg/commit"
	"github.com/duravela/djob/pkg/db"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/storage/wal"
	"go.etcd.io/etcd/server/v3/storage/wal/walpb"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

const records, valueBytes, batch = 20480, 4096, 128
const checkpointIndex = uint64(records / batch)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) != 4 || (os.Args[1] != "seed" && os.Args[1] != "verify") {
		return fmt.Errorf("usage: restore-fixture seed|verify DATA_DIR MEMBER_ID")
	}
	id, err := strconv.ParseUint(os.Args[3], 10, 64)
	if err != nil || id < 1 || id > 3 {
		return fmt.Errorf("member ID must be 1..3")
	}
	dir := os.Args[2]
	if err := os.MkdirAll(filepath.Join(dir, "store"), 0700); err != nil {
		return err
	}
	path := filepath.Join(dir, "store", "jobs.snap")
	q, err := db.NewDB(path)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			_ = q.Close()
		}
	}()
	report := map[string]any{"mode": os.Args[1], "member_id": id, "records": records, "value_bytes": valueBytes, "raw_value_bytes": records * valueBytes, "checkpoint_index": checkpointIndex, "wal_tail_index": checkpointIndex + 1}
	if info, ok := debug.ReadBuildInfo(); ok {
		report["go_version"] = info.GoVersion
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" {
				report["source_revision"] = setting.Value
			}
		}
	}
	if os.Args[1] == "seed" {
		for i := 0; i < records; i += batch {
			jobs := make([]db.Job, batch)
			for j := range jobs {
				jobs[j] = fixtureJob(i + j)
			}
			entry, err := command(uint64(i/batch+1), jobs)
			if err != nil {
				return err
			}
			if result := q.ApplyEntries([]commit.Entry{entry}); result.Error != nil {
				return result.Error
			}
		}
		var data bytes.Buffer
		if err := q.SnapshotAt(&data, checkpointIndex); err != nil {
			return err
		}
		report["snapshot_payload_bytes"] = data.Len()
		snapdir := filepath.Join(dir, "raft", fmt.Sprintf("snap-%d", id))
		if err := os.MkdirAll(snapdir, 0700); err != nil {
			return err
		}
		checkpoint := raftpb.Snapshot{Data: data.Bytes(), Metadata: raftpb.SnapshotMetadata{Index: checkpointIndex, Term: 1, ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}}}}
		if err := snap.New(zap.NewNop(), snapdir).SaveSnap(checkpoint); err != nil {
			return err
		}
		writer, err := wal.Create(zap.NewNop(), filepath.Join(dir, "raft", fmt.Sprintf("wal-%d", id)), nil)
		if err != nil {
			return err
		}
		tail, err := command(checkpointIndex+1, []db.Job{fixtureJob(records)})
		if err == nil {
			err = writer.SaveSnapshot(walpb.Snapshot{Index: checkpointIndex, Term: 1, ConfState: &checkpoint.Metadata.ConfState})
		}
		if err == nil {
			err = writer.Save(raftpb.HardState{Term: 1, Commit: tail.Index}, []raftpb.Entry{{Index: tail.Index, Term: 1, Type: raftpb.EntryNormal, Data: tail.Data}})
		}
		closeErr := writer.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		if err := q.Close(); err != nil {
			return err
		}
		closed = true
		// Force the controller to rebuild from its native checkpoint and WAL.
		if err := os.Remove(path); err != nil {
			return err
		}
		empty, err := db.NewDB(path)
		if err != nil {
			return err
		}
		if empty.LastAppliedIndex() != 0 {
			_ = empty.Close()
			return fmt.Errorf("fixture database is not empty")
		}
		if err := empty.Close(); err != nil {
			return err
		}
		identity, err := json.Marshal(map[string]any{"version": 1, "member_id": fmt.Sprint(id), "cluster_id": "4096", "peers": []string{"https://member-1:9021", "https://member-2:9021", "https://member-3:9021"}})
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(dir, ".djob-cluster.json"), identity, 0600); err != nil {
			return err
		}
		report["empty_database_index"] = 0
	} else {
		if q.LastAppliedIndex() < checkpointIndex+1 {
			return fmt.Errorf("committed WAL tail was not applied")
		}
		jobs, err := q.FetchPendingJobs(records + 2)
		if err != nil {
			return err
		}
		if len(jobs) != records+1 || q.Stats().Pending != records+1 {
			return fmt.Errorf("restored job count differs: %d", len(jobs))
		}
		seen := make(map[string]bool, len(jobs))
		for _, job := range jobs {
			var n int
			if _, err := fmt.Sscanf(job.ID, "recovery-%08d", &n); err != nil || n < 0 || n > records {
				return fmt.Errorf("unexpected job %q", job.ID)
			}
			want := fixtureJob(n)
			if seen[job.ID] || job.ID != want.ID || job.Data != want.Data {
				return fmt.Errorf("corrupt or duplicate restored job %q", job.ID)
			}
			seen[job.ID] = true
		}
		names, err := filepath.Glob(filepath.Join(dir, "raft", fmt.Sprintf("snap-%d", id), "*.snap"))
		if err != nil || len(names) != 1 {
			return fmt.Errorf("expected one complete large checkpoint: %v", err)
		}
		var term, snapshotIndex uint64
		if _, err := fmt.Sscanf(filepath.Base(names[0]), "%016x-%016x.snap", &term, &snapshotIndex); err != nil {
			return err
		}
		info, err := os.Stat(names[0])
		if err != nil || info.Size() < records*valueBytes || snapshotIndex < checkpointIndex {
			return fmt.Errorf("complete large checkpoint is missing: %v", err)
		}
		report["verified_jobs"] = len(jobs)
		report["snapshot_file_bytes"] = info.Size()
		report["snapshot_index"], report["snapshot_term"] = snapshotIndex, term
		report["applied_index"] = q.LastAppliedIndex()
		info, err = os.Stat(path)
		if err != nil || info.Size() > 192<<20 {
			return fmt.Errorf("restored database exceeds 192 MiB: %v", err)
		}
		report["database_file_bytes"] = info.Size()
	}
	if !closed {
		if err := q.Close(); err != nil {
			return err
		}
		closed = true
	}
	report["passed"] = true
	return json.NewEncoder(os.Stdout).Encode(report)
}

func fixtureJob(n int) db.Job {
	return db.Job{ID: fmt.Sprintf("recovery-%08d", n), Data: strings.Repeat(string(byte('a'+n%26)), valueBytes)}
}

func command(index uint64, jobs []db.Job) (commit.Entry, error) {
	var data bytes.Buffer
	err := gob.NewEncoder(&data).Encode(db.Command{Version: db.CurrentCommandVersion, Type: db.CmdAddJobs, Jobs: jobs})
	return commit.Entry{Index: index, Data: data.Bytes()}, err
}
