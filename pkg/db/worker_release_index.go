package db

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"

	"go.etcd.io/bbolt"
)

// This index accelerates inspection only. Release retirement still checks the
// authoritative usage counter. Its watermark detects databases/snapshots written
// by a binary that did not maintain the index, without changing command formats.
var workerReleaseRunsBucket = []byte("worker_release_runs")
var workerReleaseRunsVersionKey = []byte("worker_release_runs_version")
var workerReleaseRunsAppliedKey = []byte("worker_release_runs_applied")

func initializeWorkerReleaseRunIndex(tx *bbolt.Tx) error {
	applied, err := readAppliedIndex(tx)
	if err != nil {
		return err
	}
	meta := tx.Bucket(metadataBucket)
	indexed := meta.Get(workerReleaseRunsAppliedKey)
	if tx.Bucket(workerReleaseRunsBucket) != nil && bytes.Equal(meta.Get(workerReleaseRunsVersionKey), []byte{1}) && len(indexed) == 8 && binary.BigEndian.Uint64(indexed) == applied {
		return nil
	}
	if tx.Bucket(workerReleaseRunsBucket) != nil {
		if err := tx.DeleteBucket(workerReleaseRunsBucket); err != nil {
			return err
		}
	}
	if _, err := tx.CreateBucket(workerReleaseRunsBucket); err != nil {
		return err
	}
	if err := tx.Bucket(jobRunsBucket).ForEach(func(_, data []byte) error {
		var run JobRun
		if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&run); err != nil {
			return fmt.Errorf("rebuild worker release runs: %w", err)
		}
		return indexWorkerReleaseRun(tx, &run, false)
	}); err != nil {
		return err
	}
	if err := meta.Put(workerReleaseRunsVersionKey, []byte{1}); err != nil {
		return err
	}
	return meta.Put(workerReleaseRunsAppliedKey, binary.BigEndian.AppendUint64(nil, applied))
}

func indexWorkerReleaseRun(tx *bbolt.Tx, run *JobRun, remove bool) error {
	release := outstandingWorkerRelease(run)
	if release == "" {
		return nil
	}
	key := []byte(jobKey(run.TenantID, release, run.ID))
	if remove {
		return tx.Bucket(workerReleaseRunsBucket).Delete(key)
	}
	return tx.Bucket(workerReleaseRunsBucket).Put(key, []byte(jobKey(run.TenantID, run.ID)))
}
