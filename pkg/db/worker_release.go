package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"go.etcd.io/bbolt"
)

var workerReleaseUsageBucket = []byte("worker_release_usage")
var workerReleaseVersionKey = []byte("worker_release_version")

func initializeWorkerReleases(tx *bbolt.Tx) error {
	meta := tx.Bucket(metadataBucket)
	marker := meta.Get(workerReleaseVersionKey)
	if marker != nil && !bytes.Equal(marker, []byte{1}) || marker == nil && meta.Get(formatKey)[0] >= 17 {
		return fmt.Errorf("missing or unsupported worker release marker")
	}
	b := tx.Bucket(workerReleaseUsageBucket)
	if marker != nil && b == nil || marker == nil && b != nil && b.Stats().KeyN != 0 {
		return fmt.Errorf("inconsistent worker release usage")
	}
	if _, err := tx.CreateBucketIfNotExists(workerReleaseUsageBucket); err != nil {
		return err
	}
	return meta.Put(workerReleaseVersionKey, []byte{1})
}

// A published deployment is a release: its execution template is immutable,
// while replicas and release admission are independently managed.
func checkNewWorkerReleaseRun(tx *bbolt.Tx, tenant, release string) error {
	if release == "" {
		return nil
	}
	if !validWorkerID(release) {
		return reject("invalid", "invalid worker release ID")
	}
	v, err := readRecord[WorkerDeployment](tx, workerDeploymentsBucket, jobKey(tenant, release))
	if err != nil {
		return err
	}
	if !v.Published || v.State != "active" || v.Spec.Replicas == 0 {
		return reject("release_inactive", "worker release is not accepting new runs")
	}
	return nil
}

func checkWorkerReleaseClaim(tx *bbolt.Tx, command StateCommand) error {
	c := command.Job.Claim
	run, err := readRecord[JobRun](tx, jobRunsBucket, jobKey(c.TenantID, c.RunID))
	if err != nil {
		return err
	}
	release := run.Definition.WorkerRelease
	guard := command.WorkerAdmission
	if release == "" {
		if guard != nil && guard.DeploymentID != "" {
			v, err := readRecord[WorkerDeployment](tx, workerDeploymentsBucket, jobKey(c.TenantID, guard.DeploymentID))
			if err != nil {
				return err
			}
			if v.Published {
				return reject("worker_denied", "published workers require an explicitly pinned run")
			}
		}
		return nil
	}
	if guard == nil || guard.DeploymentID != release {
		return reject("worker_denied", "run requires its pinned worker release")
	}
	v, err := readRecord[WorkerDeployment](tx, workerDeploymentsBucket, jobKey(c.TenantID, release))
	if err != nil {
		return err
	}
	if !v.Published {
		return reject("worker_denied", "worker release is not published")
	}
	return nil // Enrollment, generation and deployment admission are checked next.
}

func workerReleaseOutstanding(tx *bbolt.Tx, tenant, release string) (uint64, error) {
	raw := tx.Bucket(workerReleaseUsageBucket).Get([]byte(jobKey(tenant, release)))
	if raw == nil {
		return 0, nil
	}
	if len(raw) != 8 {
		return 0, fmt.Errorf("invalid worker release usage")
	}
	return binary.BigEndian.Uint64(raw), nil
}

func outstandingWorkerRelease(run *JobRun) string {
	if run == nil || knownTerminalJob(run) && run.OwnerID == "" {
		return ""
	}
	return run.Definition.WorkerRelease
}

// Usage changes commit alongside run transitions. Unknown outcomes and ownerless
// waits remain outstanding; a retry/resumption never rechecks new-run admission.
func updateWorkerReleaseUsage(tx *bbolt.Tx, changes jobChanges) error {
	final := make(map[string]jobWrite)
	for _, w := range changes {
		if bytes.Equal(w.bucket, jobRunsBucket) {
			final[w.key] = w
		}
	}
	deltas := make(map[string]int64)
	for key, w := range final {
		before, err := quotaValue[JobRun](tx.Bucket(jobRunsBucket).Get([]byte(key)), nil)
		if err != nil {
			return err
		}
		var after *JobRun
		if !w.remove {
			after, err = quotaValue[JobRun](w.raw, w.value)
			if err != nil {
				return err
			}
		}
		if err := indexWorkerReleaseRun(tx, before, true); err != nil {
			return err
		}
		if err := indexWorkerReleaseRun(tx, after, false); err != nil {
			return err
		}
		if release := outstandingWorkerRelease(before); release != "" {
			deltas[jobKey(before.TenantID, release)]--
		}
		if release := outstandingWorkerRelease(after); release != "" {
			deltas[jobKey(after.TenantID, release)]++
		}
	}
	for key, delta := range deltas {
		if delta == 0 {
			continue
		}
		var count uint64
		raw := tx.Bucket(workerReleaseUsageBucket).Get([]byte(key))
		if raw != nil {
			if len(raw) != 8 {
				return fmt.Errorf("invalid worker release usage")
			}
			count = binary.BigEndian.Uint64(raw)
		}
		if delta < 0 {
			if count < uint64(-delta) {
				return fmt.Errorf("worker release usage underflow")
			}
			count -= uint64(-delta)
		} else {
			if count > math.MaxUint64-uint64(delta) {
				return fmt.Errorf("worker release usage overflow")
			}
			count += uint64(delta)
		}
		if count == 0 {
			if err := tx.Bucket(workerReleaseUsageBucket).Delete([]byte(key)); err != nil {
				return err
			}
		} else if err := tx.Bucket(workerReleaseUsageBucket).Put([]byte(key), binary.BigEndian.AppendUint64(nil, count)); err != nil {
			return err
		}
	}
	return nil
}
