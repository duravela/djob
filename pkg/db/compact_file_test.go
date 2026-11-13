//go:build darwin || linux

package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/gob"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
)

func bloatedCompactionFile(t *testing.T) (string, Snapshot, StateCommand) {
	t.Helper()
	h := newChannelHarness(t)
	run, original := h.finishedEventRun()
	h.ok(jc(purge("tenant", run.ID, run.Step, h.at, MaxJobPurgeBatch)))
	h.ok(jc(compactReceipts("tenant", run.ID, run.Step, MaxJobPurgeBatch)))
	h.launch("tenant", "work", "uncertain")
	owner := h.claimJob("tenant", "uncertain", "unknown-attempt")
	h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, Kind: JobDoAction, Operation: "write", Input: []byte("possible remote effect")}}))
	h.at = h.at.Add(2 * time.Minute)
	h.ok(jc(JobCommand{Expire: &ExpireJob{JobOwner: owner, LeaseUntil: h.at.Add(-time.Minute)}}))
	require.NoError(t, h.db.db.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucket([]byte("opaque-extension"))
		if err != nil {
			return err
		}
		if err := b.SetSequence(17); err != nil {
			return err
		}
		child, err := b.CreateBucket([]byte("nested"))
		if err != nil {
			return err
		}
		if err := child.SetSequence(^uint64(0) - 1); err != nil {
			return err
		}
		if err := child.Put([]byte{0, 255}, []byte{}); err != nil {
			return err
		}
		return child.Put([]byte("payload"), []byte("frozen-state-that-must-survive-physical-compaction"))
	}))
	addCompactionSlack(t, h.db.db)
	var encoded bytes.Buffer
	require.NoError(t, h.db.Snapshot(&encoded))
	var before Snapshot
	require.NoError(t, gob.NewDecoder(bytes.NewReader(encoded.Bytes())).Decode(&before))
	path := h.db.db.Path()
	require.NoError(t, h.db.Close())
	return path, before, original
}

func addCompactionSlack(t *testing.T, database *bbolt.DB) {
	t.Helper()
	require.NoError(t, database.Update(func(tx *bbolt.Tx) error {
		b, err := tx.CreateBucket([]byte("temporary-space"))
		if err != nil {
			return err
		}
		value := bytes.Repeat([]byte("discarded"), 64<<10)
		for i := range 32 {
			if err := b.Put([]byte(fmt.Sprintf("%03d", i)), value); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, database.Update(func(tx *bbolt.Tx) error { return tx.DeleteBucket([]byte("temporary-space")) }))
}

func compactionFileHash(t *testing.T, path string) [32]byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return sha256.Sum256(data)
}

func noCompactionArtifacts(t *testing.T, path string) {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".jobs.snap.compact-*"))
	require.NoError(t, err)
	require.Empty(t, paths)
}

func TestFileCompactionShrinksWithoutChangingStateOrRecoveryGuards(t *testing.T) {
	path, before, original := bloatedCompactionFile(t)
	require.NoError(t, os.Chmod(path, 0640))
	attribute := "user.djob.compaction-test"
	attributeValue := []byte{0, 255, 1}
	attributeErr := unix.Setxattr(path, attribute, attributeValue, 0)
	result, err := CompactFile(context.Background(), path, FileCompactionOptions{})
	require.NoError(t, err)
	require.True(t, result.Installed)
	require.Equal(t, before.LastIndex, result.AppliedIndex)
	require.Equal(t, byte(7), result.Format)
	require.Less(t, result.AfterBytes, result.BeforeBytes/4)
	require.Equal(t, result.BeforeBytes-result.AfterBytes, result.SavedBytes)
	require.Empty(t, result.BackupPath)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0640), info.Mode().Perm())
	if attributeErr == nil {
		value := make([]byte, len(attributeValue))
		_, err := unix.Getxattr(path, attribute, value)
		require.NoError(t, err)
		require.Equal(t, attributeValue, value)
	}
	q, err := NewDB(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = q.Close() })
	var encoded bytes.Buffer
	require.NoError(t, q.Snapshot(&encoded))
	var after Snapshot
	require.NoError(t, gob.NewDecoder(bytes.NewReader(encoded.Bytes())).Decode(&after))
	require.Equal(t, before, after, "every bucket, sequence, key and raw value must survive")
	resource, err := q.GetJobResource("bus")
	require.NoError(t, err)
	require.Equal(t, ResourceBlocked, resource.State)
	unknown, err := q.GetJobRun("tenant", "uncertain")
	require.NoError(t, err)
	require.Equal(t, JobUnknown, unknown.Status)
	require.Equal(t, EffectUnknown, unknown.PendingAction.Status)
	h := &channelHarness{t: t, db: q, at: original.Job.Activate.OccurredAt, index: before.LastIndex}
	require.True(t, h.apply(original).Compacted)
	info, err = os.Stat(path)
	require.NoError(t, err)
	require.Less(t, info.Size(), result.BeforeBytes, "reopening and retrying should retain the space saving")
	noCompactionArtifacts(t, path)
}

func TestFileCompactionRetainsOriginalOnlyWhenRequested(t *testing.T) {
	path, _, _ := bloatedCompactionFile(t)
	before := compactionFileHash(t, path)
	result, err := CompactFile(context.Background(), path, FileCompactionOptions{KeepBackup: true})
	require.NoError(t, err)
	require.True(t, result.Installed)
	require.NotEmpty(t, result.BackupPath)
	require.Equal(t, before, compactionFileHash(t, result.BackupPath))
	backup, err := os.Stat(result.BackupPath)
	require.NoError(t, err)
	installed, err := os.Stat(path)
	require.NoError(t, err)
	require.False(t, os.SameFile(backup, installed))
	second, err := CompactFile(context.Background(), path, FileCompactionOptions{})
	require.NoError(t, err)
	require.False(t, second.Installed)
	unchanged, err := os.Stat(path)
	require.NoError(t, err)
	require.True(t, os.SameFile(installed, unchanged), "no saving must preserve the source inode")
}

func TestFileCompactionFailureBeforePublicationLeavesOriginalUnchanged(t *testing.T) {
	for _, phase := range []CompactionPhase{CompactionCheck, CompactionCopy, CompactionVerify, CompactionPublish} {
		t.Run(string(phase), func(t *testing.T) {
			path, _, _ := bloatedCompactionFile(t)
			before := compactionFileHash(t, path)
			injected := errors.New("stop maintenance")
			result, err := CompactFile(context.Background(), path, FileCompactionOptions{OnPhase: func(current CompactionPhase) error {
				if current == phase {
					return injected
				}
				return nil
			}})
			require.ErrorIs(t, err, injected)
			require.False(t, result.Installed)
			require.Equal(t, before, compactionFileHash(t, path))
			noCompactionArtifacts(t, path)
		})
	}
}

func TestFileCompactionCancellationDuringCopyLeavesOriginalUnchanged(t *testing.T) {
	path, _, _ := bloatedCompactionFile(t)
	before := compactionFileHash(t, path)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result, err := CompactFile(ctx, path, FileCompactionOptions{OnPhase: func(phase CompactionPhase) error {
		if phase == CompactionCopy {
			cancel()
		}
		return nil
	}})
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, result.Installed)
	require.Equal(t, before, compactionFileHash(t, path))
	noCompactionArtifacts(t, path)
}

func TestFileCompactionRenameFailureDoesNotLoseOriginal(t *testing.T) {
	path, _, _ := bloatedCompactionFile(t)
	before := compactionFileHash(t, path)
	injected := errors.New("rename failure")
	result, err := compactFile(context.Background(), path, FileCompactionOptions{}, compactionFileOps{
		rename: func(string, string) error { return injected }, syncDir: syncCompactionDirectory, remove: os.Remove,
	})
	require.ErrorIs(t, err, injected)
	require.False(t, result.Installed)
	require.Equal(t, before, compactionFileHash(t, path))
	noCompactionArtifacts(t, path)
}

func TestFileCompactionPostRenameFailuresReportInstalledAndRetainOriginal(t *testing.T) {
	for _, failure := range []string{"directory-sync", "remove-backup"} {
		t.Run(failure, func(t *testing.T) {
			path, _, _ := bloatedCompactionFile(t)
			before := compactionFileHash(t, path)
			injected := errors.New("post-publication failure")
			syncs := 0
			ops := compactionFileOps{rename: os.Rename, syncDir: func(path string) error {
				syncs++
				if failure == "directory-sync" && syncs == 2 {
					return injected
				}
				return syncCompactionDirectory(path)
			}, remove: func(path string) error {
				if failure == "remove-backup" {
					return injected
				}
				return os.Remove(path)
			}}
			result, err := compactFile(context.Background(), path, FileCompactionOptions{}, ops)
			require.ErrorIs(t, err, injected)
			require.True(t, result.Installed)
			require.Contains(t, err.Error(), "installed")
			require.Equal(t, before, compactionFileHash(t, result.BackupPath))
			q, err := NewDB(path)
			require.NoError(t, err)
			require.Equal(t, result.AppliedIndex, q.LastAppliedIndex())
			require.NoError(t, q.Close())
		})
	}
}

func TestFileCompactionRefusesOpenDatabaseAndStorageAliases(t *testing.T) {
	for _, kind := range []string{"writer", "reader", "symlink", "hardlink"} {
		t.Run(kind, func(t *testing.T) {
			path, _, _ := bloatedCompactionFile(t)
			before := compactionFileHash(t, path)
			source := path
			switch kind {
			case "writer", "reader":
				q, err := bbolt.Open(path, 0600, &bbolt.Options{ReadOnly: kind == "reader"})
				require.NoError(t, err)
				t.Cleanup(func() { _ = q.Close() })
			case "symlink":
				source = filepath.Join(filepath.Dir(path), "alias")
				require.NoError(t, os.Symlink(path, source))
			case "hardlink":
				require.NoError(t, os.Link(path, filepath.Join(filepath.Dir(path), "alias")))
			}
			result, err := CompactFile(context.Background(), source, FileCompactionOptions{})
			require.Error(t, err)
			require.False(t, result.Installed)
			require.Equal(t, before, compactionFileHash(t, path))
			noCompactionArtifacts(t, path)
		})
	}
}

func TestFileCompactionRefusesUnsupportedOrIncompleteStateWithoutChangingFile(t *testing.T) {
	for _, kind := range []string{"future-format", "missing-marker", "missing-bucket", "configuration-marker", "configuration-bucket", "configuration-receipt-marker", "configuration-receipt-bucket", "unsupported-feature"} {
		t.Run(kind, func(t *testing.T) {
			path, _, _ := bloatedCompactionFile(t)
			q, err := bbolt.Open(path, 0600, nil)
			require.NoError(t, err)
			require.NoError(t, q.Update(func(tx *bbolt.Tx) error {
				switch kind {
				case "future-format":
					return tx.Bucket(metadataBucket).Put(formatKey, []byte{CurrentDatabaseFormat + 1})
				case "missing-marker":
					return tx.Bucket(metadataBucket).Delete(jobReceiptVersionKey)
				case "missing-bucket":
					return tx.DeleteBucket(jobCompactReceiptsBucket)
				case "configuration-marker", "configuration-bucket":
					if err := tx.Bucket(metadataBucket).Put(formatKey, []byte{8}); err != nil {
						return err
					}
					if kind == "configuration-marker" {
						return tx.Bucket(metadataBucket).Delete(jobConfigurationVersionKey)
					}
					return tx.DeleteBucket(jobConfigurationUsageBucket)
				case "configuration-receipt-marker", "configuration-receipt-bucket":
					if err := tx.Bucket(metadataBucket).Put(formatKey, []byte{9}); err != nil {
						return err
					}
					if kind == "configuration-receipt-marker" {
						return tx.Bucket(metadataBucket).Delete(jobConfigurationReceiptVersionKey)
					}
					return tx.DeleteBucket(jobConfigurationReceiptIndexBucket)
				default:
					return tx.Bucket(metadataBucket).Put(channelStateVersionKey, []byte{5})
				}
			}))
			require.NoError(t, q.Close())
			before := compactionFileHash(t, path)
			_, err = CompactFile(context.Background(), path, FileCompactionOptions{})
			require.Error(t, err)
			require.Equal(t, before, compactionFileHash(t, path))
			noCompactionArtifacts(t, path)
		})
	}
}

func TestFileCompactionCopiesLargeValuesAcrossTransactions(t *testing.T) {
	path, _, _ := bloatedCompactionFile(t)
	q, err := bbolt.Open(path, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, q.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("opaque-extension")).Bucket([]byte("nested"))
		for i := range 20 {
			if err := b.Put([]byte(fmt.Sprintf("large-%03d", i)), bytes.Repeat([]byte{byte(i)}, 1<<20)); err != nil {
				return err
			}
		}
		return nil
	}))
	before, buckets, records, err := compactDigest(context.Background(), q)
	require.NoError(t, err)
	require.NoError(t, q.Close())
	result, err := CompactFile(context.Background(), path, FileCompactionOptions{})
	require.NoError(t, err)
	require.Equal(t, before, result.SHA256)
	require.Equal(t, buckets, result.Buckets)
	require.Equal(t, records, result.Records)
}

type cancelDuringCompaction struct {
	context.Context
	check func()
}

func (c cancelDuringCompaction) Err() error {
	c.check()
	return c.Context.Err()
}

func TestFileCompactionCancelledAfterCommittedCopyBatchKeepsOriginal(t *testing.T) {
	path, _, _ := bloatedCompactionFile(t)
	q, err := bbolt.Open(path, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, q.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("opaque-extension"))
		for i := range 20 {
			if err := b.Put([]byte(fmt.Sprintf("live-%03d", i)), bytes.Repeat([]byte{byte(i)}, 1<<20)); err != nil {
				return err
			}
		}
		return nil
	}))
	require.NoError(t, q.Close())
	before := compactionFileHash(t, path)
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	copying, committed := false, false
	ctx := cancelDuringCompaction{Context: base, check: func() {
		if !copying {
			return
		}
		paths, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".jobs.snap.compact-*"))
		require.NoError(t, err)
		for _, temporary := range paths {
			info, err := os.Stat(temporary)
			require.NoError(t, err)
			if info.Size() > 4<<20 {
				committed = true
				cancel()
			}
		}
	}}
	result, err := CompactFile(ctx, path, FileCompactionOptions{OnPhase: func(phase CompactionPhase) error {
		copying = phase == CompactionCopy
		return nil
	}})
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, committed, "cancellation must follow a committed staging transaction")
	require.False(t, result.Installed)
	require.Equal(t, before, compactionFileHash(t, path))
	noCompactionArtifacts(t, path)
}

func TestFileCompactionRejectsCopyWithChangedValue(t *testing.T) {
	path, _, _ := bloatedCompactionFile(t)
	before := compactionFileHash(t, path)
	result, err := CompactFile(context.Background(), path, FileCompactionOptions{OnPhase: func(phase CompactionPhase) error {
		if phase != CompactionVerify {
			return nil
		}
		paths, err := filepath.Glob(filepath.Join(filepath.Dir(path), ".jobs.snap.compact-*"))
		require.NoError(t, err)
		require.Len(t, paths, 1)
		data, err := os.ReadFile(paths[0])
		require.NoError(t, err)
		needle := []byte("frozen-state-that-must-survive-physical-compaction")
		require.Equal(t, 1, bytes.Count(data, needle))
		offset := bytes.Index(data, needle)
		f, err := os.OpenFile(paths[0], os.O_RDWR, 0600)
		require.NoError(t, err)
		_, err = f.WriteAt([]byte{'X'}, int64(offset))
		require.NoError(t, err)
		require.NoError(t, f.Sync())
		return f.Close()
	}})
	require.ErrorContains(t, err, "differs from its source")
	require.False(t, result.Installed)
	require.Equal(t, before, compactionFileHash(t, path))
	noCompactionArtifacts(t, path)
}

func TestFileCompactionPreservesOlderIndexedLayoutWithoutMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	q, err := bbolt.Open(path, 0600, nil)
	require.NoError(t, err)
	require.NoError(t, q.Update(func(tx *bbolt.Tx) error {
		if err := initializeMetadata(tx); err != nil {
			return err
		}
		for _, name := range jobBuckets {
			if _, err := tx.CreateBucket(name); err != nil {
				return err
			}
		}
		return writeAppliedIndex(tx, 47)
	}))
	addCompactionSlack(t, q)
	digest, buckets, records, err := compactDigest(context.Background(), q)
	require.NoError(t, err)
	require.NoError(t, q.Close())
	result, err := CompactFile(context.Background(), path, FileCompactionOptions{})
	require.NoError(t, err)
	require.True(t, result.Installed)
	require.Equal(t, byte(1), result.Format)
	require.Equal(t, uint64(47), result.AppliedIndex)
	require.Equal(t, digest, result.SHA256)
	require.Equal(t, buckets, result.Buckets)
	require.Equal(t, records, result.Records)
	q, err = bbolt.Open(path, 0600, &bbolt.Options{ReadOnly: true})
	require.NoError(t, err)
	defer q.Close()
	require.NoError(t, q.View(func(tx *bbolt.Tx) error {
		require.Nil(t, tx.Bucket(jobRunsBucket))
		require.Nil(t, tx.Bucket(metadataBucket).Get(channelStateVersionKey))
		return nil
	}))
}

func TestFileCompactionRefusesFilesystemFlagsInsteadOfDroppingThem(t *testing.T) {
	path, _, _ := bloatedCompactionFile(t)
	restore, err := setCompactionTestNoDump(path)
	if err != nil {
		t.Skipf("filesystem does not permit test flag: %v", err)
	}
	defer restore()
	before := compactionFileHash(t, path)
	result, err := CompactFile(context.Background(), path, FileCompactionOptions{})
	require.ErrorContains(t, err, "filesystem flags")
	require.False(t, result.Installed)
	require.Equal(t, before, compactionFileHash(t, path))
	noCompactionArtifacts(t, path)
}

func TestFileCompactionPreservesActivatedConfigurationUsageAndPolicy(t *testing.T) {
	path, _, _ := bloatedCompactionFile(t)
	q, err := NewDB(path)
	require.NoError(t, err)
	h := &channelHarness{t: t, db: q, index: q.LastAppliedIndex(), at: time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)}
	before := h.configurationUsage("tenant")
	policy := h.configurationLimits(JobConfigurationLimits{TenantID: "tenant", MaxDefinitions: 1, MaxTriggers: 1, MaxPayloadBytes: 1})
	require.NoError(t, q.Close())
	compacted, err := CompactFile(context.Background(), path, FileCompactionOptions{})
	require.NoError(t, err)
	require.Equal(t, byte(8), compacted.Format)
	q, err = NewDB(path)
	require.NoError(t, err)
	defer q.Close()
	usage, err := q.GetJobConfigurationUsage("tenant")
	require.NoError(t, err)
	require.Equal(t, before, *usage)
	actual, err := q.GetJobConfigurationLimits("tenant")
	require.NoError(t, err)
	require.Equal(t, policy, actual)
}

func TestFileCompactionPreservesConfigurationReceiptSweepAndRetry(t *testing.T) {
	path, _, _ := bloatedCompactionFile(t)
	q, err := NewDB(path)
	require.NoError(t, err)
	h := &channelHarness{t: t, db: q, index: q.LastAppliedIndex(), at: time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)}
	first := h.ok(jc(configurationSweep("tenant", "physical", 0, 1))).Job.ConfigurationReceiptCompaction
	require.False(t, first.Complete)
	index := h.index
	require.NoError(t, q.Close())
	result, err := CompactFile(context.Background(), path, FileCompactionOptions{})
	require.NoError(t, err)
	require.Equal(t, byte(9), result.Format)
	q, err = NewDB(path)
	require.NoError(t, err)
	defer q.Close()
	h.db = q
	h.index = index
	saved, err := q.GetJobConfigurationReceiptCompaction("tenant", "physical")
	require.NoError(t, err)
	require.Equal(t, first, saved)
	finishConfigurationSweep(h, first)
	resource, err := q.GetJobResource("bus")
	require.NoError(t, err)
	require.Equal(t, ResourceBlocked, resource.State)
}
