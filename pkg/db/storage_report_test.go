//go:build darwin || linux

package db

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func TestStorageReportCountsRawStateWithoutModifyingIt(t *testing.T) {
	path, snapshot, _ := bloatedCompactionFile(t)
	before := compactionFileHash(t, path)
	info, err := os.Stat(path)
	require.NoError(t, err)
	report, err := ReportStorageFile(context.Background(), path)
	require.NoError(t, err)
	require.Equal(t, before, compactionFileHash(t, path))
	after, err := os.Stat(path)
	require.NoError(t, err)
	require.True(t, os.SameFile(info, after))
	require.Equal(t, info.ModTime(), after.ModTime())
	require.Equal(t, info.Mode(), after.Mode())
	require.Equal(t, info.Size(), report.FileBytes)
	require.Positive(t, report.PageSize)
	require.Equal(t, snapshot.LastIndex, report.AppliedIndex)
	require.Equal(t, byte(7), report.Format)
	var expected StorageCategoryStats
	var count func(SnapshotBucket)
	count = func(bucket SnapshotBucket) {
		expected.Buckets++
		expected.BucketNameBytes += uint64(len(bucket.Name))
		for _, record := range bucket.Records {
			expected.Records++
			expected.KeyBytes += uint64(len(record.Key))
			expected.ValueBytes += uint64(len(record.Value))
		}
		for _, child := range bucket.Buckets {
			count(child)
		}
	}
	for _, bucket := range snapshot.Buckets {
		count(bucket)
	}
	require.Equal(t, expected.Buckets, report.Buckets)
	require.Equal(t, expected.Records, report.Records)
	require.Equal(t, expected.BucketNameBytes, report.BucketNameBytes)
	require.Equal(t, expected.KeyBytes, report.KeyBytes)
	require.Equal(t, expected.ValueBytes, report.ValueBytes)
	var sum StorageCategoryStats
	for _, category := range report.Categories {
		sum.Buckets += category.Buckets
		sum.Records += category.Records
		sum.BucketNameBytes += category.BucketNameBytes
		sum.KeyBytes += category.KeyBytes
		sum.ValueBytes += category.ValueBytes
	}
	require.Equal(t, expected, sum)
	other := report.Categories[len(report.Categories)-1]
	require.Equal(t, StorageCategoryStats{
		Category: "other", Buckets: 2, Records: 2,
		BucketNameBytes: uint64(len("opaque-extension") + len("nested")),
		KeyBytes:        2 + uint64(len("payload")),
		ValueBytes:      uint64(len("frozen-state-that-must-survive-physical-compaction")),
	}, other, "nested buckets and empty values must be counted exactly once")
	encoded, err := json.Marshal(report)
	require.NoError(t, err)
	for _, private := range []string{"opaque-extension", "nested", "frozen-state-that-must-survive-physical-compaction", "possible remote effect", "unknown-attempt"} {
		require.NotContains(t, string(encoded), private)
	}

	compacted, err := CompactFile(context.Background(), path, FileCompactionOptions{})
	require.NoError(t, err)
	require.True(t, compacted.Installed)
	shrunken, err := ReportStorageFile(context.Background(), path)
	require.NoError(t, err)
	require.Less(t, shrunken.FileBytes, report.FileBytes)
	shrunken.FileBytes = report.FileBytes
	require.Equal(t, report, shrunken, "page reclamation must not alter the serialized live-state census")
}

func TestStorageReportCategoriesCoverCurrentSchema(t *testing.T) {
	path := filepath.Join(t.TempDir(), "current.db")
	q, err := NewDB(path)
	require.NoError(t, err)
	seen := make(map[string]bool)
	for _, category := range storageCategoryBuckets {
		for _, name := range category.buckets {
			require.False(t, seen[string(name)], "a bucket must belong to only one category: %s", name)
			seen[string(name)] = true
		}
	}
	require.NoError(t, q.db.View(func(tx *bbolt.Tx) error {
		return tx.ForEach(func(name []byte, _ *bbolt.Bucket) error {
			require.True(t, seen[string(name)], "classify every current root bucket: %s", name)
			return nil
		})
	}))
	require.NoError(t, q.Close())
	report, err := ReportStorageFile(context.Background(), path)
	require.NoError(t, err)
	require.Zero(t, report.Categories[len(report.Categories)-1].Buckets)
}

func TestStorageReportReadsOlderLayoutAndPreservesLargeIndex(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	q, err := bbolt.Open(path, 0600, nil)
	require.NoError(t, err)
	const index = uint64(1<<63 + 9)
	require.NoError(t, q.Update(func(tx *bbolt.Tx) error {
		if err := initializeMetadata(tx); err != nil {
			return err
		}
		for _, name := range jobBuckets {
			if _, err := tx.CreateBucket(name); err != nil {
				return err
			}
		}
		return writeAppliedIndex(tx, index)
	}))
	require.NoError(t, q.Close())
	require.NoError(t, os.Chmod(path, 0400))
	before := compactionFileHash(t, path)
	report, err := ReportStorageFile(context.Background(), path)
	require.NoError(t, err)
	require.Equal(t, index, report.AppliedIndex)
	require.Equal(t, byte(1), report.Format)
	require.Equal(t, uint64(len(jobBuckets)+1), report.Buckets)
	require.Equal(t, uint64(2), report.Records)
	require.Equal(t, before, compactionFileHash(t, path), "reporting must not initialize any feature or rebuild an index")
	verified, err := VerifyStorageFile(context.Background(), path)
	require.NoError(t, err)
	require.Equal(t, report, verified)
	require.Equal(t, before, compactionFileHash(t, path), "verification must preserve the older layout too")
	encoded, err := json.Marshal(report)
	require.NoError(t, err)
	var raw map[string]any
	require.NoError(t, json.Unmarshal(encoded, &raw))
	require.Equal(t, "9223372036854775817", raw["applied_index"])
	require.Equal(t, "2", raw["records"])
	require.Equal(t, "0", raw["categories"].([]any)[len(report.Categories)-1].(map[string]any)["value_bytes"])
}

func TestStorageReportRejectsInvalidSourcesWithoutPartialResults(t *testing.T) {
	for _, kind := range []string{"missing", "empty", "symlink", "writer", "future_format", "invalid_index", "missing_bucket", "missing_marker", "unmarked_data"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "source.db")
			if kind == "missing" {
				result, err := ReportStorageFile(context.Background(), path)
				require.Error(t, err)
				require.Equal(t, FileStorageReport{}, result)
				_, err = os.Stat(path)
				require.True(t, os.IsNotExist(err))
				return
			}
			q, err := NewDB(path)
			require.NoError(t, err)
			require.NoError(t, q.db.Update(func(tx *bbolt.Tx) error {
				switch kind {
				case "future_format":
					return tx.Bucket(metadataBucket).Put(formatKey, []byte{CurrentDatabaseFormat + 1})
				case "invalid_index":
					return tx.Bucket(metadataBucket).Put(appliedKey, []byte{1})
				case "missing_bucket":
					return tx.DeleteBucket(jobResourcesBucket)
				case "missing_marker":
					if err := tx.Bucket(metadataBucket).Put(formatKey, []byte{CurrentDatabaseFormat}); err != nil {
						return err
					}
					return tx.Bucket(metadataBucket).Delete(jobConfigurationRetirementVersionKey)
				case "unmarked_data":
					if err := tx.Bucket(metadataBucket).Delete(jobStateVersionKey); err != nil {
						return err
					}
					return tx.Bucket(jobRunsBucket).Put([]byte("unmarked-private-id"), []byte("private input"))
				}
				return nil
			}))
			if kind == "writer" {
				defer q.Close()
			} else {
				require.NoError(t, q.Close())
			}
			if kind == "empty" {
				require.NoError(t, os.Truncate(path, 0))
			}
			if kind == "symlink" {
				alias := filepath.Join(t.TempDir(), "alias.db")
				require.NoError(t, os.Symlink(path, alias))
				path = alias
			}
			before := compactionFileHash(t, path)
			result, err := ReportStorageFile(context.Background(), path)
			require.Error(t, err)
			require.Equal(t, FileStorageReport{}, result)
			require.Equal(t, before, compactionFileHash(t, path))
			verified, err := VerifyStorageFile(context.Background(), path)
			require.Error(t, err)
			require.Equal(t, FileStorageReport{}, verified)
			require.Equal(t, before, compactionFileHash(t, path))
		})
	}
}

func TestVerifyStorageFilePreservesStateAndReleasesLocksOnCancellation(t *testing.T) {
	path, snapshot, _ := bloatedCompactionFile(t)
	before := compactionFileHash(t, path)
	info, err := os.Stat(path)
	require.NoError(t, err)
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	checks := 0
	ctx := cancelDuringCompaction{Context: base, check: func() {
		checks++
		if checks == 3 {
			cancel() // Cancel as the structural walk completes, before the census.
		}
	}}
	report, err := VerifyStorageFile(ctx, path)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, FileStorageReport{}, report)
	report, err = VerifyStorageFile(context.Background(), path)
	require.NoError(t, err, "the cancelled verification must release its read transaction and file lock")
	require.Equal(t, snapshot.LastIndex, report.AppliedIndex)
	require.Equal(t, byte(7), report.Format)
	after, err := os.Stat(path)
	require.NoError(t, err)
	require.True(t, os.SameFile(info, after))
	require.Equal(t, info.ModTime(), after.ModTime())
	require.Equal(t, info.Mode(), after.Mode())
	require.Equal(t, before, compactionFileHash(t, path))
}

func TestStorageReportCancellationDiscardsPartialCounts(t *testing.T) {
	path, _, _ := bloatedCompactionFile(t)
	before := compactionFileHash(t, path)
	base, cancel := context.WithCancel(context.Background())
	defer cancel()
	checks := 0
	ctx := cancelDuringCompaction{Context: base, check: func() {
		checks++
		if checks == 20 {
			cancel()
		}
	}}
	result, err := ReportStorageFile(ctx, path)
	require.ErrorIs(t, err, context.Canceled)
	require.GreaterOrEqual(t, checks, 20, "cancel after traversal has begun")
	require.Equal(t, FileStorageReport{}, result)
	require.Equal(t, before, compactionFileHash(t, path))
	_, err = ReportStorageFile(context.Background(), path)
	require.NoError(t, err, "a cancelled report must release its database read lock")
}
