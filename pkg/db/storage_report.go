package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.etcd.io/bbolt"
)

type StorageCategoryStats struct {
	Category        string `json:"category"`
	Buckets         uint64 `json:"buckets,string"`
	Records         uint64 `json:"records,string"`
	BucketNameBytes uint64 `json:"bucket_name_bytes,string"`
	KeyBytes        uint64 `json:"key_bytes,string"`
	ValueBytes      uint64 `json:"value_bytes,string"`
}

// FileStorageReport accounts for live serialized state, not page allocation or
// unique application payloads. Categories are fixed; no record contents or IDs
// are returned. Nested extension buckets inherit their root bucket's category.
type FileStorageReport struct {
	Path            string                 `json:"path"`
	FileBytes       int64                  `json:"file_bytes"`
	PageSize        int                    `json:"page_size"`
	AppliedIndex    uint64                 `json:"applied_index,string"`
	Format          byte                   `json:"database_format"`
	Buckets         uint64                 `json:"buckets,string"`
	Records         uint64                 `json:"records,string"`
	BucketNameBytes uint64                 `json:"bucket_name_bytes,string"`
	KeyBytes        uint64                 `json:"key_bytes,string"`
	ValueBytes      uint64                 `json:"value_bytes,string"`
	Categories      []StorageCategoryStats `json:"categories"`
}

var storageCategoryBuckets = [...]struct {
	name    string
	buckets [][]byte
}{
	{"metadata", [][]byte{metadataBucket}},
	{"legacy_jobs", jobBuckets},
	{"gateway_configuration", [][]byte{channelsBucket}},
	{"gateway_runs", [][]byte{runsBucket}},
	{"gateway_history", [][]byte{attemptsBucket}},
	{"gateway_schedules", [][]byte{schedulesBucket}},
	{"tenant_configuration", [][]byte{jobTenantsBucket, jobGroupsBucket, jobDefinitionsBucket, jobBindingsBucket, jobGrantsBucket, jobTriggersBucket, jobLimitsBucket, jobRetentionBucket, jobConfigurationLimitsBucket}},
	{"shared_resources", [][]byte{jobResourcesBucket}},
	{"job_runs", [][]byte{jobRunsBucket}},
	{"job_history", [][]byte{jobAttemptsBucket, jobActionsBucket, jobEventsBucket}},
	{"job_activations", [][]byte{jobActivationsBucket}},
	{"request_receipts", [][]byte{requestsBucket}},
	{"compact_receipts", [][]byte{jobCompactReceiptsBucket, jobActivationReceiptsBucket}},
	{"identity_ledgers", [][]byte{occurrencesBucket, jobOccurrencesBucket}},
	{"worker_identities", workerEnrollmentBuckets},
	{"worker_deployments", append(append([][]byte(nil), workerDeploymentBuckets...), workerReleaseUsageBucket)},
	{"audit_records", [][]byte{resolutionsBucket, jobResolutionsBucket, jobDeletionBucket, jobReceiptCompactionBucket, jobConfigurationReceiptCompactionsBucket, jobTriggerRetirementsBucket, jobConfigurationRetirementsBucket}},
	{"indexes", [][]byte{workerReleaseRunsBucket, scheduleDueBucket, readyRunsBucket, readyCountsBucket, readyByChannelBucket, runDeadlinesBucket, retryDueBucket, pendingCountsBucket,
		jobEventKeysBucket, jobReadyBucket, jobLeasesBucket, jobWaitsBucket, jobCountersBucket, jobTriggerDueBucket, jobTriggerPendingBucket,
		jobUsageBucket, jobConfigurationUsageBucket, jobRetentionIndexBucket, jobPurgePendingBucket, jobReceiptRunIndexBucket, jobReceiptPendingBucket,
		jobConfigurationReceiptIndexBucket, jobConfigurationReceiptCountersBucket, jobConfigurationReceiptPendingBucket, jobConfigurationReferencesBucket}},
	{"other", nil},
}

func storageCategoryFor(name []byte) int {
	for category, spec := range storageCategoryBuckets {
		for _, known := range spec.buckets {
			if bytes.Equal(name, known) {
				return category
			}
		}
	}
	return len(storageCategoryBuckets) - 1
}

// ReportStorageFile reads an existing indexed database without initializing
// features or rebuilding indexes. The caller must hold the member's stable
// data-directory lock, excluding runtime and physical replacement throughout.
// node.ReportStoreStorage provides that lock for the application's layout.
func ReportStorageFile(ctx context.Context, path string) (FileStorageReport, error) {
	return reportStorageFile(ctx, path, false)
}

// VerifyStorageFile also checks bbolt's page structure. Like ReportStorageFile,
// it never initializes features, migrates state or repairs indexes. The caller
// must exclude writers and physical replacement for the duration of the check.
func VerifyStorageFile(ctx context.Context, path string) (FileStorageReport, error) {
	return reportStorageFile(ctx, path, true)
}

func reportStorageFile(ctx context.Context, path string, checkStructure bool) (result FileStorageReport, err error) {
	if err := ctx.Err(); err != nil {
		return result, err
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return result, err
	}
	directory, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return result, err
	}
	path = filepath.Join(directory, filepath.Base(absolute))
	info, err := os.Lstat(path)
	if err != nil {
		return result, fmt.Errorf("inspect storage-report source: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() == 0 {
		return result, fmt.Errorf("storage report requires an existing nonempty regular database file")
	}
	source, err := bbolt.Open(path, 0400, &bbolt.Options{
		ReadOnly: true,
		Timeout:  100 * time.Millisecond,
		OpenFile: func(name string, flags int, mode os.FileMode) (*os.File, error) {
			f, err := os.OpenFile(name, flags&^os.O_CREATE, mode)
			if err != nil {
				return nil, err
			}
			opened, err := f.Stat()
			if err != nil || !os.SameFile(info, opened) {
				_ = f.Close()
				return nil, fmt.Errorf("storage-report source changed while opening")
			}
			return f, nil
		},
	})
	if err != nil {
		return result, fmt.Errorf("open storage-report source (stop database writers first): %w", err)
	}
	defer func() {
		err = errors.Join(err, source.Close())
		if err != nil {
			result = FileStorageReport{}
		}
	}()
	if checkStructure {
		if err := checkCompactStructure(ctx, source); err != nil {
			return result, err
		}
	}
	result.Path, result.FileBytes, result.PageSize = path, info.Size(), source.Info().PageSize
	result.Categories = make([]StorageCategoryStats, len(storageCategoryBuckets))
	for i, spec := range storageCategoryBuckets {
		result.Categories[i].Category = spec.name
	}
	err = source.View(func(tx *bbolt.Tx) error {
		var err error
		result.AppliedIndex, result.Format, err = validateCompactSchema(tx)
		if err != nil {
			return err
		}
		var visit func(*bbolt.Bucket, []byte, int, int) error
		visit = func(b *bbolt.Bucket, name []byte, category, depth int) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			if depth > 64 {
				return fmt.Errorf("storage report exceeds maximum nested bucket depth")
			}
			stats := &result.Categories[category]
			stats.Buckets++
			stats.BucketNameBytes += uint64(len(name))
			cursor := b.Cursor()
			for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
				if err := ctx.Err(); err != nil {
					return err
				}
				if value == nil {
					if child := b.Bucket(key); child != nil {
						if err := visit(child, key, category, depth+1); err != nil {
							return err
						}
						continue
					}
				}
				stats.Records++
				stats.KeyBytes += uint64(len(key))
				stats.ValueBytes += uint64(len(value))
			}
			return nil
		}
		return tx.ForEach(func(name []byte, b *bbolt.Bucket) error {
			return visit(b, name, storageCategoryFor(name), 1)
		})
	})
	if err != nil {
		return FileStorageReport{}, err // A failed scan never returns a partial census.
	}
	for _, category := range result.Categories {
		result.Buckets += category.Buckets
		result.Records += category.Records
		result.BucketNameBytes += category.BucketNameBytes
		result.KeyBytes += category.KeyBytes
		result.ValueBytes += category.ValueBytes
	}
	if err := ctx.Err(); err != nil {
		return FileStorageReport{}, err
	}
	return result, nil
}
