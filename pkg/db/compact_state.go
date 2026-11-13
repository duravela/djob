package db

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"

	"go.etcd.io/bbolt"
)

// Read feature markers without invoking migrations or rebuilding indexes.
func validateCompactSchema(tx *bbolt.Tx) (uint64, byte, error) {
	index, err := readAppliedIndex(tx)
	if err != nil {
		return 0, 0, err
	}
	meta := tx.Bucket(metadataBucket)
	format := meta.Get(formatKey)[0]
	for _, name := range jobBuckets {
		if tx.Bucket(name) == nil {
			return 0, 0, fmt.Errorf("missing legacy job bucket %q", name)
		}
	}
	channelVersion := byte(0)
	if v := meta.Get(channelStateVersionKey); len(v) == 1 {
		channelVersion = v[0]
	}
	rules := []struct {
		key      []byte
		buckets  [][]byte
		max      byte
		required bool
	}{
		{channelStateVersionKey, channelBuckets, 4, format >= 2},
		{schedulingVersionKey, schedulingBuckets, 1, false},
		{deadlineVersionKey, [][]byte{runDeadlinesBucket}, 1, channelVersion >= 3},
		{retryVersionKey, [][]byte{retryDueBucket, pendingCountsBucket}, 1, channelVersion >= 4},
		{jobStateVersionKey, jobStateBuckets, 1, format >= 2},
		{jobTriggerVersionKey, jobTriggerBuckets, 1, format >= 3},
		{jobActivationVersionKey, [][]byte{jobActivationsBucket}, 1, format >= 4},
		{jobQuotaVersionKey, [][]byte{jobLimitsBucket, jobUsageBucket}, 1, format >= 5},
		{jobRetentionVersionKey, jobRetentionBuckets, 1, format >= 6},
		{jobReceiptVersionKey, jobReceiptBuckets, 1, format >= 7},
		{jobConfigurationVersionKey, jobConfigurationBuckets, 1, format >= 8},
		{jobConfigurationReceiptVersionKey, jobConfigurationReceiptBuckets, 1, format >= 9},
		{jobTriggerRetirementVersionKey, [][]byte{jobTriggerRetirementsBucket}, 1, format >= 10},
		{jobConfigurationRetirementVersionKey, jobConfigurationRetirementBuckets, 1, format >= 11},
		{workerEnrollmentVersionKey, workerEnrollmentBuckets, 1, format >= 13},
		{workerDeploymentVersionKey, workerDeploymentBuckets, 1, format >= 14},
		{workerReleaseVersionKey, [][]byte{workerReleaseUsageBucket}, 1, format >= 17},
	}
	for _, rule := range rules {
		v := meta.Get(rule.key)
		if v == nil && rule.required {
			return 0, 0, fmt.Errorf("missing feature marker %q", rule.key)
		}
		if v != nil && (len(v) != 1 || v[0] < 1 || v[0] > rule.max) {
			return 0, 0, fmt.Errorf("unsupported feature marker %q", rule.key)
		}
		for _, name := range rule.buckets {
			b := tx.Bucket(name)
			if v != nil && b == nil {
				return 0, 0, fmt.Errorf("feature %q is missing bucket %q", rule.key, name)
			}
			if v == nil && b != nil {
				if key, _ := b.Cursor().First(); key != nil {
					return 0, 0, fmt.Errorf("bucket %q has data without feature marker %q", name, rule.key)
				}
			}
		}
	}
	return index, format, nil
}

// Every bucket, sequence, key and opaque value participates in the digest.
// Physical page placement and freelist data deliberately do not.
func compactDigest(ctx context.Context, database *bbolt.DB) (string, uint64, uint64, error) {
	h := sha256.New()
	_, _ = h.Write([]byte("djob-physical-compaction-v1\x00"))
	var buckets, records uint64
	err := database.View(func(tx *bbolt.Tx) error {
		var visit func([]byte, *bbolt.Bucket) error
		visit = func(name []byte, b *bbolt.Bucket) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			buckets++
			_, _ = h.Write([]byte{'B'})
			compactHashBytes(h, name)
			var seq [8]byte
			binary.BigEndian.PutUint64(seq[:], b.Sequence())
			_, _ = h.Write(seq[:])
			if err := b.ForEach(func(k, v []byte) error {
				if err := ctx.Err(); err != nil {
					return err
				}
				if v == nil {
					return visit(k, b.Bucket(k))
				}
				records++
				_, _ = h.Write([]byte{'V'})
				compactHashBytes(h, k)
				compactHashBytes(h, v)
				return nil
			}); err != nil {
				return err
			}
			_, _ = h.Write([]byte{'E'})
			return nil
		}
		return tx.ForEach(visit)
	})
	return hex.EncodeToString(h.Sum(nil)), buckets, records, err
}

func compactHashBytes(h hash.Hash, data []byte) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(data)))
	_, _ = h.Write(size[:])
	_, _ = h.Write(data)
}

func checkCompactStructure(ctx context.Context, database *bbolt.DB) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return database.View(func(tx *bbolt.Tx) error {
		var first error
		// Drain the checker even after cancellation: its goroutine must finish
		// reading the transaction before the mapping can be closed.
		for err := range tx.Check(bbolt.WithKVStringer(compactionDiagnostics{})) {
			if first == nil {
				first = fmt.Errorf("database integrity check: %w", err)
			}
		}
		if first != nil {
			return first
		}
		return ctx.Err()
	})
}

type compactionDiagnostics struct{}

func (compactionDiagnostics) KeyToString(key []byte) string {
	return fmt.Sprintf("key(%d bytes)", len(key))
}
func (compactionDiagnostics) ValueToString(value []byte) string {
	return fmt.Sprintf("value(%d bytes)", len(value))
}

func copyCompactState(ctx context.Context, dst, src *bbolt.DB) (err error) {
	const maxTransactionBytes = 8 << 20
	var target *bbolt.Tx
	target, err = dst.Begin(true)
	if err != nil {
		return err
	}
	defer func() {
		if target != nil {
			_ = target.Rollback()
		}
	}()
	var written int64
	err = src.View(func(source *bbolt.Tx) error {
		var visit func([][]byte, []byte, []byte, uint64) error
		visit = func(parents [][]byte, key, value []byte, sequence uint64) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			size := int64(len(key)) + int64(len(value))
			if written > 0 && written+size > maxTransactionBytes {
				if err := target.Commit(); err != nil {
					return err
				}
				target, err = dst.Begin(true)
				if err != nil {
					return err
				}
				written = 0
			}
			written += size
			var bucket *bbolt.Bucket
			if len(parents) == 0 {
				bucket, err = target.CreateBucket(key)
			} else {
				bucket = target.Bucket(parents[0])
				for _, parent := range parents[1:] {
					bucket = bucket.Bucket(parent)
				}
				bucket.FillPercent = 1
				if value != nil {
					return bucket.Put(key, value)
				}
				bucket, err = bucket.CreateBucket(key)
			}
			if err != nil {
				return err
			}
			if err := bucket.SetSequence(sequence); err != nil {
				return err
			}
			parents = append(parents, bytes.Clone(key))
			var original *bbolt.Bucket
			for i, parent := range parents {
				if i == 0 {
					original = source.Bucket(parent)
				} else {
					original = original.Bucket(parent)
				}
			}
			return original.ForEach(func(k, v []byte) error {
				var seq uint64
				if v == nil {
					seq = original.Bucket(k).Sequence()
				}
				return visit(parents, k, v, seq)
			})
		}
		return source.ForEach(func(name []byte, b *bbolt.Bucket) error { return visit(nil, name, nil, b.Sequence()) })
	})
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return target.Commit()
}
