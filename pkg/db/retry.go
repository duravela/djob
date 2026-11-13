package db

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"time"

	"go.etcd.io/bbolt"
)

type RetryClass string

// RetryNotSent attests a transient failure before ANY request bytes were
// accepted by the transport. It must never be inferred from a read operation,
// an uncertain response, or a closed TCP connection after possible delivery.
const RetryNotSent RetryClass = "not_sent"

type RetryPolicy struct {
	MaxAttempts    uint32 // Includes the first attempt. Nil policy disables retries.
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

func (p *RetryPolicy) Clone() *RetryPolicy {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}

func (p *RetryPolicy) Validate() error {
	if p != nil && (p.MaxAttempts < 1 || p.MaxAttempts > 32 || p.InitialBackoff <= 0 || p.MaxBackoff < p.InitialBackoff || p.MaxBackoff > time.Hour) {
		return reject("invalid", "retry policy requires 1..32 total attempts and positive backoffs with initial <= maximum <= 1 hour")
	}
	return nil
}

// Backoff uses capped doubling without overflow. Completion's explicit command
// time determines RetryAt identically on every replica; no timers or randomness
// are consulted during state application.
func (p *RetryPolicy) Backoff(failedAttempt uint64) time.Duration {
	if p == nil || failedAttempt == 0 {
		return 0
	}
	delay := p.InitialBackoff
	for n := uint64(1); n < failedAttempt && delay < p.MaxBackoff; n++ {
		if delay > p.MaxBackoff/2 {
			return p.MaxBackoff
		}
		delay *= 2
	}
	return delay
}

var (
	retryDueBucket      = []byte("retry_due")
	pendingCountsBucket = []byte("pending_counts")
	retryVersionKey     = []byte("retry_state_version")
)

// Retry-enabled work reserves its backlog slot during active attempts and
// backoff. Other active work keeps the historical ready-queue capacity behavior.
func pendingRun(run Run) bool {
	return run.State == RunReady || run.State == RunRetryWait || (run.State == RunRunning && run.RetryPolicy != nil)
}

func unstartedRun(run Run) bool { return run.State == RunReady || run.State == RunRetryWait }

func initializeRetryState(tx *bbolt.Tx) error {
	metadata := tx.Bucket(metadataBucket)
	if version := metadata.Get(retryVersionKey); version != nil {
		if !bytes.Equal(version, []byte{1}) || tx.Bucket(retryDueBucket) == nil || tx.Bucket(pendingCountsBucket) == nil {
			return fmt.Errorf("missing or unsupported retry state")
		}
		// Before v7 activation an older binary may still open this directory.
		// It maintains ready counts but cannot maintain the new derived counter.
		// Rebuild until the format marker excludes those older writers.
		if metadata.Get(channelStateVersionKey)[0] < 4 {
			return rebuildLegacyPendingCounts(tx)
		}
		return nil
	}
	if metadata.Get(channelStateVersionKey)[0] >= 4 {
		return fmt.Errorf("missing retry format marker")
	}
	for _, name := range [][]byte{retryDueBucket, pendingCountsBucket} {
		if b := tx.Bucket(name); b != nil && b.Stats().KeyN != 0 {
			return fmt.Errorf("retry data has no format marker")
		}
		if _, err := tx.CreateBucketIfNotExists(name); err != nil {
			return err
		}
	}
	if err := tx.Bucket(runsBucket).ForEach(func(_, data []byte) error {
		var run Run
		if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&run); err != nil {
			return err
		}
		if pendingRun(run) {
			if err := changePendingCount(tx, run.ChannelID, 1); err != nil {
				return err
			}
		}
		if run.State == RunRetryWait {
			return tx.Bucket(retryDueBucket).Put(timedKey(run.RetryAt, run.ID), []byte(run.ID))
		}
		return nil
	}); err != nil {
		return err
	}
	return metadata.Put(retryVersionKey, []byte{1})
}

func rebuildLegacyPendingCounts(tx *bbolt.Tx) error {
	if tx.Bucket(retryDueBucket).Stats().KeyN != 0 {
		return fmt.Errorf("retry data exists before format activation")
	}
	if err := tx.DeleteBucket(pendingCountsBucket); err != nil {
		return err
	}
	bucket, err := tx.CreateBucket(pendingCountsBucket)
	if err != nil {
		return err
	}
	return tx.Bucket(readyCountsBucket).ForEach(func(key, value []byte) error {
		if len(value) != 8 {
			return fmt.Errorf("invalid ready-run count during retry upgrade")
		}
		return bucket.Put(key, value)
	})
}

func pendingCount(tx *bbolt.Tx, channel string) (uint64, error) {
	data := tx.Bucket(pendingCountsBucket).Get([]byte(channel))
	if data == nil {
		return 0, nil
	}
	if len(data) != 8 {
		return 0, fmt.Errorf("invalid pending-run count")
	}
	return binary.BigEndian.Uint64(data), nil
}

func changePendingCount(tx *bbolt.Tx, channel string, delta int) error {
	count, err := pendingCount(tx, channel)
	if err != nil {
		return err
	}
	if delta < 0 {
		if count == 0 {
			return fmt.Errorf("pending-run count underflow")
		}
		count--
	} else {
		if count == ^uint64(0) {
			return fmt.Errorf("pending-run count overflow")
		}
		count++
	}
	var data [8]byte
	binary.BigEndian.PutUint64(data[:], count)
	return tx.Bucket(pendingCountsBucket).Put([]byte(channel), data[:])
}

func (q *DB) PendingRunCount(channel string) (uint64, error) {
	var count uint64
	err := q.db.View(func(tx *bbolt.Tx) error { var err error; count, err = pendingCount(tx, channel); return err })
	return count, err
}

func (q *DB) ListDueRetries(at time.Time, after []byte, limit int) (RunPage, error) {
	page := RunPage{}
	if err := queryLimit(limit); err != nil {
		return page, err
	}
	err := q.db.View(func(tx *bbolt.Tx) error {
		cursor := tx.Bucket(retryDueBucket).Cursor()
		for key, value := pageStart(cursor, after); key != nil && len(page.Runs) < limit; key, value = cursor.Next() {
			if len(key) < 12 {
				return fmt.Errorf("invalid retry due index key")
			}
			if bytes.Compare(key[:12], orderedTime(at)) > 0 {
				break
			}
			run, err := readRecord[Run](tx, runsBucket, string(value))
			if err != nil {
				return err
			}
			if run.State != RunRetryWait || run.RetryAt.IsZero() || !bytes.Equal(key, timedKey(run.RetryAt, run.ID)) {
				return fmt.Errorf("inconsistent retry due index")
			}
			page.Runs = append(page.Runs, *run)
			page.NextCursor = bytes.Clone(key)
		}
		return nil
	})
	return page, err
}

func releaseRetry(tx *bbolt.Tx, command StateCommand, result *StateResult) error {
	c := command.ReleaseRetry
	if !validIdentity(c.RunID) || !validIdentity(c.AttemptID) || !validDeadline(c.RetryAt) {
		return reject("invalid", "retry release requires run, attempt and retry time")
	}
	run, err := readRecord[Run](tx, runsBucket, c.RunID)
	if err != nil {
		return err
	}
	if run.LastAttemptID != c.AttemptID {
		return reject("retry_conflict", "retry attempt changed")
	}
	attempt, err := readRecord[Attempt](tx, attemptsBucket, c.AttemptID)
	if err != nil {
		return err
	}
	if attempt.RunID != run.ID || !attempt.RetryAt.Equal(c.RetryAt) {
		return reject("retry_conflict", "retry plan changed")
	}
	if command.At.Before(c.RetryAt) {
		return reject("not_due", "retry backoff has not elapsed")
	}
	if run.State == RunRetryWait {
		if run.RetryPolicy == nil || run.AttemptCount >= uint64(run.RetryPolicy.MaxAttempts) || attempt.Outcome != AttemptFailed || attempt.RetryClass != RetryNotSent || !attempt.Quiescent {
			return fmt.Errorf("waiting retry lacks a recoverable no-send failure")
		}
		if !run.RetryAt.Equal(c.RetryAt) || run.ActiveAttemptID != "" {
			return fmt.Errorf("inconsistent waiting retry")
		}
		run.State = RunReady
		run.ReadySince = command.At
		if run.CancellationRequested() {
			run.State = RunCancelled
		} else if run.StartDeadlinePassed(command.At) {
			run.State = RunExpired
		}
		run.RetryAt, run.LastTransition = time.Time{}, later(run.LastTransition, command.At)
		if err := writeRunRecord(tx, *run); err != nil {
			return err
		}
	}
	result.Run, result.Attempt = run, attempt
	return nil
}
