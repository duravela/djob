package db

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"time"

	"go.etcd.io/bbolt"
)

var runDeadlinesBucket = []byte("run_start_deadlines")
var deadlineVersionKey = []byte("deadline_state_version")

func validDeadline(at time.Time) bool {
	return !at.IsZero() && at.UTC().Year() >= 1 && at.UTC().Year() <= 9999
}

// The feature marker distinguishes an older complete snapshot from a damaged
// new snapshot whose deadline index was lost.
func initializeDeadlineState(tx *bbolt.Tx) error {
	metadata := tx.Bucket(metadataBucket)
	if version := metadata.Get(deadlineVersionKey); version != nil {
		if !bytes.Equal(version, []byte{1}) || tx.Bucket(runDeadlinesBucket) == nil {
			return fmt.Errorf("missing or unsupported run deadline state")
		}
		return nil
	}
	if tx.Bucket(metadataBucket).Get(channelStateVersionKey)[0] >= 3 {
		return fmt.Errorf("missing run deadline format marker")
	}
	if bucket := tx.Bucket(runDeadlinesBucket); bucket != nil && bucket.Stats().KeyN != 0 {
		return fmt.Errorf("run deadline data has no format marker")
	}
	bucket, err := tx.CreateBucketIfNotExists(runDeadlinesBucket)
	if err != nil {
		return err
	}
	if err := tx.Bucket(runsBucket).ForEach(func(_, data []byte) error {
		var run Run
		if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&run); err != nil {
			return err
		}
		if unstartedRun(run) && !run.StartDeadline.IsZero() {
			return bucket.Put(timedKey(run.StartDeadline, run.ID), []byte(run.ID))
		}
		return nil
	}); err != nil {
		return err
	}
	return metadata.Put(deadlineVersionKey, []byte{1})
}

func expireRun(tx *bbolt.Tx, command StateCommand, result *StateResult) error {
	c := command.ExpireRun
	if !validIdentity(c.RunID) || !validDeadline(c.StartDeadline) {
		return reject("invalid", "run ID and nonzero start deadline are required")
	}
	run, err := readRecord[Run](tx, runsBucket, c.RunID)
	if err != nil {
		return err
	}
	if !run.StartDeadline.Equal(c.StartDeadline) {
		return reject("deadline_conflict", "run start deadline does not match")
	}
	if !run.StartDeadlinePassed(command.At) {
		return reject("not_due", "run start deadline has not passed")
	}
	if run.State == RunRetryWait && !c.RetryAware {
		return reject("invalid", "waiting retry requires retry-aware expiry")
	}
	// Never interrupt a claimed exchange or rewrite an unknown/confirmed effect.
	if unstartedRun(*run) {
		if run.ActiveAttemptID != "" {
			return fmt.Errorf("ready run has an active attempt")
		}
		run.State, run.LastTransition = RunExpired, later(run.LastTransition, command.At)
		run.RetryAt = time.Time{}
		if err := writeRunRecord(tx, *run); err != nil {
			return err
		}
	}
	result.Run = run
	return nil
}

// ListExpiredReadyRuns uses a bounded due index, including work behind a busy or
// blocked gateway. Local reads select candidates; the replicated command checks
// the immutable deadline and current state again.
func (q *DB) ListExpiredReadyRuns(at time.Time, after []byte, limit int) (RunPage, error) {
	page := RunPage{}
	if err := queryLimit(limit); err != nil {
		return page, err
	}
	err := q.db.View(func(tx *bbolt.Tx) error {
		cursor := tx.Bucket(runDeadlinesBucket).Cursor()
		for key, value := pageStart(cursor, after); key != nil && len(page.Runs) < limit; key, value = cursor.Next() {
			if len(key) < 12 {
				return fmt.Errorf("invalid run deadline index key")
			}
			if bytes.Compare(key[:12], orderedTime(at)) > 0 {
				break
			}
			run, err := readRecord[Run](tx, runsBucket, string(value))
			if err != nil {
				return err
			}
			if !unstartedRun(*run) || !run.StartDeadlinePassed(at) || !bytes.Equal(key, timedKey(run.StartDeadline, run.ID)) {
				return fmt.Errorf("inconsistent run deadline index")
			}
			page.Runs = append(page.Runs, *run)
			page.NextCursor = bytes.Clone(key)
		}
		return nil
	})
	return page, err
}
