package db

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"time"

	"go.etcd.io/bbolt"
)

var (
	schedulesBucket      = []byte("schedules")
	scheduleDueBucket    = []byte("schedule_due")
	readyRunsBucket      = []byte("ready_runs")
	readyCountsBucket    = []byte("ready_counts")
	readyByChannelBucket = []byte("ready_by_channel")
	occurrencesBucket    = []byte("schedule_occurrences")
	schedulingVersionKey = []byte("scheduling_state_version")
	schedulingBuckets    = [][]byte{schedulesBucket, scheduleDueBucket, readyRunsBucket, readyCountsBucket, readyByChannelBucket, occurrencesBucket}
)

func initializeSchedulingState(tx *bbolt.Tx) error {
	metadata := tx.Bucket(metadataBucket)
	if version := metadata.Get(schedulingVersionKey); version != nil {
		if !bytes.Equal(version, []byte{1}) {
			return fmt.Errorf("unsupported scheduling state version")
		}
		for _, name := range schedulingBuckets {
			if tx.Bucket(name) == nil {
				return fmt.Errorf("scheduling state is missing bucket %q", name)
			}
		}
		return nil
	}
	for _, name := range schedulingBuckets {
		if b := tx.Bucket(name); b != nil && b.Stats().KeyN != 0 {
			return fmt.Errorf("scheduling bucket %q has data without a format marker", name)
		}
		if _, err := tx.CreateBucketIfNotExists(name); err != nil {
			return err
		}
	}
	if err := tx.Bucket(runsBucket).ForEach(func(_, value []byte) error {
		var run Run
		if err := gob.NewDecoder(bytes.NewReader(value)).Decode(&run); err != nil {
			return fmt.Errorf("rebuild ready index: %w", err)
		}
		if run.State == RunReady {
			if err := tx.Bucket(readyRunsBucket).Put(runKey(run), []byte(run.ID)); err != nil {
				return err
			}
			if err := tx.Bucket(readyByChannelBucket).Put(channelRunKey(run), []byte(run.ID)); err != nil {
				return err
			}
			return changeReadyCount(tx, run.ChannelID, 1)
		}
		return nil
	}); err != nil {
		return err
	}
	return metadata.Put(schedulingVersionKey, []byte{1})
}

func orderedTime(at time.Time) []byte {
	key := make([]byte, 12)
	binary.BigEndian.PutUint64(key, uint64(at.Unix())^(uint64(1)<<63))
	binary.BigEndian.PutUint32(key[8:], uint32(at.Nanosecond()))
	return key
}

func timedKey(at time.Time, id string) []byte { return append(orderedTime(at), []byte(id)...) }
func runKey(run Run) []byte {
	at := run.ScheduledAt
	if at.IsZero() {
		at = run.CreatedAt
	}
	return timedKey(at, run.ID)
}

func channelPrefix(id string) []byte {
	key := make([]byte, 2, 2+len(id))
	binary.BigEndian.PutUint16(key, uint16(len(id)))
	return append(key, []byte(id)...)
}
func channelRunKey(run Run) []byte { return append(channelPrefix(run.ChannelID), runKey(run)...) }

func writeRunRecord(tx *bbolt.Tx, run Run) error {
	if tx.Bucket(runsBucket).Get([]byte(run.ID)) != nil {
		old, err := readRecord[Run](tx, runsBucket, run.ID)
		if err != nil {
			return err
		}
		if pendingRun(*old) {
			if err := changePendingCount(tx, old.ChannelID, -1); err != nil {
				return err
			}
		}
		if old.State == RunRetryWait {
			if err := tx.Bucket(retryDueBucket).Delete(timedKey(old.RetryAt, old.ID)); err != nil {
				return err
			}
		}
		if unstartedRun(*old) && !old.StartDeadline.IsZero() {
			if err := tx.Bucket(runDeadlinesBucket).Delete(timedKey(old.StartDeadline, old.ID)); err != nil {
				return err
			}
		}
		if old.State == RunReady {
			if err := tx.Bucket(readyRunsBucket).Delete(runKey(*old)); err != nil {
				return err
			}
			if err := tx.Bucket(readyByChannelBucket).Delete(channelRunKey(*old)); err != nil {
				return err
			}
			if err := changeReadyCount(tx, old.ChannelID, -1); err != nil {
				return err
			}
		}
	}
	if err := writeRecord(tx, runsBucket, run.ID, run); err != nil {
		return err
	}
	if pendingRun(run) {
		if err := changePendingCount(tx, run.ChannelID, 1); err != nil {
			return err
		}
	}
	if run.State == RunRetryWait {
		if err := tx.Bucket(retryDueBucket).Put(timedKey(run.RetryAt, run.ID), []byte(run.ID)); err != nil {
			return err
		}
	}
	if unstartedRun(run) && !run.StartDeadline.IsZero() {
		if err := tx.Bucket(runDeadlinesBucket).Put(timedKey(run.StartDeadline, run.ID), []byte(run.ID)); err != nil {
			return err
		}
	}
	if run.State == RunReady {
		if err := tx.Bucket(readyRunsBucket).Put(runKey(run), []byte(run.ID)); err != nil {
			return err
		}
		if err := tx.Bucket(readyByChannelBucket).Put(channelRunKey(run), []byte(run.ID)); err != nil {
			return err
		}
		return changeReadyCount(tx, run.ChannelID, 1)
	}
	return nil
}

func (q *DB) FirstReadyRun(channel string) (*Run, error) {
	var run *Run
	err := q.db.View(func(tx *bbolt.Tx) error {
		prefix := channelPrefix(channel)
		key, value := tx.Bucket(readyByChannelBucket).Cursor().Seek(prefix)
		if !bytes.HasPrefix(key, prefix) {
			return reject("not_found", "channel has no ready run")
		}
		var err error
		run, err = readRecord[Run](tx, runsBucket, string(value))
		if err != nil {
			return fmt.Errorf("channel ready index: %v", err)
		}
		if run.State != RunReady || run.ChannelID != channel || !bytes.Equal(key, channelRunKey(*run)) {
			return fmt.Errorf("inconsistent channel ready index")
		}
		return nil
	})
	return run, err
}

func readyCount(tx *bbolt.Tx, channel string) (uint64, error) {
	data := tx.Bucket(readyCountsBucket).Get([]byte(channel))
	if data == nil {
		return 0, nil
	}
	if len(data) != 8 {
		return 0, fmt.Errorf("invalid ready-run count")
	}
	return binary.BigEndian.Uint64(data), nil
}

func changeReadyCount(tx *bbolt.Tx, channel string, delta int) error {
	count, err := readyCount(tx, channel)
	if err != nil {
		return err
	}
	if delta < 0 {
		if count == 0 {
			return fmt.Errorf("ready-run count underflow")
		}
		count--
	} else {
		if count == ^uint64(0) {
			return fmt.Errorf("ready-run count overflow")
		}
		count++
	}
	var data [8]byte
	binary.BigEndian.PutUint64(data[:], count)
	return tx.Bucket(readyCountsBucket).Put([]byte(channel), data[:])
}

func checkRunCapacity(tx *bbolt.Tx, run Run) error {
	if run.State != RunReady {
		return nil
	}
	if tx.Bucket(runsBucket).Get([]byte(run.ID)) != nil {
		old, err := readRecord[Run](tx, runsBucket, run.ID)
		if err != nil {
			return err
		}
		if pendingRun(*old) {
			return nil
		}
	}
	count, err := pendingCount(tx, run.ChannelID)
	if err != nil {
		return err
	}
	if count >= MaxReadyRunsPerChannel {
		return reject("backpressure", "gateway ready-run capacity reached")
	}
	return nil
}

func (q *DB) ReadyRunCount(channel string) (uint64, error) {
	var count uint64
	err := q.db.View(func(tx *bbolt.Tx) error { var err error; count, err = readyCount(tx, channel); return err })
	return count, err
}

func writeScheduleRecord(tx *bbolt.Tx, schedule Schedule) error {
	if tx.Bucket(schedulesBucket).Get([]byte(schedule.ID)) != nil {
		old, err := readRecord[Schedule](tx, schedulesBucket, schedule.ID)
		if err != nil {
			return err
		}
		if !old.NextAt.IsZero() {
			if err := tx.Bucket(scheduleDueBucket).Delete(timedKey(old.NextAt, old.ID)); err != nil {
				return err
			}
		}
	}
	if err := writeRecord(tx, schedulesBucket, schedule.ID, schedule); err != nil {
		return err
	}
	if !schedule.Paused && !schedule.Completed && !schedule.NextAt.IsZero() {
		return tx.Bucket(scheduleDueBucket).Put(timedKey(schedule.NextAt, schedule.ID), []byte(schedule.ID))
	}
	return nil
}

func queryLimit(limit int) error {
	if limit < 1 || limit > MaxQueryPage {
		return fmt.Errorf("query limit must be between 1 and %d", MaxQueryPage)
	}
	return nil
}

func pageStart(cursor *bbolt.Cursor, after []byte) ([]byte, []byte) {
	if len(after) == 0 {
		return cursor.First()
	}
	key, value := cursor.Seek(after)
	if bytes.Equal(key, after) {
		return cursor.Next()
	}
	return key, value
}

func (q *DB) ListReadyRuns(after []byte, limit int) (RunPage, error) {
	page := RunPage{}
	if err := queryLimit(limit); err != nil {
		return page, err
	}
	err := q.db.View(func(tx *bbolt.Tx) error {
		cursor := tx.Bucket(readyRunsBucket).Cursor()
		for key, value := pageStart(cursor, after); key != nil && len(page.Runs) < limit; key, value = cursor.Next() {
			run, err := readRecord[Run](tx, runsBucket, string(value))
			if err != nil {
				return fmt.Errorf("ready index: %v", err)
			}
			if run.State != RunReady || !bytes.Equal(key, runKey(*run)) {
				return fmt.Errorf("inconsistent ready index")
			}
			page.Runs = append(page.Runs, *run)
			page.NextCursor = bytes.Clone(key)
		}
		return nil
	})
	return page, err
}

func (q *DB) ListDueSchedules(at time.Time, after []byte, limit int) (SchedulePage, error) {
	page := SchedulePage{}
	if err := queryLimit(limit); err != nil {
		return page, err
	}
	err := q.db.View(func(tx *bbolt.Tx) error {
		cursor := tx.Bucket(scheduleDueBucket).Cursor()
		for key, value := pageStart(cursor, after); key != nil && len(page.Schedules) < limit; key, value = cursor.Next() {
			if len(key) < 12 {
				return fmt.Errorf("invalid schedule due index key")
			}
			if bytes.Compare(key[:12], orderedTime(at)) > 0 {
				break
			}
			schedule, err := readRecord[Schedule](tx, schedulesBucket, string(value))
			if err != nil {
				return fmt.Errorf("schedule due index: %v", err)
			}
			if schedule.Paused || schedule.Completed || !bytes.Equal(key, timedKey(schedule.NextAt, schedule.ID)) {
				return fmt.Errorf("inconsistent schedule due index")
			}
			page.Schedules = append(page.Schedules, *schedule)
			page.NextCursor = bytes.Clone(key)
		}
		return nil
	})
	return page, err
}

func (q *DB) GetSchedule(id string) (*Schedule, error) {
	return getStateRecord[Schedule](q, schedulesBucket, id)
}
