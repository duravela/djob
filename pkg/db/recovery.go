package db

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"go.etcd.io/bbolt"
)

// ActivateRecoveryFormat excludes binaries that do not implement restoration
// holds and recovered-cluster identities. Normal operation keeps its prior format.
func (q *DB) ActivateRecoveryFormat() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.db.Update(func(tx *bbolt.Tx) error {
		meta := tx.Bucket(metadataBucket)
		if meta.Get(formatKey)[0] >= 12 {
			return nil
		}
		return meta.Put(formatKey, []byte{12})
	})
}

// ValidateRecoveryRelease requires restored pending work to be reconciled before
// automatic execution resumes. Unknown outcomes and blocked access may remain.
func (q *DB) ValidateRecoveryRelease() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.db.View(func(tx *bbolt.Tx) error {
		for _, item := range []struct {
			bucket []byte
			kind   int
		}{{runsBucket, 0}, {jobRunsBucket, 1}, {channelsBucket, 2}, {jobResourcesBucket, 3}, {jobTriggersBucket, 4}, {schedulesBucket, 5}} {
			if err := tx.Bucket(item.bucket).ForEach(func(_, value []byte) error {
				header := struct {
					Status               string
					Completed            bool
					JobTriggerDefinition struct{ Paused bool }
					ScheduleDefinition   struct{ Paused bool }
				}{}
				state := struct{ State string }{}
				var target any = &header
				if item.kind == 0 || item.kind == 2 || item.kind == 3 {
					target = &state
				}
				if err := gob.NewDecoder(bytes.NewReader(value)).Decode(target); err != nil {
					return err
				}
				pending := false
				switch item.kind {
				case 0:
					pending = state.State == string(RunReady) || state.State == string(RunRunning) || state.State == string(RunRetryWait)
				case 1:
					pending = header.Status == string(JobReady) || header.Status == string(JobRunning) || header.Status == string(JobWaiting)
				case 2:
					pending = state.State == string(ChannelOwned) || state.State == string(ChannelDraining)
				case 3:
					pending = state.State == string(ResourceOwned)
				case 4:
					pending = !header.JobTriggerDefinition.Paused && !header.Completed
				case 5:
					pending = !header.ScheduleDefinition.Paused && !header.Completed
				}
				if pending {
					return fmt.Errorf("recovery still has executable pending work or held ownership; reconcile it before releasing the hold")
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
}
