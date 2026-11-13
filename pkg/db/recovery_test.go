package db

import (
	"bytes"
	"encoding/gob"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func TestRecoveryReleaseChecksSerializedWorkAndOwnership(t *testing.T) {
	for _, item := range []struct {
		name    string
		bucket  []byte
		pending any
		safe    any
	}{
		{"gateway run", runsBucket, Run{State: RunRetryWait}, Run{State: RunUnknown}},
		{"generic checkpoint", jobRunsBucket, JobRun{Status: JobWaiting, State: []byte("checkpoint")}, JobRun{Status: JobUnknown, State: []byte("checkpoint")}},
		{"gateway ownership", channelsBucket, CommunicationChannel{State: ChannelDraining}, CommunicationChannel{State: ChannelBlocked}},
		{"resource ownership", jobResourcesBucket, JobResource{State: ResourceOwned}, JobResource{State: ResourceBlocked}},
		{"generic trigger", jobTriggersBucket, JobTrigger{JobTriggerDefinition: JobTriggerDefinition{Paused: false}}, JobTrigger{JobTriggerDefinition: JobTriggerDefinition{Paused: true}}},
		{"gateway schedule", schedulesBucket, Schedule{ScheduleDefinition: ScheduleDefinition{Paused: false}}, Schedule{ScheduleDefinition: ScheduleDefinition{Paused: true}}},
	} {
		t.Run(item.name, func(t *testing.T) {
			q, cleanup := NewTestDB(t)
			defer cleanup()
			put := func(record any) {
				var encoded bytes.Buffer
				require.NoError(t, gob.NewEncoder(&encoded).Encode(record))
				require.NoError(t, q.db.Update(func(tx *bbolt.Tx) error { return tx.Bucket(item.bucket).Put([]byte("record"), encoded.Bytes()) }))
			}
			put(item.pending)
			require.ErrorContains(t, q.ValidateRecoveryRelease(), "pending work or held ownership")
			put(item.safe)
			require.NoError(t, q.ValidateRecoveryRelease())
		})
	}
}
