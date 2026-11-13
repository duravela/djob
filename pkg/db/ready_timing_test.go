package db

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReadyTimingTracksYieldAndSignalWithoutChangingExecutionTimestamp(t *testing.T) {
	h := newChannelHarness(t)
	h.job("alpha", "", "work", "")
	h.launch("alpha", "work", "r")
	run, err := h.db.GetJobRun("alpha", "r")
	require.NoError(t, err)
	require.Equal(t, h.at, run.ReadySince)
	created := run.CreatedAt
	owner := h.claimJob("alpha", "r", "first")
	h.at = h.at.Add(time.Second)
	h.ok(jc(JobCommand{Yield: &YieldJob{JobOwner: owner}}))
	run, err = h.db.GetJobRun("alpha", "r")
	require.NoError(t, err)
	require.Equal(t, h.at, run.ReadySince)
	owner = h.claimJob("alpha", "r", "second")
	h.ok(jc(JobCommand{Advance: &AdvanceJob{JobOwner: owner, Kind: JobWait, WaitKey: "continue"}}))
	waited, err := h.db.GetJobRun("alpha", "r")
	require.NoError(t, err)
	h.at = h.at.Add(10 * time.Second)
	h.ok(jc(JobCommand{Signal: &SignalJob{TenantID: "alpha", RunID: "r", EventID: "event", Key: "continue"}}))
	run, err = h.db.GetJobRun("alpha", "r")
	require.NoError(t, err)
	require.Equal(t, JobReady, run.Status)
	require.Equal(t, h.at, run.ReadySince)
	require.Equal(t, waited.LastTransition, run.LastTransition, "diagnostic timestamp must not change execution checkpoint ordering")
	require.Equal(t, created, run.CreatedAt)
	stats, err := h.db.QueueStats(context.Background(), 100)
	require.NoError(t, err)
	require.Equal(t, run.ReadySince, stats.Generic.OldestReadySince)
	require.Zero(t, stats.Generic.MissingReadySince)
}
