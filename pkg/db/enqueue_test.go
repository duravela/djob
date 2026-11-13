package db

import (
	"testing"
	"time"

	"github.com/duravela/djob/pkg/commit"
	"github.com/stretchr/testify/require"
)

func TestVersionFourEnqueueDoesNotAlterChannelClockOrVersionTwoReplay(t *testing.T) {
	h := newChannelHarness(t)
	owner := h.setup("gateway")
	renewedAt := h.at.Add(10 * time.Second)
	h.ok(StateCommand{At: renewedAt, Renew: &RenewChannel{ChannelID: "gateway", OwnerID: owner.OwnerID, ExpectedEpoch: owner.Epoch, LeaseUntil: h.at.Add(time.Hour)}})
	before, err := h.db.GetChannel("gateway")
	require.NoError(t, err)
	issuedAt := h.at.Add(time.Second)
	h.reject(StateCommand{At: issuedAt, CreateRun: &CreateRun{ID: "legacy", ChannelID: "gateway", Executor: "fake"}}, "stale_time")
	command := StateCommand{RequestID: "queued", At: issuedAt, EnqueueRun: &CreateRun{ID: "queued", ChannelID: "gateway", Executor: "fake", Payload: []byte{0, 0xff}}}
	created := h.ok(command)
	require.Equal(t, RunReady, created.Run.State)
	require.Equal(t, issuedAt, created.Run.CreatedAt)
	channel, err := h.db.GetChannel("gateway")
	require.NoError(t, err)
	require.Equal(t, before, channel)
	require.Equal(t, created, h.ok(command), "an exact retry returns its receipt")
	claim := claim(owner, "queued", "attempt")
	claim.At = renewedAt.Add(time.Second)
	h.ok(claim)
	command.RequestID = "wrong-version"
	result := h.db.ApplyEntries([]commit.Entry{logEntry(t, h.index+1, Command{Version: ChannelCommandVersion, Type: CmdState, State: &command})})
	require.Error(t, result.Error, "old envelopes must not interpret new semantics")
	require.Equal(t, h.index, h.db.LastAppliedIndex())
}
