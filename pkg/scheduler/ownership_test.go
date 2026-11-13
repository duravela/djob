package scheduler

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor/fake"
	"github.com/stretchr/testify/require"
)

type ownershipScan struct {
	after []byte
	limit int
	rows  int
}

type ownershipTestBackend struct {
	*runtimeBackend
	scans      []ownershipScan
	commands   []db.StateCommand
	loseAck    bool
	failBefore int
	listError  error
	before     func(db.StateCommand)
	corrupt    func(*db.StateResult)
}

func (b *ownershipTestBackend) ListChannels(after []byte, limit int) (db.ChannelPage, error) {
	if b.listError != nil {
		return db.ChannelPage{}, b.listError
	}
	page, err := b.runtimeBackend.ListChannels(after, limit)
	b.scans = append(b.scans, ownershipScan{after: bytes.Clone(after), limit: limit, rows: len(page.Channels)})
	return page, err
}

func (b *ownershipTestBackend) ProposeStateCommand(ctx context.Context, c db.StateCommand) (db.StateResult, error) {
	b.commands = append(b.commands, c)
	if b.before != nil {
		f := b.before
		b.before = nil
		f(c)
	}
	if b.failBefore > 0 {
		b.failBefore--
		return db.StateResult{}, context.DeadlineExceeded
	}
	result, err := b.runtimeBackend.ProposeStateCommand(ctx, c)
	if b.loseAck {
		b.loseAck = false
		return db.StateResult{}, context.DeadlineExceeded
	}
	if b.corrupt != nil {
		b.corrupt(&result)
	}
	return result, err
}

func sweepOwner(f *runtimeFixture, id string, until time.Time) db.ChannelOwner {
	f.t.Helper()
	result := f.state(db.StateCommand{Acquire: &db.AcquireChannel{ChannelID: id, OwnerID: "old/process", LeaseUntil: until}})
	return db.ChannelOwner{ChannelID: id, Epoch: result.Channel.Epoch, OwnerID: result.Channel.OwnerID}
}

func ownershipState(t *testing.T, b *runtimeBackend, id string, at time.Time, c db.StateCommand) db.StateResult {
	t.Helper()
	c.RequestID, c.At = id, at.UTC()
	r, err := b.ProposeStateCommand(context.Background(), c)
	require.NoError(t, err)
	return r
}

func TestOwnershipSweepBoundsPagesAndBlocksIdleDrainingAndActiveOwners(t *testing.T) {
	f := newRuntime(t, fake.Options{}, "a-available", "b-idle", "c-draining", "d-active", "e-live", "f-blocked", "g-idle")
	due := time.Now().UTC().Add(time.Minute)
	for _, id := range []string{"b-idle", "f-blocked", "g-idle"} {
		sweepOwner(f, id, due)
	}
	draining := sweepOwner(f, "c-draining", due)
	f.state(db.StateCommand{Drain: &draining})
	active := sweepOwner(f, "d-active", due)
	f.ready("run", "d-active")
	f.ready("queued-unit-7", "d-active")
	f.state(db.StateCommand{Claim: &db.ClaimRun{ChannelOwner: active, RunID: "run", AttemptID: "first", ClaimToken: "token"}})
	f.state(db.StateCommand{StartAttempt: &db.StartAttempt{ChannelOwner: active, RunID: "run", AttemptID: "first", ClaimToken: "token", DeadlineAware: true, RetryAware: true}})
	sweepOwner(f, "e-live", due.Add(time.Hour))
	ownershipState(t, f.backend, "already-blocked", due, db.StateCommand{Expire: &db.ExpireChannel{ChannelID: "f-blocked", Epoch: 1}})
	untouched, err := f.backend.GetChannel("e-live")
	require.NoError(t, err)
	b := &ownershipTestBackend{runtimeBackend: f.backend, loseAck: true}
	leader := false
	s, err := NewOwnershipSweeper(OwnershipSweeperConfig{BatchSize: 2}, b, func() bool { return leader })
	require.NoError(t, err)
	n, err := s.Tick(context.Background(), due)
	require.NoError(t, err)
	require.Zero(t, n)
	require.Empty(t, b.scans, "followers must not discover or propose expiry")
	leader = true
	for _, expected := range []int{1, 2, 0, 1, 0} {
		n, err := s.Tick(context.Background(), due)
		require.NoError(t, err)
		require.Equal(t, expected, n)
	}
	require.Len(t, b.scans, 5)
	for _, scan := range b.scans {
		require.Equal(t, 2, scan.limit)
		require.LessOrEqual(t, scan.rows, 2)
	}
	require.Len(t, b.commands, 5)
	require.Equal(t, b.commands[0], b.commands[1], "a lost acknowledgement must retry the exact envelope")
	for _, c := range b.commands {
		require.NotNil(t, c.Expire)
		require.Equal(t, db.ChannelCommandVersion, c.CommandVersion())
	}
	for _, id := range []string{"b-idle", "c-draining", "d-active", "f-blocked", "g-idle"} {
		channel, err := f.backend.GetChannel(id)
		require.NoError(t, err)
		require.Equal(t, db.ChannelBlocked, channel.State)
		require.EqualValues(t, 1, channel.Epoch)
		require.Equal(t, "old/process", channel.OwnerID, "expiry must not manufacture a replacement owner")
	}
	live, err := f.backend.GetChannel("e-live")
	require.NoError(t, err)
	require.Equal(t, untouched, live)
	channel, err := f.backend.GetChannel("d-active")
	require.NoError(t, err)
	require.Equal(t, "first", channel.ActiveAttemptID, "uncertain physical occupancy must remain visible")
	run, err := f.backend.GetRun("run")
	require.NoError(t, err)
	require.Equal(t, db.RunUnknown, run.State)
	require.EqualValues(t, 1, run.AttemptCount)
	attempt, err := f.backend.GetAttempt("first")
	require.NoError(t, err)
	require.Equal(t, db.AttemptUnknown, attempt.Outcome)
	require.Equal(t, due, attempt.FinishedAt)
	require.False(t, attempt.Quiescent)
	queued, err := f.backend.GetRun("queued-unit-7")
	require.NoError(t, err)
	require.Equal(t, db.RunReady, queued.State)
	require.Zero(t, queued.AttemptCount)
	for _, gateway := range f.gateways {
		require.Zero(t, gateway.Stats().Started)
	}
}

func TestOwnershipSweepDoesNotOverwriteConcurrentRenewalHandoverOrExpiry(t *testing.T) {
	for _, race := range []string{"renew", "release", "new owner", "other expiry", "newer timestamp"} {
		t.Run(race, func(t *testing.T) {
			f := newRuntime(t, fake.Options{}, "gateway")
			due := time.Now().UTC().Add(time.Minute)
			owner := sweepOwner(f, "gateway", due)
			b := &ownershipTestBackend{runtimeBackend: f.backend}
			var expected *db.CommunicationChannel
			b.before = func(db.StateCommand) {
				var result db.StateResult
				switch race {
				case "renew":
					result = ownershipState(t, f.backend, "renew", due.Add(-time.Millisecond), db.StateCommand{Renew: &db.RenewChannel{
						ChannelID: owner.ChannelID, ExpectedEpoch: owner.Epoch, OwnerID: owner.OwnerID, LeaseUntil: due.Add(time.Hour)}})
				case "release", "new owner":
					ownershipState(t, f.backend, "drain", due.Add(-3*time.Millisecond), db.StateCommand{Drain: &owner})
					result = ownershipState(t, f.backend, "release", due.Add(-2*time.Millisecond), db.StateCommand{Release: &db.ReleaseChannel{ChannelOwner: owner, Quiescent: true}})
					if race == "new owner" {
						result = ownershipState(t, f.backend, "acquire", due.Add(-time.Millisecond), db.StateCommand{Acquire: &db.AcquireChannel{
							ChannelID: "gateway", ExpectedEpoch: owner.Epoch, OwnerID: "replacement/process", LeaseUntil: due.Add(time.Hour)}})
					}
				case "other expiry", "newer timestamp":
					at := due
					if race == "newer timestamp" {
						at = at.Add(time.Millisecond)
					}
					result = ownershipState(t, f.backend, "other-expiry", at, db.StateCommand{Expire: &db.ExpireChannel{ChannelID: "gateway", Epoch: owner.Epoch}})
				}
				expected = result.Channel
			}
			s, err := NewOwnershipSweeper(OwnershipSweeperConfig{}, b, func() bool { return true })
			require.NoError(t, err)
			n, err := s.Tick(context.Background(), due)
			require.NoError(t, err, "a stale observation is an expected race")
			require.Zero(t, n)
			require.Len(t, b.commands, 1, "business rejections must not be retried")
			channel, err := f.backend.GetChannel("gateway")
			require.NoError(t, err)
			require.Equal(t, expected, channel)
		})
	}
}

func TestOwnershipSweepRevisitsUnacknowledgedWorkAndResetsOnLeaderLoss(t *testing.T) {
	f := newRuntime(t, fake.Options{}, "a", "b")
	due := time.Now().UTC().Add(time.Minute)
	sweepOwner(f, "a", due)
	sweepOwner(f, "b", due)
	b := &ownershipTestBackend{runtimeBackend: f.backend, failBefore: 2}
	leader := true
	s, err := NewOwnershipSweeper(OwnershipSweeperConfig{BatchSize: 1}, b, func() bool { return leader })
	require.NoError(t, err)
	n, err := s.Tick(context.Background(), due)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Zero(t, n)
	require.Equal(t, b.commands[0], b.commands[1])
	n, err = s.Tick(context.Background(), due)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	n, err = s.Tick(context.Background(), due)
	require.NoError(t, err)
	require.Equal(t, 1, n, "wrapping discovery must revisit uncommitted work")
	require.NotEmpty(t, s.cursor)
	leader = false
	n, err = s.Tick(context.Background(), due)
	require.NoError(t, err)
	require.Zero(t, n)
	require.Empty(t, s.cursor)
	leader = true
	_, err = s.Tick(context.Background(), due)
	require.NoError(t, err)
	require.Empty(t, b.scans[len(b.scans)-1].after, "a new leader starts discovery from the beginning")
}

func TestOwnershipSweepStopsProposingWhenLeadershipChangesMidPage(t *testing.T) {
	f := newRuntime(t, fake.Options{}, "a", "b")
	due := time.Now().UTC().Add(time.Minute)
	sweepOwner(f, "a", due)
	sweepOwner(f, "b", due)
	var leader atomic.Bool
	leader.Store(true)
	b := &ownershipTestBackend{runtimeBackend: f.backend, before: func(db.StateCommand) { leader.Store(false) }}
	s, err := NewOwnershipSweeper(OwnershipSweeperConfig{}, b, leader.Load)
	require.NoError(t, err)
	n, err := s.Tick(context.Background(), due)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Len(t, b.commands, 1)
	require.Empty(t, s.cursor)
	channel, err := f.backend.GetChannel("b")
	require.NoError(t, err)
	require.Equal(t, db.ChannelOwned, channel.State)
}

func TestOwnershipSweepRejectsMismatchedReceipts(t *testing.T) {
	for name, corrupt := range map[string]func(*db.StateResult){
		"request ID":      func(r *db.StateResult) { r.RequestID = "other" },
		"fingerprint":     func(r *db.StateResult) { r.Fingerprint[0] ^= 1 },
		"missing channel": func(r *db.StateResult) { r.Channel = nil },
		"channel ID":      func(r *db.StateResult) { r.Channel.ID = "other" },
		"epoch":           func(r *db.StateResult) { r.Channel.Epoch++ },
		"state":           func(r *db.StateResult) { r.Channel.State = db.ChannelAvailable },
	} {
		t.Run(name, func(t *testing.T) {
			f := newRuntime(t, fake.Options{}, "gateway")
			due := time.Now().UTC().Add(time.Minute)
			sweepOwner(f, "gateway", due)
			b := &ownershipTestBackend{runtimeBackend: f.backend, corrupt: corrupt}
			s, err := NewOwnershipSweeper(OwnershipSweeperConfig{}, b, func() bool { return true })
			require.NoError(t, err)
			n, err := s.Tick(context.Background(), due)
			require.ErrorContains(t, err, "receipt")
			require.Zero(t, n)
			channel, err := f.backend.GetChannel("gateway")
			require.NoError(t, err)
			require.Equal(t, db.ChannelBlocked, channel.State, "a bad acknowledgement must not undo committed expiry")
		})
	}
}

func TestOwnershipSweepValidatesInputsAndBoundsLifecycle(t *testing.T) {
	f := newRuntime(t, fake.Options{}, "gateway")
	for _, cfg := range []OwnershipSweeperConfig{{PollInterval: -1}, {BatchSize: -1}, {BatchSize: db.MaxQueryPage + 1}, {CommitTimeout: -1}} {
		_, err := NewOwnershipSweeper(cfg, f.backend, func() bool { return true })
		require.Error(t, err)
	}
	_, err := NewOwnershipSweeper(OwnershipSweeperConfig{}, nil, func() bool { return true })
	require.Error(t, err)
	_, err = NewOwnershipSweeper(OwnershipSweeperConfig{}, f.backend, nil)
	require.Error(t, err)
	b := &ownershipTestBackend{runtimeBackend: f.backend}
	s, err := NewOwnershipSweeper(OwnershipSweeperConfig{}, b, func() bool { return true })
	require.NoError(t, err)
	_, err = s.Tick(context.Background(), time.Time{})
	require.ErrorContains(t, err, "timestamp")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = s.Tick(ctx, time.Now())
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, b.scans)
	due := time.Now().UTC().Add(time.Minute)
	sweepOwner(f, "gateway", due)
	s.sequence = ^uint64(0)
	_, err = s.Tick(context.Background(), due)
	require.ErrorContains(t, err, "sequence")
	require.Empty(t, b.commands)
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	sentinel := fmt.Errorf("inspection failed")
	b.listError = sentinel
	callbacks := 0
	s, err = NewOwnershipSweeper(OwnershipSweeperConfig{PollInterval: time.Millisecond, OnError: func(err error) {
		require.True(t, errors.Is(err, sentinel))
		callbacks++
		cancel()
	}}, b, func() bool { return true })
	require.NoError(t, err)
	require.ErrorIs(t, s.Run(ctx), context.Canceled)
	require.Equal(t, 1, callbacks)
}
