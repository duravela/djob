package scheduler

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/commit"
	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/pkg/executor"
	"github.com/duravela/djob/pkg/executor/fake"
	"github.com/stretchr/testify/require"
)

// runtimeBackend uses the actual transactional state machine. Only consensus
// delivery is replaced; a lost acknowledgement still leaves its durable receipt.
type runtimeBackend struct {
	*db.DB
	mu         sync.Mutex
	index      uint64
	commands   []db.StateCommand
	loseAck    atomic.Bool
	dueBarrier chan struct{}
	dueReaders atomic.Int32
}

func (b *runtimeBackend) ProposeStateCommand(ctx context.Context, command db.StateCommand) (db.StateResult, error) {
	if err := ctx.Err(); err != nil {
		return db.StateResult{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	var data bytes.Buffer
	if err := gob.NewEncoder(&data).Encode(db.Command{Version: command.CommandVersion(), Type: db.CmdState, State: &command}); err != nil {
		return db.StateResult{}, err
	}
	b.index++
	result := b.ApplyEntries([]commit.Entry{{Index: b.index, Data: data.Bytes()}})
	if result.Error != nil {
		return db.StateResult{}, result.Error
	}
	if len(result.StateResults) != 1 {
		return db.StateResult{}, fmt.Errorf("missing state receipt")
	}
	b.commands = append(b.commands, command)
	if (command.Materialize != nil || (command.Job != nil && (command.Job.Materialize != nil || command.Job.Purge != nil || command.Job.CompactReceipts != nil || command.Job.CompactConfigurationReceipts != nil))) && b.loseAck.CompareAndSwap(true, false) {
		return db.StateResult{}, context.DeadlineExceeded
	}
	receipt := result.StateResults[0]
	return receipt, receipt.Err()
}

func (b *runtimeBackend) ListDueSchedules(at time.Time, after []byte, limit int) (db.SchedulePage, error) {
	page, err := b.DB.ListDueSchedules(at, after, limit)
	if b.dueBarrier != nil {
		if b.dueReaders.Add(1) == 2 {
			close(b.dueBarrier)
		}
		<-b.dueBarrier
	}
	return page, err
}

type runtimeFixture struct {
	t           *testing.T
	backend     *runtimeBackend
	gateways    map[string]*fake.Gateway
	dispatchers []*Dispatcher
	sequence    int
}

func newRuntime(t *testing.T, options fake.Options, channels ...string) *runtimeFixture {
	t.Helper()
	q, err := db.NewDB(filepath.Join(t.TempDir(), "state.db"))
	require.NoError(t, err)
	f := &runtimeFixture{t: t, backend: &runtimeBackend{DB: q}, gateways: make(map[string]*fake.Gateway)}
	t.Cleanup(func() { require.NoError(t, q.Close()) })
	for _, id := range channels {
		f.gateways[id] = fake.New(options)
		f.state(db.StateCommand{Configure: &db.ConfigureChannel{ID: id, GatewayID: id, Protocol: "fake", Endpoint: id + ":502"}})
	}
	t.Cleanup(func() {
		stop := make(chan struct{})
		go func() {
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()
			for {
				for _, gateway := range f.gateways {
					gateway.ReleaseAll()
				}
				select {
				case <-stop:
					return
				case <-ticker.C:
				}
			}
		}()
		defer close(stop)
		for _, dispatcher := range f.dispatchers {
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			err := dispatcher.Close(ctx)
			cancel()
			require.NotErrorIs(t, err, context.DeadlineExceeded, "cleanup must retain and then release the actual worker")
		}
	})
	return f
}

func (f *runtimeFixture) state(command db.StateCommand) db.StateResult {
	f.t.Helper()
	f.sequence++
	command.RequestID, command.At = fmt.Sprintf("runtime/%d", f.sequence), time.Now().UTC()
	result, err := f.backend.ProposeStateCommand(context.Background(), command)
	require.NoError(f.t, err)
	return result
}

func (f *runtimeFixture) ready(id, channel string) {
	f.t.Helper()
	f.state(db.StateCommand{CreateRun: &db.CreateRun{ID: id, ChannelID: channel, Executor: "fake", Payload: []byte(id)}})
}

func runtimeExecutorConfig() executor.Config {
	return executor.Config{LeaseDuration: 5 * time.Second, LeaseSafetyMargin: time.Second, RenewInterval: time.Second,
		CommitTimeout: 200 * time.Millisecond, OperationTimeout: 3 * time.Second, RecoveryTimeout: 2 * time.Second, CloseTimeout: 2 * time.Second}
}

func (f *runtimeFixture) dispatcher(maxChannels, scanLimit int) *Dispatcher {
	f.t.Helper()
	d, err := NewDispatcher(DispatcherConfig{MaxChannels: maxChannels, ScanLimit: scanLimit, PollInterval: 5 * time.Millisecond,
		Executor: runtimeExecutorConfig()}, f.backend, func(channel db.CommunicationChannel) (executor.Transport, error) {
		return f.gateways[channel.ID].Factory(channel)
	})
	require.NoError(f.t, err)
	f.dispatchers = append(f.dispatchers, d)
	return d
}

func phase(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatal("gateway phase did not start")
	}
}

func gatewayCall(t *testing.T, gateway *fake.Gateway) *fake.Call {
	t.Helper()
	select {
	case call := <-gateway.Calls:
		return call
	case <-time.After(3 * time.Second):
		t.Fatal("gateway exchange did not start")
		return nil
	}
}

func finish(t *testing.T, call *fake.Call) {
	t.Helper()
	call.Reply <- fake.Response{Outcome: executor.Outcome{Status: db.AttemptSucceeded, Data: []byte("result")}}
	phase(t, call.RecoveryStarted)
	call.Recover <- nil
	phase(t, call.CloseStarted)
	call.Close <- nil
}

func tick(t *testing.T, d *Dispatcher) int {
	t.Helper()
	n, err := d.Tick(context.Background())
	require.NoError(t, err)
	return n
}

func settle(t *testing.T, d *Dispatcher) {
	t.Helper()
	require.Eventually(t, func() bool { return d.Pending() == 0 }, 3*time.Second, time.Millisecond)
}

func TestMaterializerLeaderPollingAndLostAcknowledgementDoNotDuplicateRuns(t *testing.T) {
	f := newRuntime(t, fake.Options{}, "gateway")
	var leader atomic.Bool
	var observations []MaterializationObservation
	cfg := MaterializerConfig{OnMaterialized: func(o MaterializationObservation) { observations = append(observations, o) }}
	m, err := NewMaterializer(cfg, f.backend, leader.Load)
	require.NoError(t, err)
	schedule, err := m.Put(context.Background(), db.ScheduleDefinition{ID: "once", ChannelID: "gateway", Executor: "fake", Payload: []byte{0, 0xff},
		Spec: db.ScheduleSpec{Kind: db.ScheduleOnce, StartAt: time.Now().Add(-time.Minute)}, MissedPolicy: db.MissedCoalesce}, 0)
	require.NoError(t, err)
	stats, err := m.Tick(context.Background(), time.Now())
	require.NoError(t, err)
	require.Zero(t, stats.Advanced)
	require.Empty(t, observations)
	count, err := f.backend.ReadyRunCount("gateway")
	require.NoError(t, err)
	require.Zero(t, count)
	leader.Store(true)
	f.backend.loseAck.Store(true)
	stats, err = m.Tick(context.Background(), time.Now())
	require.NoError(t, err)
	require.Equal(t, 1, stats.Created)
	require.Len(t, observations, 1, "an identical proposal retry must not repeat the occurrence observation")
	require.True(t, observations[0].TimeKnown)
	require.GreaterOrEqual(t, observations[0].Lateness, time.Minute)
	f.backend.mu.Lock()
	last := f.backend.commands[len(f.backend.commands)-2:]
	require.Equal(t, last[0], last[1], "uncertain acknowledgement retries the identical envelope")
	f.backend.mu.Unlock()
	restarted, err := NewMaterializer(cfg, f.backend, leader.Load)
	require.NoError(t, err)
	stats, err = restarted.Tick(context.Background(), time.Now())
	require.NoError(t, err)
	require.Zero(t, stats.Created)
	require.Len(t, observations, 1, "recovery does not reconstruct prior local observations")
	run, err := f.backend.GetRun(db.OccurrenceRunID(schedule.ID, schedule.Revision, schedule.NextAt))
	require.NoError(t, err)
	require.Equal(t, []byte{0, 0xff}, run.Payload)
	count, err = f.backend.ReadyRunCount("gateway")
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
}

func TestConcurrentMaterializersCompareDurableCursor(t *testing.T) {
	f := newRuntime(t, fake.Options{}, "gateway")
	var observations atomic.Uint32
	cfg := MaterializerConfig{OnMaterialized: func(MaterializationObservation) { observations.Add(1) }}
	one, err := NewMaterializer(cfg, f.backend, func() bool { return true })
	require.NoError(t, err)
	_, err = one.Put(context.Background(), db.ScheduleDefinition{ID: "interval", ChannelID: "gateway", Executor: "fake",
		Spec: db.ScheduleSpec{Kind: db.ScheduleInterval, StartAt: time.Now().Add(-time.Hour), Interval: time.Minute}, MissedPolicy: db.MissedReplay, CatchUpLimit: 3}, 0)
	require.NoError(t, err)
	two, err := NewMaterializer(cfg, f.backend, func() bool { return true })
	require.NoError(t, err)
	f.backend.dueBarrier = make(chan struct{})
	cutoff := time.Now().UTC()
	type answer struct {
		stats MaterializationStats
		err   error
	}
	answers := make(chan answer, 2)
	for _, m := range []*Materializer{one, two} {
		go func() { stats, err := m.Tick(context.Background(), cutoff); answers <- answer{stats, err} }()
	}
	var total MaterializationStats
	for i := 0; i < 2; i++ {
		select {
		case result := <-answers:
			require.NoError(t, result.err)
			total.Created += result.stats.Created
			total.Conflicts += result.stats.Conflicts
		case <-time.After(3 * time.Second):
			t.Fatal("concurrent materializers did not finish")
		}
	}
	require.Equal(t, 3, total.Created)
	require.Equal(t, 1, total.Conflicts)
	require.EqualValues(t, 3, observations.Load(), "a losing materializer must not observe successful creation")
	count, err := f.backend.ReadyRunCount("gateway")
	require.NoError(t, err)
	require.EqualValues(t, 3, count)
}

func TestDispatcherUsesOldestReadyRunAndRetainsOccupancyThroughCleanup(t *testing.T) {
	f := newRuntime(t, fake.Options{}, "gateway")
	for _, id := range []string{"first", "second", "third"} {
		f.ready(id, "gateway")
	}
	d := f.dispatcher(1, 1)
	page, err := f.backend.ListReadyRuns(nil, 1)
	require.NoError(t, err)
	d.cursor = page.NextCursor // Discovery starts at the second run, selection must not.
	require.Equal(t, 1, tick(t, d))
	call := gatewayCall(t, f.gateways["gateway"])
	require.Equal(t, "first", call.Request.RunID)
	call.Reply <- fake.Response{Outcome: executor.Outcome{Status: db.AttemptSucceeded}}
	phase(t, call.RecoveryStarted)
	for i := 0; i < 5; i++ {
		require.Zero(t, tick(t, d))
	}
	for _, id := range []string{"second", "third"} {
		run, err := f.backend.GetRun(id)
		require.NoError(t, err)
		require.Equal(t, db.RunReady, run.State)
		require.Zero(t, run.AttemptCount)
	}
	call.Recover <- nil
	phase(t, call.CloseStarted)
	require.Zero(t, tick(t, d))
	call.Close <- nil
	settle(t, d)
	for _, id := range []string{"second", "third"} {
		require.Eventually(t, func() bool { return tick(t, d) == 1 }, time.Second, time.Millisecond)
		call = gatewayCall(t, f.gateways["gateway"])
		require.Equal(t, id, call.Request.RunID)
		finish(t, call)
		settle(t, d)
	}
	require.Equal(t, 1, f.gateways["gateway"].Stats().MaxActive)
	require.Equal(t, 3, f.gateways["gateway"].Stats().Started)
}

func TestDispatcherIndependentGatewaysAndBoundedPoolEviction(t *testing.T) {
	for _, limit := range []int{1, 2} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			f := newRuntime(t, fake.Options{}, "a", "b")
			f.ready("a-first", "a")
			f.ready("b-first", "b")
			d := f.dispatcher(limit, 1)
			require.Equal(t, 1, tick(t, d))
			first := gatewayCall(t, f.gateways["a"])
			if limit == 1 {
				for i := 0; i < 4; i++ {
					require.Zero(t, tick(t, d))
				}
				require.Zero(t, f.gateways["b"].Stats().Started)
				finish(t, first)
				settle(t, d)
			}
			require.Eventually(t, func() bool { return tick(t, d) == 1 }, time.Second, time.Millisecond)
			second := gatewayCall(t, f.gateways["b"])
			if limit == 2 {
				require.Equal(t, 1, f.gateways["a"].Stats().Active)
				finish(t, first)
			} else {
				channel, err := f.backend.GetChannel("a")
				require.NoError(t, err)
				require.Equal(t, db.ChannelAvailable, channel.State, "idle eviction commits drain and release")
			}
			finish(t, second)
			settle(t, d)
			d.mu.Lock()
			require.LessOrEqual(t, len(d.workers), limit)
			d.mu.Unlock()
		})
	}
}

func TestDispatcherContendersAndCloseDeadlineCannotBypassStuckCleanup(t *testing.T) {
	f := newRuntime(t, fake.Options{IgnoreRecoveryCancellation: true}, "gateway")
	f.ready("first", "gateway")
	f.ready("second", "gateway")
	one, two := f.dispatcher(1, 8), f.dispatcher(1, 8)
	require.Equal(t, 1, tick(t, one))
	call := gatewayCall(t, f.gateways["gateway"])
	call.Reply <- fake.Response{Outcome: executor.Outcome{Status: db.AttemptSucceeded}}
	phase(t, call.RecoveryStarted)
	require.Zero(t, tick(t, two))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err := one.Close(ctx)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Zero(t, tick(t, two))
	channel, err := f.backend.GetChannel("gateway")
	require.NoError(t, err)
	require.Equal(t, call.Request.AttemptID, channel.ActiveAttemptID)
	require.Equal(t, 1, f.gateways["gateway"].Stats().Started)
	call.Recover <- nil
	phase(t, call.CloseStarted)
	call.Close <- nil
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	require.NoError(t, one.Close(ctx))
	cancel()
	require.Eventually(t, func() bool { return tick(t, two) == 1 }, time.Second, time.Millisecond)
	second := gatewayCall(t, f.gateways["gateway"])
	require.Equal(t, "second", second.Request.RunID)
	require.Greater(t, second.Request.Channel.Epoch, call.Request.Channel.Epoch)
	finish(t, second)
	settle(t, two)
	require.Equal(t, 1, f.gateways["gateway"].Stats().MaxActive)
}

func TestBackgroundLoopsDiscoverDurableWorkAndResumeAfterDispatcherRestart(t *testing.T) {
	f := newRuntime(t, fake.Options{}, "gateway")
	m, err := NewMaterializer(MaterializerConfig{PollInterval: 5 * time.Millisecond}, f.backend, func() bool { return true })
	require.NoError(t, err)
	_, err = m.Put(context.Background(), db.ScheduleDefinition{ID: "once", ChannelID: "gateway", Executor: "fake",
		Spec: db.ScheduleSpec{Kind: db.ScheduleOnce, StartAt: time.Now().Add(20 * time.Millisecond)}, MissedPolicy: db.MissedCoalesce}, 0)
	require.NoError(t, err)
	d := f.dispatcher(1, 8)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	answers := make(chan error, 2)
	go func() { answers <- m.Run(ctx) }()
	go func() { answers <- d.Run(ctx) }()
	call := gatewayCall(t, f.gateways["gateway"])
	finish(t, call)
	settle(t, d)
	cancel()
	for i := 0; i < 2; i++ {
		select {
		case err := <-answers:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(3 * time.Second):
			t.Fatal("background loop did not stop")
		}
	}
	f.ready("after-restart", "gateway")
	restarted := f.dispatcher(1, 8)
	require.Equal(t, 1, tick(t, restarted))
	call = gatewayCall(t, f.gateways["gateway"])
	require.Equal(t, "after-restart", call.Request.RunID)
	finish(t, call)
	settle(t, restarted)
}
