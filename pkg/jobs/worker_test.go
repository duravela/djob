package jobs

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
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

// Consensus delivery is replaced here; mutations still use the actual durable
// FSM, indexed Bolt storage, envelope encoding, and request receipt semantics.
type testBackend struct {
	*db.DB
	mu       sync.Mutex
	path     string
	commands []db.StateCommand
}

func newBackend(t *testing.T) *testBackend {
	t.Helper()
	b := &testBackend{path: filepath.Join(t.TempDir(), "jobs.db")}
	var err error
	b.DB, err = db.NewDB(b.path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, b.DB.Close()) })
	return b
}

func (b *testBackend) restart(t *testing.T) {
	t.Helper()
	require.NoError(t, b.DB.Close())
	var err error
	b.DB, err = db.NewDB(b.path)
	require.NoError(t, err)
}

func (b *testBackend) ProposeStateCommand(ctx context.Context, c db.StateCommand) (db.StateResult, error) {
	if err := ctx.Err(); err != nil {
		return db.StateResult{}, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	var data bytes.Buffer
	if err := gob.NewEncoder(&data).Encode(db.Command{Version: c.CommandVersion(), Type: db.CmdState, State: &c}); err != nil {
		return db.StateResult{}, err
	}
	result := b.ApplyEntries([]commit.Entry{{Index: b.LastAppliedIndex() + 1, Data: data.Bytes()}})
	if result.Error != nil {
		return db.StateResult{}, result.Error
	}
	if len(result.StateResults) != 1 {
		return db.StateResult{}, fmt.Errorf("missing state receipt")
	}
	b.commands = append(b.commands, c)
	r := result.StateResults[0]
	return r, r.Err()
}

func state(t *testing.T, b Backend, c db.JobCommand) db.StateResult {
	t.Helper()
	id, err := newID()
	require.NoError(t, err)
	r, err := b.ProposeStateCommand(context.Background(), db.StateCommand{RequestID: id, At: time.Now().UTC(), Job: &c})
	require.NoError(t, err)
	return r
}

func prepare(t *testing.T, b *testBackend, tenant, group, definition, binding string, resources []string) {
	t.Helper()
	if _, err := b.GetJobTenant(tenant); err != nil {
		state(t, b, db.JobCommand{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: tenant}}})
	}
	for _, resource := range resources {
		state(t, b, db.JobCommand{Grant: &db.PutResourceGrant{Grant: db.ResourceGrant{TenantID: tenant, ResourceID: resource, Allowed: true}}})
	}
	if group != "" {
		state(t, b, db.JobCommand{PutGroup: &db.PutJobGroup{Group: db.JobGroup{TenantID: tenant, ID: group, Runtime: db.StepRuntime, Resources: resources}}})
	}
	state(t, b, db.JobCommand{PutDefinition: &db.PutJobDefinition{Definition: db.JobDefinition{TenantID: tenant, ID: definition, GroupID: group, Runtime: db.StepRuntime, Handler: definition, HandlerVersion: "1", Input: []byte("input"), BindingID: binding}}})
}

func launch(t *testing.T, b Backend, tenant, definition, id string) {
	t.Helper()
	state(t, b, db.JobCommand{Launch: &db.LaunchJob{TenantID: tenant, DefinitionID: definition, DefinitionRevision: 1, RunID: id}})
}
func worker(t *testing.T, b Backend, cfg Config, registry Registry) *Worker {
	t.Helper()
	w, err := NewWorker(b, cfg, registry)
	require.NoError(t, err)
	return w
}

func TestComputeJobNeedsNoDriverOrCommunicationResource(t *testing.T) {
	b := newBackend(t)
	prepare(t, b, "tenant", "batch", "hash", "", nil)
	launch(t, b, "tenant", "hash", "r")
	w := worker(t, b, Config{}, Registry{Handlers: map[Version]Handler{{"hash", "1"}: func(v View) (Decision, error) {
		digest := sha256.Sum256(v.Run.Definition.Input)
		return Decision{Kind: db.JobComplete, Status: db.JobSucceeded, Result: digest[:]}, nil
	}}})
	r, err := w.Execute(context.Background(), "tenant", "r")
	require.NoError(t, err)
	require.Equal(t, db.JobSucceeded, r.Run.Status)
	require.Empty(t, r.Run.Resources)
	require.Zero(t, r.Run.ActionCount)
	want := sha256.Sum256([]byte("input"))
	require.Equal(t, want[:], r.Run.Result)
	b.restart(t)
	run, err := b.GetJobRun("tenant", "r")
	require.NoError(t, err)
	require.Equal(t, want[:], run.Result)
	channels, err := b.ListChannels(nil, 256)
	require.NoError(t, err)
	require.Empty(t, channels.Channels)
}

func TestHTTPConversationResumesCallbackAfterRestartWithoutResending(t *testing.T) {
	b := newBackend(t)
	var requests atomic.Int32
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("remote-receipt"))
	}))
	defer remote.Close()
	state(t, b, db.JobCommand{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: "alpha"}}})
	config, err := json.Marshal(HTTPBinding{Operations: []HTTPOperation{{Name: "request-report", Method: "POST", URL: remote.URL}}})
	require.NoError(t, err)
	state(t, b, db.JobCommand{PutBinding: &db.PutJobBinding{Binding: db.JobBinding{TenantID: "alpha", ID: "remote", Driver: "http", DriverVersion: "1", Config: config}}})
	prepare(t, b, "alpha", "", "report", "remote", nil)
	launch(t, b, "alpha", "report", "r")
	handler := func(v View) (Decision, error) {
		switch string(v.Run.State) {
		case "":
			return Decision{Kind: db.JobDoAction, State: []byte("submitted"), Operation: "request-report", Input: v.Run.Definition.Input}, nil
		case "submitted":
			if v.Run.PendingAction == nil || v.Run.PendingAction.Status != db.EffectSucceeded {
				return Decision{}, fmt.Errorf("request failed")
			}
			return Decision{Kind: db.JobWait, State: []byte("awaiting"), WaitKey: "report-42"}, nil
		case "awaiting":
			if v.Event == nil {
				return Decision{}, fmt.Errorf("missing callback")
			}
			return Decision{Kind: db.JobComplete, Status: db.JobSucceeded, Result: v.Event.Data}, nil
		}
		return Decision{}, fmt.Errorf("invalid checkpoint")
	}
	registry := Registry{Handlers: map[Version]Handler{{"report", "1"}: handler}, Drivers: map[Version]DriverFactory{{"http", "1"}: HTTPFactory(remote.Client())}}
	r, err := worker(t, b, Config{}, registry).Execute(context.Background(), "alpha", "r")
	require.NoError(t, err)
	require.True(t, r.Yielded)
	require.Equal(t, db.JobWaiting, r.Run.Status)
	require.Empty(t, r.Run.OwnerID)
	b.restart(t)
	callback, err := CallbackHandler(b, func(r *http.Request) (string, error) {
		switch r.Header.Get("Authorization") {
		case "Bearer alpha-secret":
			return "alpha", nil
		case "Bearer beta-secret":
			return "beta", nil
		}
		return "", fmt.Errorf("invalid credential")
	})
	require.NoError(t, err)
	post := func(auth, body string) int {
		req := httptest.NewRequest(http.MethodPost, "/callback", bytes.NewBufferString(body))
		req.Header.Set("Authorization", auth)
		response := httptest.NewRecorder()
		callback.ServeHTTP(response, req)
		return response.Code
	}
	body := `{"run_id":"r","event_id":"callback-1","key":"report-42","data":"cmVhZGluZw=="}`
	require.Equal(t, 401, post("", body))
	require.Equal(t, 404, post("Bearer beta-secret", body))
	require.Equal(t, 400, post("Bearer alpha-secret", `{"tenant_id":"beta","run_id":"r"}`))
	require.Equal(t, 202, post("Bearer alpha-secret", body))
	require.Equal(t, 202, post("Bearer alpha-secret", body))
	require.Equal(t, 409, post("Bearer alpha-secret", `{"run_id":"r","event_id":"callback-1","key":"report-42","data":"Y2hhbmdlZA=="}`))
	registry.Drivers[Version{"http", "1"}] = func(db.JobRun) (Driver, error) { return nil, fmt.Errorf("resume must not reopen the request driver") }
	r, err = worker(t, b, Config{}, registry).Execute(context.Background(), "alpha", "r")
	require.NoError(t, err)
	require.Equal(t, db.JobSucceeded, r.Run.Status)
	require.Equal(t, []byte("reading"), r.Run.Result)
	require.EqualValues(t, 2, r.Run.AttemptCount)
	require.EqualValues(t, 1, r.Run.ActionCount)
	require.EqualValues(t, 1, requests.Load())
	action, err := b.GetJobAction("alpha", "r", 1)
	require.NoError(t, err)
	require.Equal(t, []byte("remote-receipt"), action.Result)
}

func configureGateway(t *testing.T, b *testBackend, id string) {
	t.Helper()
	request, _ := newID()
	_, err := b.ProposeStateCommand(context.Background(), db.StateCommand{RequestID: request, At: time.Now().UTC(), Configure: &db.ConfigureChannel{ID: id, GatewayID: id, Protocol: "fake", Endpoint: "gateway:502"}})
	require.NoError(t, err)
	state(t, b, db.JobCommand{PutResource: &db.PutJobResource{Spec: db.ResourceSpec{ID: id, ChannelID: id, Duplex: "half", Capacity: 1}}})
}

func bindGateway(t *testing.T, b *testBackend, tenant string) {
	t.Helper()
	state(t, b, db.JobCommand{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: tenant}}})
	state(t, b, db.JobCommand{PutBinding: &db.PutJobBinding{Binding: db.JobBinding{TenantID: tenant, ID: "device", Driver: "gateway", DriverVersion: "1", Config: []byte(`{"channel_id":"bus"}`)}}})
}

func receiveCall(t *testing.T, g *fake.Gateway) *fake.Call {
	t.Helper()
	select {
	case call := <-g.Calls:
		return call
	case <-time.After(3 * time.Second):
		t.Fatal("no gateway exchange")
		return nil
	}
}
func answer(call *fake.Call, data string) {
	call.Reply <- fake.Response{Outcome: executor.Outcome{Status: db.AttemptSucceeded, Data: []byte(data)}}
	call.Recover <- nil
	call.Close <- nil
}

func TestConversationKeepsExclusiveBusAcrossCommandsRecoveryAndEventWait(t *testing.T) {
	b := newBackend(t)
	configureGateway(t, b, "bus")
	bindGateway(t, b, "alpha")
	bindGateway(t, b, "beta")
	prepare(t, b, "alpha", "pollers", "collect", "device", []string{"bus"})
	prepare(t, b, "beta", "commands", "collect", "device", []string{"bus"})
	launch(t, b, "alpha", "collect", "r")
	launch(t, b, "beta", "collect", "r")
	g := fake.New(fake.Options{})
	t.Cleanup(g.ReleaseAll)
	handler := func(v View) (Decision, error) {
		switch string(v.Run.State) {
		case "":
			return Decision{Kind: db.JobDoAction, State: []byte("selected"), Operation: "select-device"}, nil
		case "selected":
			return Decision{Kind: db.JobWait, State: []byte("selected-and-waiting"), WaitKey: "next-command"}, nil
		case "selected-and-waiting":
			return Decision{Kind: db.JobDoAction, State: []byte("reading"), Operation: "read", Input: v.Event.Data}, nil
		case "reading":
			return Decision{Kind: db.JobDoAction, State: []byte("finishing"), Operation: "finish", Input: v.Run.PendingAction.Result}, nil
		case "finishing":
			return Decision{Kind: db.JobComplete, Status: db.JobSucceeded, Result: v.Run.PendingAction.Result}, nil
		}
		return Decision{}, fmt.Errorf("invalid state")
	}
	factory := GatewayFactory(b, g.Factory, func(_ db.JobBinding, a Action) (string, []byte, error) {
		return "fake", append([]byte(a.Operation+":"), a.Input...), nil
	}, time.Second)
	registry := Registry{Handlers: map[Version]Handler{{"collect", "1"}: handler}, Drivers: map[Version]DriverFactory{{"gateway", "1"}: factory}}
	w := worker(t, b, Config{}, registry)
	type reply struct {
		result Result
		err    error
	}
	done := make(chan reply, 1)
	finished := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { defer close(finished); r, err := w.Execute(ctx, "alpha", "r"); done <- reply{r, err} }()
	t.Cleanup(func() {
		cancel()
		deadline := time.After(4 * time.Second)
		for {
			g.ReleaseAll()
			select {
			case <-finished:
				return
			case <-deadline:
				t.Error("conversation worker did not stop")
				return
			case <-time.After(time.Millisecond):
			}
		}
	})
	first := receiveCall(t, g)
	require.Equal(t, "select-device:", string(first.Request.Payload))
	first.Reply <- fake.Response{Outcome: executor.Outcome{Status: db.AttemptSucceeded}}
	select {
	case <-first.RecoveryStarted:
	case <-time.After(time.Second):
		t.Fatal("recovery did not begin")
	}
	assertBusy := func() {
		_, err := w.Execute(context.Background(), "beta", "r")
		require.ErrorContains(t, err, "resource_busy")
		channel, err := b.GetChannel("bus")
		require.NoError(t, err)
		id, _ := newID()
		_, err = b.ProposeStateCommand(context.Background(), db.StateCommand{RequestID: id, At: time.Now().UTC(), Acquire: &db.AcquireChannel{ChannelID: "bus", ExpectedEpoch: channel.Epoch, OwnerID: "legacy", LeaseUntil: time.Now().Add(time.Minute)}})
		require.ErrorContains(t, err, "channel_busy")
	}
	assertBusy()
	first.Recover <- nil
	first.Close <- nil
	require.Eventually(t, func() bool { v, _ := b.GetJobView("alpha", "r"); return v.Run.Status == db.JobWaiting }, time.Second, time.Millisecond)
	require.Zero(t, g.Stats().Active)
	assertBusy()
	state(t, b, db.JobCommand{Signal: &db.SignalJob{TenantID: "alpha", RunID: "r", EventID: "next", Key: "next-command", Data: []byte("measurement")}})
	second := receiveCall(t, g)
	require.Equal(t, "read:measurement", string(second.Request.Payload))
	answer(second, "reading-17")
	third := receiveCall(t, g)
	require.Equal(t, "finish:reading-17", string(third.Request.Payload))
	answer(third, "complete")
	select {
	case r := <-done:
		require.NoError(t, r.err)
		require.Equal(t, db.JobSucceeded, r.result.Run.Status)
	case <-time.After(3 * time.Second):
		t.Fatal("conversation did not finish")
	}
	require.Equal(t, 1, g.Stats().MaxActive)
	resource, err := b.GetJobResource("bus")
	require.NoError(t, err)
	require.Equal(t, db.ResourceAvailable, resource.State)
	channel, err := b.GetChannel("bus")
	require.NoError(t, err)
	require.Equal(t, db.ChannelAvailable, channel.State)
	// A different tenant/group can now acquire exactly that physical authority.
	state(t, b, db.JobCommand{Claim: &db.ClaimJob{JobOwner: db.JobOwner{TenantID: "beta", RunID: "r", AttemptID: "next-owner", OwnerID: "worker-beta"}, LeaseUntil: time.Now().Add(time.Minute)}})
}

type testDriver struct {
	execute func(context.Context, Action) Effect
	quiet   func(context.Context) error
	close   func(context.Context) error
}

func (d *testDriver) Execute(ctx context.Context, a Action) Effect { return d.execute(ctx, a) }
func (d *testDriver) Quiesce(ctx context.Context) error {
	if d.quiet != nil {
		return d.quiet(ctx)
	}
	return nil
}
func (d *testDriver) Close(ctx context.Context) error {
	if d.close != nil {
		return d.close(ctx)
	}
	return nil
}

func oneAction(v View) (Decision, error) {
	if v.Run.PendingAction == nil {
		return Decision{Kind: db.JobDoAction, Operation: "do", State: []byte("sent")}, nil
	}
	status := db.JobSucceeded
	if v.Run.PendingAction.Status != db.EffectSucceeded {
		status = db.JobFailed
	}
	return Decision{Kind: db.JobComplete, Status: status, Result: v.Run.PendingAction.Result}, nil
}

func setupAction(t *testing.T, b *testBackend, resource bool) {
	t.Helper()
	state(t, b, db.JobCommand{PutTenant: &db.PutJobTenant{Tenant: db.JobTenant{ID: "t"}}})
	state(t, b, db.JobCommand{PutBinding: &db.PutJobBinding{Binding: db.JobBinding{TenantID: "t", ID: "binding", Driver: "test", DriverVersion: "1"}}})
	var resources []string
	if resource {
		state(t, b, db.JobCommand{PutResource: &db.PutJobResource{Spec: db.ResourceSpec{ID: "resource", Duplex: "half", Capacity: 1}}})
		resources = []string{"resource"}
	}
	prepare(t, b, "t", "group", "job", "binding", resources)
	launch(t, b, "t", "job", "r")
}

func TestCleanupFailureBlocksResourceButKeepsKnownActionOutcome(t *testing.T) {
	b := newBackend(t)
	setupAction(t, b, true)
	d := &testDriver{execute: func(context.Context, Action) Effect {
		return Effect{Status: db.EffectSucceeded, Data: []byte("written")}
	}, quiet: func(context.Context) error { return errors.New("quiet period not established") }}
	w := worker(t, b, Config{}, Registry{Handlers: map[Version]Handler{{"job", "1"}: oneAction}, Drivers: map[Version]DriverFactory{{"test", "1"}: func(db.JobRun) (Driver, error) { return d, nil }}})
	r, err := w.Execute(context.Background(), "t", "r")
	require.NoError(t, err)
	require.Equal(t, db.JobUnknown, r.Run.Status)
	resource, err := b.GetJobResource("resource")
	require.NoError(t, err)
	require.Equal(t, db.ResourceBlocked, resource.State)
	action, err := b.GetJobAction("t", "r", 1)
	require.NoError(t, err)
	require.Equal(t, db.EffectSucceeded, action.Status)
	require.Equal(t, []byte("written"), action.Result)
}

type faultBackend struct {
	*testBackend
	before func(context.Context, db.StateCommand) error
	after  func(db.StateCommand, *db.StateResult) error
}

func (b *faultBackend) ProposeStateCommand(ctx context.Context, c db.StateCommand) (db.StateResult, error) {
	if b.before != nil {
		if err := b.before(ctx, c); err != nil {
			return db.StateResult{}, err
		}
	}
	r, err := b.testBackend.ProposeStateCommand(ctx, c)
	if err == nil && b.after != nil {
		err = b.after(c, &r)
	}
	return r, err
}

func TestLostActionAcknowledgementsNeverRepeatDriverIO(t *testing.T) {
	b := newBackend(t)
	setupAction(t, b, false)
	var start, complete, io atomic.Int32
	fault := &faultBackend{testBackend: b, after: func(c db.StateCommand, _ *db.StateResult) error {
		if c.Job.Advance != nil && c.Job.Advance.Kind == db.JobDoAction && start.Add(1) == 1 {
			return context.DeadlineExceeded
		}
		if c.Job.CompleteAction != nil && complete.Add(1) == 1 {
			return context.DeadlineExceeded
		}
		return nil
	}}
	w := worker(t, fault, Config{}, Registry{Handlers: map[Version]Handler{{"job", "1"}: oneAction}, Drivers: map[Version]DriverFactory{{"test", "1"}: func(db.JobRun) (Driver, error) {
		return &testDriver{execute: func(context.Context, Action) Effect { io.Add(1); return Effect{Status: db.EffectSucceeded} }}, nil
	}}})
	r, err := w.Execute(context.Background(), "t", "r")
	require.NoError(t, err)
	require.Equal(t, db.JobSucceeded, r.Run.Status)
	require.EqualValues(t, 1, io.Load())
	require.EqualValues(t, 2, start.Load())
	require.EqualValues(t, 2, complete.Load())
}

func TestMismatchedAuthorizationReceiptPreventsDriverCreation(t *testing.T) {
	b := newBackend(t)
	setupAction(t, b, false)
	var opened atomic.Int32
	fault := &faultBackend{testBackend: b, after: func(c db.StateCommand, r *db.StateResult) error {
		if c.Job.Advance != nil {
			r.RequestID = "wrong"
		}
		return nil
	}}
	w := worker(t, fault, Config{}, Registry{Handlers: map[Version]Handler{{"job", "1"}: oneAction}, Drivers: map[Version]DriverFactory{{"test", "1"}: func(db.JobRun) (Driver, error) { opened.Add(1); return nil, errors.New("must not open") }}})
	_, err := w.Execute(context.Background(), "t", "r")
	require.Error(t, err)
	require.Zero(t, opened.Load())
}

func TestLeaseLossCancelsButNeverDetachesAnUncooperativeDriver(t *testing.T) {
	b := newBackend(t)
	setupAction(t, b, true)
	started, cancelled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	blockedRenew := make(chan struct{})
	observations := make(chan ExecutionObservation, 8)
	var releaseOnce, renewOnce sync.Once
	fault := &faultBackend{testBackend: b, before: func(ctx context.Context, c db.StateCommand) error {
		if c.Job.Renew != nil {
			<-blockedRenew // Ignore proposal cancellation to exercise the independent watchdog.
			return ctx.Err()
		}
		return nil
	}}
	d := &testDriver{execute: func(ctx context.Context, _ Action) Effect {
		close(started)
		<-ctx.Done()
		close(cancelled)
		<-release
		return Effect{Status: db.EffectSucceeded}
	}}
	registry := Registry{Handlers: map[Version]Handler{{"job", "1"}: oneAction}, Drivers: map[Version]DriverFactory{{"test", "1"}: func(db.JobRun) (Driver, error) { return d, nil }}}
	cfg := Config{LeaseDuration: 400 * time.Millisecond, SafetyMargin: 80 * time.Millisecond, RenewInterval: 40 * time.Millisecond, CommitTimeout: 100 * time.Millisecond, PollInterval: time.Millisecond}
	cfg.OnExecution = func(o ExecutionObservation) { observations <- o }
	w := worker(t, fault, cfg, registry)
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() { defer close(finished); _, err := w.Execute(context.Background(), "t", "r"); done <- err }()
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		renewOnce.Do(func() { close(blockedRenew) })
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Error("blocked worker did not stop")
		}
	})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("driver did not start")
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("lease loss did not interrupt driver")
	}
	select {
	case err := <-done:
		t.Fatalf("detached running driver: %v", err)
	default:
	}
	resource, err := b.GetJobResource("resource")
	require.NoError(t, err)
	require.Equal(t, db.ResourceOwned, resource.State)
	require.Equal(t, ExecutionStarted, nextExecutionObservation(t, observations).Phase)
	assertExecutionPending(t, observations)
	releaseOnce.Do(func() { close(release) })
	renewOnce.Do(func() { close(blockedRenew) })
	select {
	case err := <-done:
		require.ErrorIs(t, err, ErrOwnershipLost)
	case <-time.After(time.Second):
		t.Fatal("worker did not stop")
	}
	returned := nextExecutionObservation(t, observations)
	require.Equal(t, "ownership_lost", returned.Result)
	require.False(t, returned.CleanupFailed)
	view, err := b.GetJobView("t", "r")
	require.NoError(t, err)
	until := view.Run.LeaseUntil
	if delay := time.Until(until); delay > 0 {
		time.Sleep(delay)
	}
	state(t, b, db.JobCommand{Expire: &db.ExpireJob{JobOwner: db.JobOwner{TenantID: "t", RunID: "r", AttemptID: view.Run.AttemptID, OwnerID: view.Run.OwnerID}, LeaseUntil: until}})
	resource, err = b.GetJobResource("resource")
	require.NoError(t, err)
	require.Equal(t, db.ResourceBlocked, resource.State)
	view, err = b.GetJobView("t", "r")
	require.NoError(t, err)
	require.Equal(t, db.JobUnknown, view.Run.Status)
	action, err := b.GetJobAction("t", "r", 1)
	require.NoError(t, err)
	require.Equal(t, db.EffectUnknown, action.Status)
}

func TestUnavailableHandlerYieldsPureWorkButPreservesFailureAndPhysicalSafety(t *testing.T) {
	for _, tc := range []struct {
		name      string
		failure   error
		resources []string
		want      db.JobStatus
		yielded   bool
	}{
		{"transport", fmt.Errorf("transport: %w", ErrHandlerUnavailable), nil, db.JobReady, true},
		{"application", errors.New("bad input"), nil, db.JobFailed, false},
		{"physical", ErrHandlerUnavailable, []string{"bus"}, db.JobUnknown, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newBackend(t)
			if len(tc.resources) > 0 {
				state(t, b, db.JobCommand{PutResource: &db.PutJobResource{Spec: db.ResourceSpec{ID: "bus", Duplex: "half", Capacity: 1}}})
			}
			prepare(t, b, "tenant", "batch", "handler", "", tc.resources)
			launch(t, b, "tenant", "handler", "r")
			w := worker(t, b, Config{}, Registry{Handlers: map[Version]Handler{{Name: "handler", Version: "1"}: func(View) (Decision, error) { return Decision{}, tc.failure }}})
			result, err := w.Execute(context.Background(), "tenant", "r")
			require.NoError(t, err)
			require.Equal(t, tc.want, result.Run.Status)
			require.Equal(t, tc.yielded, result.Yielded)
			if tc.yielded {
				resumed := worker(t, b, Config{}, Registry{Handlers: map[Version]Handler{{Name: "handler", Version: "1"}: func(View) (Decision, error) { return Decision{Kind: db.JobComplete, Status: db.JobSucceeded}, nil }}})
				completed, err := resumed.Execute(context.Background(), "tenant", "r")
				require.NoError(t, err)
				require.Equal(t, db.JobSucceeded, completed.Run.Status)
			}
		})
	}
}
