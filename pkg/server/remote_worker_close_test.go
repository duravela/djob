package server

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/db"
	"github.com/duravela/djob/sdk/go"
	"github.com/duravela/djob/sdk/go/worker"
	"github.com/stretchr/testify/require"
)

// Simulate a version-1 client that reports failed Close without asking the
// controller to drain. The server must stop admission from that report alone.
type closeWithoutDrainTransport struct{ base http.RoundTripper }

func (c closeWithoutDrainTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if strings.HasSuffix(r.URL.Path, "/drain") {
		return &http.Response{StatusCode: http.StatusNoContent, Body: io.NopCloser(strings.NewReader("")), Header: make(http.Header)}, nil
	}
	return c.base.RoundTrip(r)
}

type failedCloseRemoteDriver struct {
	*remoteDriver
	failure                error
	closing, releaseClose  chan struct{}
	retrying, releaseRetry chan struct{}
}

func (d *failedCloseRemoteDriver) Close(context.Context) error {
	switch d.device.closed.Add(1) {
	case 1:
		close(d.closing)
		<-d.releaseClose
		return d.failure
	case 2:
		close(d.retrying)
		<-d.releaseRetry
	}
	return nil
}

func TestRemoteCloseFailureDoesNotClaimAnotherJobWhileLocalCleanupJoins(t *testing.T) {
	s, c := workerAPI(t)
	remotePrepare(t, s)
	device := &remoteDevice{}
	failure := errors.New("local close failed")
	driver := &failedCloseRemoteDriver{remoteDriver: &remoteDriver{device: device}, failure: failure,
		closing: make(chan struct{}), releaseClose: make(chan struct{}), retrying: make(chan struct{}), releaseRetry: make(chan struct{})}
	registry := remoteRegistry(device)
	registry.Drivers[djob.Version{Name: "device", Version: "1"}] = func(djob.JobRun) (djob.Driver, error) { device.factories.Add(1); return driver, nil }
	legacyClient := *c
	legacyClient.Transport = closeWithoutDrainTransport{base: c.Transport}
	_, done := runRemoteWorker(t, s, &legacyClient, "alpha", registry, func(cfg *worker.Config) { cfg.Transport = worker.TransportHTTPSV1 })
	t.Cleanup(func() {
		for _, ch := range []chan struct{}{driver.releaseClose, driver.releaseRetry} {
			select {
			case <-ch:
			default:
				close(ch)
			}
		}
	})
	remoteLaunch(t, s, "alpha", "invoke", "bad-close")
	select {
	case <-driver.closing:
	case <-time.After(3 * time.Second):
		t.Fatal("driver did not start closing")
	}
	// Queue independent work while the first conversation still owns its slot.
	remoteLaunch(t, s, "alpha", "compute", "next-job")
	remoteState(t, s, "alpha", "next-job", db.JobReady)
	close(driver.releaseClose)
	select {
	case <-driver.retrying:
	case <-time.After(3 * time.Second):
		t.Fatal("local cleanup retry did not start")
	}
	remoteState(t, s, "alpha", "bad-close", db.JobUnknown)
	resource, err := s.application.Store.GetJobResource("bus")
	require.NoError(t, err)
	require.Equal(t, db.ResourceBlocked, resource.State)
	require.Never(t, func() bool {
		view, err := s.application.Store.GetJobView("alpha", "next-job")
		return err != nil || view.Run.Status != db.JobReady || view.Run.AttemptCount != 0
	}, 200*time.Millisecond, 5*time.Millisecond, "a retiring session cannot claim and then fail an unrelated queued job")
	close(driver.releaseRetry)
	select {
	case err := <-done:
		require.ErrorIs(t, err, worker.ErrCleanupFailed)
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not finish its local cleanup")
	}
	require.EqualValues(t, 1, device.calls.Load())
	require.EqualValues(t, 2, device.closed.Load())
	action, err := s.application.Store.GetJobAction("alpha", "bad-close", 1)
	require.NoError(t, err)
	require.Equal(t, db.EffectSucceeded, action.Status, "a close failure cannot erase an acknowledged external effect")
	// Another worker can execute independent work. The old bus stays blocked.
	fresh := &remoteDevice{}
	runRemoteWorker(t, s, c, "alpha", remoteRegistry(fresh))
	remoteState(t, s, "alpha", "next-job", db.JobSucceeded)
	require.Zero(t, fresh.calls.Load())
	resource, err = s.application.Store.GetJobResource("bus")
	require.NoError(t, err)
	require.Equal(t, db.ResourceBlocked, resource.State)
}
