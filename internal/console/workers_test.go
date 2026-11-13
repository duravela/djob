package console

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"github.com/stretchr/testify/require"
)

func inventoryFixture() jp.WorkerInventory {
	return jp.WorkerInventory{ControllerID: "9007199254740993", InstanceID: "process-1", TenantID: "alpha", ObservedAt: time.Now().UTC(), Workers: []jp.WorkerInfo{{ID: "remote/display-id", WorkerID: "alpha-1", TenantID: "alpha", Mode: "remote", State: "connected", Runtimes: []string{"steps/v1"}, Handlers: []jp.WorkerVersion{{Name: "sha256", Version: "1"}}}}}
}

func observations(t *testing.T, w *httptest.ResponseRecorder) workerObservations {
	t.Helper()
	require.Equal(t, 200, w.Code, w.Body.String())
	var result workerObservations
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
	return result
}

func TestConsoleWorkerInventoryAggregatesPartialControllerObservations(t *testing.T) {
	var healthy, unavailable atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		unavailable.Add(1)
		require.Equal(t, "Bearer "+testToken, r.Header.Get("Authorization"))
		problem(w, 503, "private internal details")
	}))
	t.Cleanup(other.Close)
	b := newBrowser(t, func(w http.ResponseWriter, r *http.Request) {
		healthy.Add(1)
		require.Equal(t, "/api/jobs/v1/workers", r.URL.Path)
		require.Equal(t, "alpha", r.URL.Query().Get("tenant_id"))
		require.Equal(t, "16", r.URL.Query().Get("page_size"))
		v := inventoryFixture()
		v.NextPageToken = "controller-local-cursor"
		writeJSON(w, 200, v)
	}, func(cfg *Config) { cfg.Controllers = append(cfg.Controllers, other.URL) })
	w := b.call(http.MethodPost, "workers", map[string]string{})
	v := observations(t, w)
	require.Equal(t, "alpha", v.TenantID)
	require.Len(t, v.Controllers, 2)
	require.NotNil(t, v.Controllers[0].Inventory)
	require.Equal(t, "controller-local-cursor", v.Controllers[0].Inventory.NextPageToken)
	require.Nil(t, v.Controllers[1].Inventory)
	require.Equal(t, 503, v.Controllers[1].HTTPStatus)
	require.NotEmpty(t, v.Controllers[1].Error)
	require.NotContains(t, w.Body.String(), "private internal details")
	require.EqualValues(t, 1, healthy.Load())
	require.EqualValues(t, 1, unavailable.Load(), "inventory reads must never fail over or retry")
	require.Equal(t, 403, b.call(http.MethodPost, "workers", map[string]string{"tenant_id": "beta"}).Code)
	require.EqualValues(t, 1, healthy.Load(), "foreign tenant rejected before network access")
}

func TestConsoleWorkerInventoryPinsPagesAndRejectsUntrustedMetadata(t *testing.T) {
	var calls atomic.Int32
	b := newBrowser(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		require.Equal(t, "exact-page-token", r.URL.Query().Get("page_token"))
		v := inventoryFixture()
		v.Workers[0].TenantID, v.Workers[0].WorkerID = "beta", "private-beta-worker"
		writeJSON(w, 200, v)
	})
	for _, q := range []any{
		map[string]any{"controller_index": -1}, map[string]any{"controller_index": 1},
		map[string]any{"url": "http://attacker.invalid"}, map[string]any{"page_token": "unscoped-token"},
		map[string]any{"controller_index": 0, "page_token": strings.Repeat("x", 4097)},
	} {
		require.Equal(t, 400, b.call(http.MethodPost, "workers", q).Code)
	}
	require.Zero(t, calls.Load())
	w := b.call(http.MethodPost, "workers", map[string]any{"controller_index": 0, "page_token": "exact-page-token"})
	v := observations(t, w)
	require.Len(t, v.Controllers, 1)
	require.Equal(t, 502, v.Controllers[0].HTTPStatus)
	require.Nil(t, v.Controllers[0].Inventory)
	require.NotContains(t, w.Body.String(), "private-beta-worker")
	require.EqualValues(t, 1, calls.Load())
}

func TestConsoleWorkerInventoryValidatesBoundsAndRetainsEmptyObservations(t *testing.T) {
	base := inventoryFixture()
	require.True(t, validInventory(base, "alpha"))
	for _, change := range []func(*jp.WorkerInventory){
		func(v *jp.WorkerInventory) { v.ControllerID = "18446744073709551616" },
		func(v *jp.WorkerInventory) { v.Workers[0].Handlers = make([]jp.WorkerVersion, 65) },
		func(v *jp.WorkerInventory) { v.Workers[0].LocalTasks = -1 },
		func(v *jp.WorkerInventory) { v.Workers[0].State = "invented" },
		func(v *jp.WorkerInventory) { v.Workers = append(v.Workers, v.Workers[0]) },
	} {
		v := inventoryFixture()
		change(&v)
		require.False(t, validInventory(v, "alpha"))
	}
	b := newBrowser(t, func(w http.ResponseWriter, r *http.Request) {
		v := inventoryFixture()
		v.Workers = []jp.WorkerInfo{}
		writeJSON(w, 200, v)
	})
	v := observations(t, b.call(http.MethodPost, "workers", map[string]string{"tenant_id": "alpha"}))
	require.Empty(t, v.Controllers[0].Error)
	require.NotNil(t, v.Controllers[0].Inventory)
	require.Empty(t, v.Controllers[0].Inventory.Workers)
}

func TestConsoleWorkerInventoryBoundsFanoutAcrossBrowserSessions(t *testing.T) {
	var active, peak atomic.Int32
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	b := newBrowser(t, func(w http.ResponseWriter, r *http.Request) {
		n := active.Add(1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		defer active.Add(-1)
		select {
		case <-release:
			writeJSON(w, 200, inventoryFixture())
		case <-r.Context().Done():
		}
	}, func(cfg *Config) {
		for range 7 {
			cfg.Controllers = append(cfg.Controllers, cfg.Controllers[0])
		}
	})
	results := make(chan *httptest.ResponseRecorder, 2)
	for range 2 {
		go func() { results <- b.call(http.MethodPost, "workers", map[string]string{"tenant_id": "alpha"}) }()
	}
	require.Eventually(t, func() bool { return active.Load() == 4 }, time.Second, time.Millisecond)
	once.Do(func() { close(release) })
	for range 2 {
		v := observations(t, <-results)
		require.Len(t, v.Controllers, 8)
		for _, source := range v.Controllers {
			require.Empty(t, source.Error)
		}
	}
	require.LessOrEqual(t, peak.Load(), int32(4), "bounded across simultaneous browser requests")
}

func TestConsoleWorkerInventoryPreservesLongOpaqueControllerCursors(t *testing.T) {
	cursor := strings.Repeat("a", 3000)
	b := newBrowser(t, func(w http.ResponseWriter, r *http.Request) {
		v := inventoryFixture()
		if token := r.URL.Query().Get("page_token"); token == "" {
			v.NextPageToken = cursor
		} else {
			require.Equal(t, cursor, token)
		}
		writeJSON(w, 200, v)
	})
	first := observations(t, b.call(http.MethodPost, "workers", map[string]string{}))
	require.Empty(t, first.Controllers[0].Error)
	require.Equal(t, cursor, first.Controllers[0].Inventory.NextPageToken)
	second := observations(t, b.call(http.MethodPost, "workers", map[string]any{"controller_index": 0, "page_token": cursor}))
	require.Empty(t, second.Controllers[0].Error)
	require.Empty(t, second.Controllers[0].Inventory.NextPageToken)
}
