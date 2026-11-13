package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/duravela/djob/pkg/jobs"
	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"github.com/duravela/djob/sdk/go"
	wire "github.com/duravela/djob/sdk/go/worker/protocol/v1"
	"github.com/stretchr/testify/require"
)

func TestWorkerInventoryScopesCredentialsAndPaginationToLocalProcess(t *testing.T) {
	svc, err := NewJobsService(nil, *genericCredentials())
	require.NoError(t, err)
	s := svc.(*jobsServer)
	instance := "first-process"
	s.inventory = func(tenant string, at time.Time) jp.WorkerInventory {
		return jp.WorkerInventory{ControllerID: "9007199254740993", InstanceID: instance, TenantID: tenant, ObservedAt: at, Workers: []jp.WorkerInfo{{ID: "a"}, {ID: "b"}, {ID: "c"}}}
	}
	call := func(path, token string, want int) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest(http.MethodGet, "/api/jobs/v1/workers"+path, nil)
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		r.Header.Set("Grpc-Metadata-Authorization", "Bearer "+genericToken("ops/alice"))
		r.Header.Set("X-Tenant-Id", "beta")
		w := httptest.NewRecorder()
		s.HTTPHandler().ServeHTTP(w, r)
		require.Equal(t, want, w.Code, w.Body.String())
		require.Equal(t, "no-store", w.Header().Get("Cache-Control"))
		return w
	}
	call("?tenant_id=alpha", "", 401)
	for _, subject := range []string{"alpha/callback", "alpha/producer", "reader"} {
		call("?tenant_id=alpha", genericToken(subject), 403)
	}
	call("?tenant_id=beta", genericToken("alpha/reader"), 403)
	call("", genericToken("ops/alice"), 400)
	for _, query := range []string{"?page_size=17", "?page_size=0", "?page_size=1&page_size=2", "?url=http://elsewhere", "?page_token=" + strings.Repeat("a", 4097)} {
		call(query, genericToken("alpha/reader"), 400)
	}
	var first jp.WorkerInventory
	require.NoError(t, json.Unmarshal(call("?page_size=1", genericToken("alpha/reader"), 200).Body.Bytes(), &first))
	require.Equal(t, "alpha", first.TenantID)
	require.Equal(t, "9007199254740993", first.ControllerID)
	require.Equal(t, "a", first.Workers[0].ID)
	require.NotEmpty(t, first.NextPageToken)
	var second jp.WorkerInventory
	next := "?page_size=1&page_token=" + url.QueryEscape(first.NextPageToken)
	require.NoError(t, json.Unmarshal(call(next, genericToken("alpha/writer"), 200).Body.Bytes(), &second))
	require.Equal(t, "b", second.Workers[0].ID)
	call(next, genericToken("beta/reader"), 400)
	instance = "restarted-process"
	call(next, genericToken("alpha/reader"), 400)
	s.inventory = nil
	call("", genericToken("alpha/reader"), 503)
}

func TestWorkerInventoryReportsRealRegistrationsWithoutSessionAuthority(t *testing.T) {
	s, client := workerAPI(t)
	register := func(tenant, id string) wire.Registered {
		t.Helper()
		var registered wire.Registered
		workerPost(t, client, s, workerToken(tenant), wire.Path+"register", wire.Register{Protocol: wire.Version, TenantID: tenant, WorkerID: id,
			Handlers: []djob.Version{{Name: "sha256", Version: "2"}, {Name: "sha256", Version: "1"}},
			Drivers:  []djob.Version{{Name: "mqtt", Version: "1"}}}, &registered, 200)
		return registered
	}
	a := register("alpha", "alpha-worker")
	_ = register("beta", "private-beta-worker")
	get := func() jp.WorkerInventory {
		t.Helper()
		data := authenticatedHTTP(t, client, "https://"+s.HTTPAddr(), http.MethodGet, "/api/jobs/v1/workers", genericToken("alpha/reader"), nil, nil, 200, nil)
		for _, secret := range []string{a.SessionID, workerToken("alpha"), "private-beta-worker"} {
			require.NotContains(t, string(data), secret)
		}
		var inventory jp.WorkerInventory
		require.NoError(t, json.Unmarshal(data, &inventory))
		return inventory
	}
	info := get().Workers
	require.Len(t, info, 1)
	require.Equal(t, "alpha-worker", info[0].WorkerID)
	require.Equal(t, workerDisplayID(a.SessionID), info[0].ID)
	require.Equal(t, "registered", info[0].State)
	require.Equal(t, []jp.WorkerVersion{{Name: "sha256", Version: "1"}, {Name: "sha256", Version: "2"}}, info[0].Handlers)
	require.Equal(t, []jp.WorkerVersion{{Name: "mqtt", Version: "1"}}, info[0].Drivers)
	require.NotNil(t, info[0].LastSeen)
	stale := s.workerInventory("alpha", time.Now().Add(s.remoteWorkers.cfg.IdleTimeout))
	require.Equal(t, "stale", stale.Workers[0].State)
	for _, token := range []string{workerToken("alpha"), genericToken("alpha/callback")} {
		authenticatedHTTP(t, client, "https://"+s.HTTPAddr(), http.MethodGet, "/api/jobs/v1/workers", token, nil, nil, 403, nil)
	}
	workerPost(t, client, s, workerToken("alpha"), wire.Path+a.SessionID+"/drain", struct{}{}, nil, 204)
	require.Equal(t, "draining", get().Workers[0].State)
	workerPost(t, client, s, workerToken("alpha"), wire.Path+a.SessionID+"/disconnect", struct{}{}, nil, 204)
	require.Empty(t, get().Workers)
}

func TestWorkerCapabilitiesBoundLargeEmbeddedRegistries(t *testing.T) {
	registry := jobs.Registry{Handlers: map[jobs.Version]jobs.Handler{}}
	for i := range 65 {
		registry.Handlers[jobs.Version{Name: fmt.Sprintf("handler-%02d", i), Version: "1"}] = nil
	}
	info := workerCapabilities(registry)
	require.Len(t, info.Handlers, 64)
	require.True(t, info.CapabilitiesTruncated)
	registry.Handlers = map[jobs.Version]jobs.Handler{{Name: strings.Repeat("a", 257), Version: "1"}: nil}
	info = workerCapabilities(registry)
	require.Empty(t, info.Handlers)
	require.True(t, info.CapabilitiesTruncated)
}

func TestWorkerInventoryBoundsEscapedMetadataAndContinuesAtTheLastReturnedWorker(t *testing.T) {
	svc, err := NewJobsService(nil, *genericCredentials())
	require.NoError(t, err)
	s := svc.(*jobsServer)
	tenant := strings.Repeat("\x01", 256)
	s.inventory = func(tenant string, at time.Time) jp.WorkerInventory {
		versions := make([]jp.WorkerVersion, 64)
		for i := range versions {
			versions[i] = jp.WorkerVersion{Name: strings.Repeat("\x01", 256), Version: strings.Repeat("\x02", 256)}
		}
		first := jp.WorkerInfo{ID: "a", Handlers: versions, Drivers: versions}
		second := jp.WorkerInfo{ID: "b", Handlers: []jp.WorkerVersion{}}
		encodedSize := func() int {
			a, _ := json.Marshal(first)
			b, _ := json.Marshal(second)
			return len(a) + len(b) + 1
		}
		// Fill entries near the page limit. The escaped tenant and encoded
		// cursor together need more than 2 KiB, exercising envelope accounting.
		target := workerInventoryReplyLimit - 2048
		for encodedSize() < target {
			second.Handlers = append(second.Handlers, versions[0])
			if encodedSize() <= target {
				continue
			}
			last := &second.Handlers[len(second.Handlers)-1]
			last.Name, last.Version = "\x01", "\x02"
			extra := (target - encodedSize()) / 6
			last.Name = strings.Repeat("\x01", 1+min(extra, 255))
			last.Version = strings.Repeat("\x02", 1+max(0, extra-255))
			break
		}
		return jp.WorkerInventory{ControllerID: "1", InstanceID: "instance", TenantID: tenant, ObservedAt: at, Workers: []jp.WorkerInfo{first, second, {ID: "c"}}}
	}
	read := func(token string) jp.WorkerInventory {
		t.Helper()
		r := httptest.NewRequest(http.MethodGet, "/api/jobs/v1/workers?tenant_id="+url.QueryEscape(tenant)+"&page_size=16&page_token="+url.QueryEscape(token), nil)
		r.Header.Set("Authorization", "Bearer "+genericToken("ops/alice"))
		w := httptest.NewRecorder()
		s.HTTPHandler().ServeHTTP(w, r)
		require.Equal(t, 200, w.Code)
		require.LessOrEqual(t, w.Body.Len(), workerInventoryReplyLimit)
		var result jp.WorkerInventory
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &result))
		return result
	}
	first := read("")
	require.Len(t, first.Workers, 1)
	require.Equal(t, "a", first.Workers[0].ID)
	require.NotEmpty(t, first.NextPageToken)
	second := read(first.NextPageToken)
	require.Len(t, second.Workers, 2)
	require.Equal(t, "b", second.Workers[0].ID)
	require.Equal(t, "c", second.Workers[1].ID)
	require.Empty(t, second.NextPageToken)
}
