package console

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	jp "github.com/duravela/djob/pkg/proto/jobs/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
)

const testOrigin = "http://127.0.0.1:8099"
const testToken = "console-test-credential-abcdefghijklmnopqrstuvwxyz"

type browser struct {
	t      *testing.T
	server *Server
	cookie *http.Cookie
	csrf   string
}

func newBrowser(t *testing.T, handler http.HandlerFunc, options ...func(*Config)) *browser {
	t.Helper()
	return newBrowserAs(t, Identity{Subject: "alpha/alice", TenantID: "alpha", Role: "writer"}, handler, options...)
}

func newBrowserAs(t *testing.T, identity Identity, handler http.HandlerFunc, options ...func(*Config)) *browser {
	t.Helper()
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer "+testToken, r.Header.Get("Authorization"))
		if r.URL.Path == "/api/jobs/v1/identity" {
			writeJSON(w, 200, identity)
			return
		}
		handler(w, r)
	}))
	t.Cleanup(controller.Close)
	cfg := Config{Origin: testOrigin, Controllers: []string{controller.URL}}
	for _, option := range options {
		option(&cfg)
	}
	s, err := New(cfg)
	require.NoError(t, err)
	t.Cleanup(s.Close)
	b := &browser{t: t, server: s}
	w := b.call(http.MethodPost, "session", map[string]string{"token": testToken})
	require.Equal(t, 200, w.Code, w.Body.String())
	require.NotContains(t, w.Body.String(), testToken)
	var data struct {
		CSRF string `json:"csrf"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &data))
	b.csrf = data.CSRF
	b.cookie = w.Result().Cookies()[0]
	return b
}

func (b *browser) call(method, path string, body any, change ...func(*http.Request)) *httptest.ResponseRecorder {
	b.t.Helper()
	data, err := json.Marshal(body)
	require.NoError(b.t, err)
	r := httptest.NewRequest(method, testOrigin+api+path, bytes.NewReader(data))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Origin", testOrigin)
	r.Header.Set("X-Djob-CSRF", b.csrf)
	if b.cookie != nil {
		r.AddCookie(b.cookie)
	}
	for _, fn := range change {
		fn(r)
	}
	w := httptest.NewRecorder()
	b.server.ServeHTTP(w, r)
	return w
}

func (b *browser) prepared() pendingMutation {
	b.t.Helper()
	w := b.call(http.MethodPost, "mutations", map[string]any{"tenant_id": "alpha", "launch": map[string]string{"definition_id": "hash", "definition_revision": "1"}})
	require.Equal(b.t, 200, w.Code, w.Body.String())
	var p pendingMutation
	require.NoError(b.t, json.Unmarshal(w.Body.Bytes(), &p))
	return p
}

func (b *browser) commit(id string) pendingMutation {
	b.t.Helper()
	w := b.call(http.MethodPost, "mutations/commit", map[string]string{"request_id": id})
	require.Equal(b.t, 200, w.Code, w.Body.String())
	var p pendingMutation
	require.NoError(b.t, json.Unmarshal(w.Body.Bytes(), &p))
	return p
}

func TestConsoleSessionHostOriginCSRFAndCredentialBoundaries(t *testing.T) {
	var queries atomic.Int32
	b := newBrowser(t, func(w http.ResponseWriter, r *http.Request) {
		queries.Add(1)
		require.Equal(t, "/api/jobs/v1/list", r.URL.Path)
		writeJSON(w, 200, map[string]any{"records": []any{}})
	})
	require.True(t, b.cookie.HttpOnly)
	require.False(t, b.cookie.Secure)
	require.Equal(t, http.SameSiteStrictMode, b.cookie.SameSite)
	q := map[string]string{"kind": "RUN", "tenant_id": "alpha"}
	for _, row := range []struct {
		name   string
		status int
		change func(*http.Request)
	}{
		{"csrf", 403, func(r *http.Request) { r.Header.Del("X-Djob-CSRF") }},
		{"origin", 403, func(r *http.Request) { r.Header.Set("Origin", "https://attacker.invalid") }},
		{"host", 421, func(r *http.Request) { r.Host = "attacker.invalid" }},
		{"anonymous", 401, func(r *http.Request) { r.Header.Del("Cookie") }},
	} {
		t.Run(row.name, func(t *testing.T) {
			w := b.call(http.MethodPost, "query/list", q, row.change)
			require.Equal(t, row.status, w.Code)
			require.Empty(t, w.Header().Get("Access-Control-Allow-Origin"))
			require.Equal(t, "no-store", w.Header().Get("Cache-Control"))
		})
	}
	require.Zero(t, queries.Load())
	w := b.call(http.MethodPost, "query/list", q, func(r *http.Request) { r.Header.Set("Authorization", "Bearer forged-browser-token") })
	require.Equal(t, 200, w.Code)
	require.EqualValues(t, 1, queries.Load())
	require.Equal(t, 404, b.call(http.MethodPost, "query/begin-io", q).Code)
	require.Equal(t, 200, b.call(http.MethodDelete, "session", map[string]any{}).Code)
	require.Equal(t, 401, b.call(http.MethodGet, "session", nil).Code)
}

func TestConsoleLostAcknowledgementRetainsExactMutationAcrossReloadAndRetry(t *testing.T) {
	var calls atomic.Int32
	var original []byte
	b := newBrowser(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/jobs/v1/mutate", r.URL.Path)
		data, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		switch calls.Add(1) {
		case 1:
			original = data
			conn, _, err := w.(http.Hijacker).Hijack()
			require.NoError(t, err)
			_ = conn.Close() // Controller could have committed before this loss.
		case 2:
			require.Equal(t, original, data)
			problem(w, 401, "credential rotated after the first attempt")
		default:
			require.Equal(t, original, data)
			var q jp.MutationRequest
			require.NoError(t, protojson.Unmarshal(data, &q))
			writeJSON(w, 200, map[string]any{"request_id": q.Command.RequestId, "reference": map[string]string{"kind": "RUN", "tenant_id": "alpha", "id": q.GetLaunch().RunId}, "status": "ready"})
		}
	})
	p := b.prepared()
	require.Equal(t, "prepared", p.State)
	var q jp.MutationRequest
	require.NoError(t, protojson.Unmarshal(p.Request, &q))
	require.NotEmpty(t, q.GetLaunch().RunId)
	require.NotNil(t, q.Command.At)
	require.Equal(t, "uncertain", b.commit(p.RequestID).State)
	// A later authentication failure cannot prove that the original did not commit.
	require.Equal(t, "uncertain", b.commit(p.RequestID).State)
	require.Equal(t, 409, b.call(http.MethodDelete, "mutations/current", map[string]string{"request_id": p.RequestID}).Code)
	require.Equal(t, 409, b.call(http.MethodPost, "mutations", map[string]any{"launch": map[string]string{"definition_id": "other", "definition_revision": "1"}}).Code)
	loaded := b.call(http.MethodGet, "session", nil)
	require.Contains(t, loaded.Body.String(), p.RequestID)
	require.Contains(t, loaded.Body.String(), "uncertain")
	require.Equal(t, "confirmed", b.commit(p.RequestID).State)
	require.Equal(t, "confirmed", b.commit(p.RequestID).State)
	require.EqualValues(t, 3, calls.Load(), "confirmed commit must not be transmitted again")
	require.Equal(t, 409, b.call(http.MethodDelete, "mutations/current", map[string]string{"request_id": "stale-tab"}).Code)
	require.Equal(t, 200, b.call(http.MethodDelete, "mutations/current", map[string]string{"request_id": p.RequestID}).Code)
}

func TestConsoleRejectsMismatchedReceiptsAndPrivilegedRecoveryPreparation(t *testing.T) {
	b := newBrowser(t, func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, 200, map[string]any{"request_id": "someone-else", "reference": map[string]string{"kind": "RUN", "id": "other"}})
	})
	require.Equal(t, 403, b.call(http.MethodPost, "mutations", map[string]any{"resolve_resource": map[string]any{"resource_id": "bus", "quiescent": true}}).Code)
	require.Equal(t, 400, b.call(http.MethodPost, "mutations", map[string]any{"command": map[string]string{"request_id": "caller-forged"}, "launch": map[string]string{"run_id": "r"}}).Code)
	p := b.prepared()
	result := b.commit(p.RequestID)
	require.Equal(t, "uncertain", result.State)
	require.Contains(t, result.Error, "does not match")
}

func TestConsoleSessionExpirySecureCookiesAndCapacity(t *testing.T) {
	b := newBrowser(t, func(http.ResponseWriter, *http.Request) {}, func(c *Config) { c.MaxSessions = 1 })
	require.Equal(t, 503, b.call(http.MethodPost, "session", map[string]string{"token": testToken}, func(r *http.Request) { r.Header.Del("Cookie") }).Code)
	b.server.mu.Lock()
	b.server.sessions[b.cookie.Value].expires = time.Now().Add(-time.Second)
	b.server.mu.Unlock()
	require.Equal(t, 401, b.call(http.MethodGet, "session", nil).Code)
	s, err := New(Config{Origin: "https://console.example.com", Controllers: []string{"https://controller.example.com"}})
	require.NoError(t, err)
	t.Cleanup(s.Close)
	cookie := s.cookie("opaque", time.Now().Add(time.Hour), 3600)
	require.Equal(t, "__Host-djob-console", cookie.Name)
	require.True(t, cookie.Secure)
	require.True(t, cookie.HttpOnly)
	require.Empty(t, cookie.Domain)
}

func TestConsoleValidatesTrustAndRefusesCredentialForwardingOnRedirect(t *testing.T) {
	for _, raw := range []string{"http://example.com", "http://localhost:8080", "https://user:password@example.com", "https://example.com/api", "https://example.com?token=secret", "https://example.com#fragment"} {
		_, err := New(Config{Origin: testOrigin, Controllers: []string{raw}})
		require.Error(t, err, raw)
	}
	_, err := New(Config{Origin: testOrigin, Controllers: []string{"https://example.com"}, TLS: &tls.Config{InsecureSkipVerify: true}})
	require.Error(t, err)
	var leaked atomic.Int32
	other := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { leaked.Add(1) }))
	t.Cleanup(other.Close)
	b := newBrowser(t, func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, other.URL, 307) })
	w := b.call(http.MethodPost, "query/list", map[string]string{"kind": "RUN"})
	require.Equal(t, 502, w.Code)
	require.Zero(t, leaked.Load())
}

func TestConsoleAssetsAndRequestBounds(t *testing.T) {
	b := newBrowser(t, func(http.ResponseWriter, *http.Request) {})
	for _, path := range []string{"/", "/app.js", "/model.js", "/history.js", "/recovery.js", "/references.js", "/forms.js", "/routing.js", "/usage.js", "/enrollments.js", "/style.css"} {
		w := httptest.NewRecorder()
		b.server.ServeHTTP(w, httptest.NewRequest(http.MethodGet, testOrigin+path, nil))
		require.Equal(t, 200, w.Code, path)
		require.NotEmpty(t, w.Body.Bytes())
		require.Contains(t, w.Header().Get("Content-Security-Policy"), "frame-ancestors 'none'")
	}
	w := b.call(http.MethodPost, "mutations", map[string]string{"padding": strings.Repeat("x", maxRequest)})
	require.Equal(t, 400, w.Code)
}

func TestConsoleReadFailoverAndExplicitMutationRetryUseConfiguredControllers(t *testing.T) {
	var primaryWrites, fallbackWrites atomic.Int32
	var firstEnvelope atomic.Value
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer "+testToken, r.Header.Get("Authorization"))
		if r.URL.Path == "/api/jobs/v1/list" {
			writeJSON(w, 200, map[string]any{"records": []any{}})
			return
		}
		fallbackWrites.Add(1)
		data, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, firstEnvelope.Load().([]byte), data)
		var q jp.MutationRequest
		require.NoError(t, protojson.Unmarshal(data, &q))
		writeJSON(w, 200, map[string]any{"request_id": q.Command.RequestId, "reference": map[string]string{"kind": "RUN", "id": q.GetLaunch().RunId, "tenant_id": "alpha"}})
	}))
	t.Cleanup(fallback.Close)
	b := newBrowser(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/jobs/v1/mutate" {
			primaryWrites.Add(1)
			data, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			firstEnvelope.Store(data)
		}
		problem(w, 503, "quorum unavailable")
	}, func(c *Config) { c.Controllers = append(c.Controllers, fallback.URL) })
	result := b.call(http.MethodPost, "query/list", map[string]string{"kind": "RUN"})
	require.Equal(t, 200, result.Code)
	require.Equal(t, fallback.URL, result.Header().Get("X-Djob-Controller"))
	b.server.mu.Lock()
	sess := b.server.sessions[b.cookie.Value]
	b.server.mu.Unlock()
	sess.mu.Lock()
	sess.controller = 0
	sess.mu.Unlock()
	p := b.prepared()
	require.Equal(t, "uncertain", b.commit(p.RequestID).State)
	require.EqualValues(t, 1, primaryWrites.Load())
	require.Zero(t, fallbackWrites.Load(), "mutation must not fail over without an explicit exact retry")
	require.Equal(t, "confirmed", b.commit(p.RequestID).State)
	require.EqualValues(t, 1, fallbackWrites.Load())
}
