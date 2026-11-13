package server

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"sync"
	"testing"
	"time"

	consoleapp "github.com/duravela/djob/internal/console"
	"github.com/duravela/djob/pkg/consolesession"
	"github.com/duravela/djob/pkg/db"
	"github.com/stretchr/testify/require"
)

func sessionHTTP(t *testing.T, s *Server, token, action string, body any) *http.Response {
	t.Helper()
	data, err := json.Marshal(body)
	require.NoError(t, err)
	r, err := http.NewRequest(http.MethodPost, "http://"+s.HTTPAddr()+"/api/jobs/v1/console-sessions/"+action, bytes.NewReader(data))
	require.NoError(t, err)
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(r)
	require.NoError(t, err)
	return response
}

func TestSharedConsoleAcrossReplicasRetainsUncertainCommandAndLogout(t *testing.T) {
	members := startReadinessServers(t, func(c *Config) { c.ConsoleSessions = true; c.Admission.Disabled = true })
	controller := members[0]
	policy := &db.ConsoleSessionPolicy{MaxRecords: 128, MaxCiphertextBytes: 64 << 20, Limits: consolesession.Limits{MaxCiphertextBytes: 4 << 20, MaxLifetime: 24 * time.Hour}}
	activated := sessionHTTP(t, controller, genericToken("ops/alice"), "activate", db.ConsoleSessionCommand{Policy: policy})
	payload, _ := io.ReadAll(activated.Body)
	activated.Body.Close()
	require.Equal(t, 200, activated.StatusCode, string(payload))
	client := genericClient(t, controller)
	tenantAPI(t, client, "alpha")
	definitionAPI(t, client, "alpha", "compute", "sha256", "", []byte(`{}`))
	var mu sync.Mutex
	var commands [][]byte
	drop := true
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		req, err := http.NewRequestWithContext(r.Context(), r.Method, "http://"+controller.HTTPAddr()+r.URL.Path, bytes.NewReader(data))
		require.NoError(t, err)
		req.Header = r.Header.Clone()
		out, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer out.Body.Close()
		body, _ := io.ReadAll(out.Body)
		if r.URL.Path == "/api/jobs/v1/mutate" {
			mu.Lock()
			commands = append(commands, append([]byte(nil), data...))
			shouldDrop := drop
			drop = false
			mu.Unlock()
			if shouldDrop {
				conn, _, err := w.(http.Hijacker).Hijack()
				require.NoError(t, err)
				conn.Close()
				return
			}
		}
		for k, v := range out.Header {
			w.Header()[k] = v
		}
		w.WriteHeader(out.StatusCode)
		w.Write(body)
	}))
	defer proxy.Close()
	const origin = "http://127.0.0.1:8899"
	first, err := consoleapp.New(consoleapp.Config{Origin: origin, Controllers: []string{proxy.URL}, SharedSessions: true})
	require.NoError(t, err)
	defer first.Close()
	target, _ := url.Parse("http://" + members[1].HTTPAddr())
	reverse := httputil.NewSingleHostReverseProxy(target)
	secondProxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/jobs/v1/mutate" {
			data, _ := io.ReadAll(r.Body)
			r.Body = io.NopCloser(bytes.NewReader(data))
			mu.Lock()
			commands = append(commands, append([]byte(nil), data...))
			mu.Unlock()
		}
		reverse.ServeHTTP(w, r)
	}))
	defer secondProxy.Close()
	second, err := consoleapp.New(consoleapp.Config{Origin: origin, Controllers: []string{secondProxy.URL}, SharedSessions: true})
	require.NoError(t, err)
	defer second.Close()
	var cookie *http.Cookie
	csrf := ""
	call := func(app *consoleapp.Server, method, path string, body any) *httptest.ResponseRecorder {
		data, _ := json.Marshal(body)
		r := httptest.NewRequest(method, origin+"/api/console/v1/"+path, bytes.NewReader(data))
		r.Header.Set("Origin", origin)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-Djob-CSRF", csrf)
		if cookie != nil {
			r.AddCookie(cookie)
		}
		w := httptest.NewRecorder()
		app.ServeHTTP(w, r)
		return w
	}
	login := call(first, "POST", "session", map[string]string{"token": genericToken("alpha/writer")})
	require.Equal(t, 200, login.Code, login.Body.String())
	cookie = login.Result().Cookies()[0]
	var info struct {
		CSRF string `json:"csrf"`
	}
	require.NoError(t, json.Unmarshal(login.Body.Bytes(), &info))
	csrf = info.CSRF
	prepared := call(first, "POST", "mutations", map[string]any{"tenant_id": "alpha", "launch": map[string]any{"definition_id": "compute", "definition_revision": "1", "run_id": "shared-run"}})
	require.Equal(t, 200, prepared.Code, prepared.Body.String())
	var pending struct {
		RequestID string          `json:"request_id"`
		State     string          `json:"state"`
		Request   json.RawMessage `json:"request"`
	}
	require.NoError(t, json.Unmarshal(prepared.Body.Bytes(), &pending))
	command := map[string]string{"request_id": pending.RequestID}
	uncertain := call(first, "POST", "mutations/commit", command)
	require.Equal(t, 200, uncertain.Code, uncertain.Body.String())
	require.NoError(t, json.Unmarshal(uncertain.Body.Bytes(), &pending))
	require.Equal(t, "uncertain", pending.State)
	first.Close()
	restored := call(second, "GET", "session", nil)
	require.Equal(t, 200, restored.Code, restored.Body.String())
	require.Contains(t, restored.Body.String(), pending.RequestID)
	confirmed := call(second, "POST", "mutations/commit", command)
	require.Equal(t, 200, confirmed.Code, confirmed.Body.String())
	require.NoError(t, json.Unmarshal(confirmed.Body.Bytes(), &pending))
	require.Equal(t, "confirmed", pending.State)
	// Both replicas forwarded exactly the same retained command bytes.
	mu.Lock()
	require.Len(t, commands, 2)
	require.Equal(t, commands[0], commands[1])
	mu.Unlock()
	token := cookie.Value
	derived := sessionHTTP(t, controller, token, "create", db.ConsoleSessionCommand{})
	derived.Body.Close()
	require.Equal(t, 401, derived.StatusCode, "delegated credentials cannot create sessions")
	id, _, err := consolesession.ParseCredential(token)
	require.NoError(t, err)
	record, err := controller.application.Store.GetConsoleSession(id)
	require.NoError(t, err)
	require.NotContains(t, string(record.Ciphertext), "shared-run")
	require.NotContains(t, string(record.Ciphertext), genericToken("alpha/writer"))
	bad := csrf
	csrf = "invalid"
	rejected := call(second, "DELETE", "session", nil)
	require.Equal(t, 403, rejected.Code)
	csrf = bad
	logout := call(second, "DELETE", "session", nil)
	require.Equal(t, 200, logout.Code, logout.Body.String())
	require.Equal(t, 200, call(second, "DELETE", "session", nil).Code, "logout retry must acknowledge the retained tombstone")
	require.Equal(t, 401, call(second, "GET", "session", nil).Code)
}
