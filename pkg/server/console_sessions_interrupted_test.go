package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	consoleapp "github.com/duravela/djob/internal/console"
	"github.com/duravela/djob/pkg/consolesession"
	"github.com/stretchr/testify/require"
)

func TestSharedConsoleInterruptedSubmissionRequiresExplicitExactRetry(t *testing.T) {
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	cfg.ConsoleSessions = true
	controller := startAPI(t, cfg)
	activateControllerSessions(t, controller)
	client := genericClient(t, controller)
	tenantAPI(t, client, "alpha")
	definitionAPI(t, client, "alpha", "compute", "sha256", "", []byte(`{}`))
	forwarded := make(chan []byte, 1)
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
			forwarded <- data
			<-r.Context().Done()
			return
		}
		for key, values := range out.Header {
			w.Header()[key] = values
		}
		w.WriteHeader(out.StatusCode)
		w.Write(body)
	}))
	defer proxy.Close()
	const origin = "http://127.0.0.1:8899"
	newConsole := func(url string) *consoleapp.Server {
		app, err := consoleapp.New(consoleapp.Config{Origin: origin, Controllers: []string{url}, SharedSessions: true})
		require.NoError(t, err)
		t.Cleanup(app.Close)
		return app
	}
	first := newConsole(proxy.URL)
	second := newConsole("http://" + controller.HTTPAddr())
	var cookie *http.Cookie
	csrf := ""
	call := func(ctx context.Context, app *consoleapp.Server, method, path string, body any) *httptest.ResponseRecorder {
		data, _ := json.Marshal(body)
		r := httptest.NewRequest(method, origin+"/api/console/v1/"+path, bytes.NewReader(data)).WithContext(ctx)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Origin", origin)
		r.Header.Set("X-Djob-CSRF", csrf)
		if cookie != nil {
			r.AddCookie(cookie)
		}
		w := httptest.NewRecorder()
		app.ServeHTTP(w, r)
		return w
	}
	login := call(context.Background(), first, "POST", "session", map[string]string{"token": genericToken("alpha/writer")})
	require.Equal(t, 200, login.Code, login.Body.String())
	cookie = login.Result().Cookies()[0]
	var session struct {
		CSRF string `json:"csrf"`
	}
	require.NoError(t, json.Unmarshal(login.Body.Bytes(), &session))
	csrf = session.CSRF
	prepared := call(context.Background(), first, "POST", "mutations", map[string]any{"tenant_id": "alpha", "launch": map[string]any{"definition_id": "compute", "definition_revision": "1", "run_id": "interrupted"}})
	require.Equal(t, 200, prepared.Code, prepared.Body.String())
	var pending struct {
		RequestID string          `json:"request_id"`
		Request   json.RawMessage `json:"request"`
	}
	require.NoError(t, json.Unmarshal(prepared.Body.Bytes(), &pending))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan struct{})
	go func() {
		call(ctx, first, "POST", "mutations/commit", map[string]string{"request_id": pending.RequestID})
		close(finished)
	}()
	select {
	case actual := <-forwarded:
		require.JSONEq(t, string(pending.Request), string(actual))
	case <-time.After(5 * time.Second):
		t.Fatal("submission did not reach controller")
	}
	cancel()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled console request did not finish")
	}
	first.Close()
	retained := call(context.Background(), second, "GET", "session", nil)
	require.Equal(t, 200, retained.Code, retained.Body.String())
	require.Contains(t, retained.Body.String(), `"state":"submitting"`)
	require.Equal(t, 409, call(context.Background(), second, "POST", "mutations/commit", map[string]string{"request_id": pending.RequestID}).Code)
	id, _, err := consolesession.ParseCredential(cookie.Value)
	require.NoError(t, err)
	record, err := controller.application.Store.GetConsoleSession(id)
	require.NoError(t, err)
	decrypted, err := consolesession.OpenReview(cookie.Value, record.Binding.ClusterID, record.Version, record.Ciphertext)
	require.NoError(t, err)
	var review struct{ AttemptUntil time.Time }
	require.NoError(t, json.Unmarshal(decrypted, &review))
	time.Sleep(max(time.Duration(0), time.Until(review.AttemptUntil)+20*time.Millisecond))
	retained = call(context.Background(), second, "GET", "session", nil)
	require.Equal(t, 200, retained.Code, retained.Body.String())
	require.Contains(t, retained.Body.String(), `"state":"uncertain"`)
	confirmed := call(context.Background(), second, "POST", "mutations/commit", map[string]string{"request_id": pending.RequestID})
	require.Equal(t, 200, confirmed.Code, confirmed.Body.String())
	require.Contains(t, confirmed.Body.String(), `"state":"confirmed"`)
}
