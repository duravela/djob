package console

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConsoleIdleSessionExpiresWithoutAnotherRequest(t *testing.T) {
	b := newBrowserAs(t, Identity{Subject: "platform", Role: "operator"}, func(http.ResponseWriter, *http.Request) {
		t.Error("preparing an enrollment must not submit it")
	}, func(c *Config) { c.MaxSessions = 1 })
	// Keep the public minimum lifetime unchanged while exercising a real timer.
	b.server.ttl = 500 * time.Millisecond
	login := b.call(http.MethodPost, "session", map[string]string{"token": testToken})
	require.Equal(t, 200, login.Code, login.Body.String())
	b.cookie = login.Result().Cookies()[0]
	var info struct {
		CSRF string `json:"csrf"`
	}
	require.NoError(t, json.Unmarshal(login.Body.Bytes(), &info))
	b.csrf = info.CSRF
	prepared := b.call(http.MethodPost, "mutations/worker-enrollment", map[string]string{
		"tenant_id": "alpha", "worker_id": "idle-worker", "action": "enroll", "subject": "company/idle-worker", "reason": "prepare company worker",
	})
	require.Equal(t, 200, prepared.Code, prepared.Body.String())
	b.server.mu.Lock()
	sess := b.server.sessions[b.cookie.Value]
	b.server.mu.Unlock()
	sess.mu.Lock()
	available := sess.pending != nil && len(sess.pending.WorkerConfig) != 0
	sess.mu.Unlock()
	require.True(t, available, "the session must retain a private configuration before expiry")
	sess = nil
	// Do not make HTTP requests or trigger capacity cleanup until the timer has
	// independently removed the session and its retained configuration.
	require.Eventually(t, func() bool {
		b.server.mu.Lock()
		defer b.server.mu.Unlock()
		return len(b.server.sessions) == 0
	}, 3*time.Second, 10*time.Millisecond)
	require.Equal(t, 401, b.call(http.MethodGet, "session", nil).Code)
	require.Equal(t, 401, b.call(http.MethodPost, "mutations/worker-config", map[string]string{"request_id": "expired"}).Code)
	require.Equal(t, 200, b.call(http.MethodPost, "session", map[string]string{"token": testToken}).Code)
}

func TestConsoleClosePreventsInflightLoginFromRestoringSessions(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var unblock sync.Once
	resume := func() { unblock.Do(func() { close(release) }) }
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/jobs/v1/identity", r.URL.Path)
		close(started)
		<-release
		writeJSON(w, 200, Identity{Subject: "platform", Role: "operator"})
	}))
	t.Cleanup(controller.Close)
	t.Cleanup(resume)
	s, err := New(Config{Origin: testOrigin, Controllers: []string{controller.URL}})
	require.NoError(t, err)
	t.Cleanup(s.Close)
	b := &browser{t: t, server: s}
	result := make(chan *httptest.ResponseRecorder, 1)
	go func() { result <- b.call(http.MethodPost, "session", map[string]string{"token": testToken}) }()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("login did not reach the controller")
	}
	s.Close()
	resume()
	select {
	case reply := <-result:
		require.Equal(t, 503, reply.Code, reply.Body.String())
		require.Empty(t, reply.Result().Cookies(), "a closed console must not issue a new session")
	case <-time.After(3 * time.Second):
		t.Fatal("login did not finish after console shutdown")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	require.Empty(t, s.sessions, "a completed controller response must not recreate closed session state")
}
