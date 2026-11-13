package console

import (
	"encoding/json"
	"github.com/duravela/djob/internal/oidctest"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestOIDCRefreshRotatesTokenAndPreservesReviewedRequest(t *testing.T) {
	issuer := oidctest.New(t)
	role := "writer"
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/jobs/v1/identity", r.URL.Path)
		writeJSON(w, 200, Identity{Subject: "company/alice", Role: role, TenantID: "alpha", ExpiresAt: time.Now().Add(time.Hour)})
	}))
	defer controller.Close()
	app, err := New(Config{Origin: testOrigin, Controllers: []string{controller.URL}, OIDC: &OIDCConfig{Issuer: issuer.Server.URL, ClientID: oidctest.ClientID, ClientSecret: oidctest.Secret, HTTPClient: issuer.Server.Client(), Refresh: true}})
	require.NoError(t, err)
	defer app.Close()
	start := httptest.NewRecorder()
	app.ServeHTTP(start, httptest.NewRequest("GET", testOrigin+api+"oidc/start", nil))
	client := issuer.Server.Client()
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := client.Get(start.Header().Get("Location"))
	require.NoError(t, err)
	response.Body.Close()
	callback := httptest.NewRequest("GET", response.Header.Get("Location"), nil)
	callback.AddCookie(start.Result().Cookies()[0])
	logged := httptest.NewRecorder()
	app.ServeHTTP(logged, callback)
	require.Equal(t, 303, logged.Code, logged.Body.String())
	var cookie *http.Cookie
	for _, c := range logged.Result().Cookies() {
		if c.Name == app.cookieName {
			cookie = c
		}
	}
	require.NotNil(t, cookie)
	app.mu.Lock()
	sess := app.sessions[cookie.Value]
	app.mu.Unlock()
	require.NotNil(t, sess)
	sess.mu.Lock()
	sess.oidc.RefreshAt = time.Now().Add(-time.Second)
	oldRefresh := sess.oidc.RefreshToken
	sess.pending = &pendingMutation{RequestID: "original-request", State: "uncertain", Request: json.RawMessage(`{"command":{"request_id":"original-request"}}`)}
	csrf := sess.csrf
	sess.mu.Unlock()
	request := httptest.NewRequest("POST", testOrigin+api+"oidc/refresh", strings.NewReader("{}"))
	request.Header.Set("Origin", testOrigin)
	request.Header.Set("X-Djob-CSRF", csrf)
	request.AddCookie(cookie)
	refreshed := httptest.NewRecorder()
	app.ServeHTTP(refreshed, request)
	require.Equal(t, 200, refreshed.Code, refreshed.Body.String())
	require.NotContains(t, refreshed.Body.String(), oldRefresh)
	sess.mu.Lock()
	require.NotEqual(t, oldRefresh, sess.oidc.RefreshToken)
	require.False(t, sess.oidc.Refreshing)
	require.Equal(t, "original-request", sess.pending.RequestID)
	require.Equal(t, "uncertain", sess.pending.State)
	require.True(t, sess.expires.Before(sess.oidc.AbsoluteExpires))
	sess.mu.Unlock()
	again := httptest.NewRecorder()
	app.ServeHTTP(again, request)
	require.Equal(t, 409, again.Code)
	role = "reader"
	infoRequest := httptest.NewRequest("GET", testOrigin+api+"session", nil)
	infoRequest.AddCookie(cookie)
	info := httptest.NewRecorder()
	app.ServeHTTP(info, infoRequest)
	require.Equal(t, 401, info.Code)
}
