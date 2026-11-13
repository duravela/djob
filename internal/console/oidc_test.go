package console

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/duravela/djob/internal/oidctest"
	"github.com/stretchr/testify/require"
)

func TestOIDCLoginUsesPKCENonceAndEncryptedStateAcrossConsoleReplicas(t *testing.T) {
	issuer := oidctest.New(t)
	controller := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/api/jobs/v1/identity", r.URL.Path)
		require.True(t, strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ey"))
		writeJSON(w, 200, Identity{Subject: "company/alice", Role: "writer", TenantID: "alpha", ExpiresAt: time.Now().Add(30 * time.Minute)})
	}))
	defer controller.Close()
	config := Config{Origin: testOrigin, Controllers: []string{controller.URL}, OIDC: &OIDCConfig{Issuer: issuer.Server.URL, ClientID: oidctest.ClientID, ClientSecret: oidctest.Secret, HTTPClient: issuer.Server.Client()}}
	first, err := New(config)
	require.NoError(t, err)
	defer first.Close()
	second, err := New(config)
	require.NoError(t, err)
	defer second.Close()
	start := httptest.NewRecorder()
	first.ServeHTTP(start, httptest.NewRequest(http.MethodGet, testOrigin+api+"oidc/start", nil))
	require.Equal(t, 303, start.Code)
	authURL, err := url.Parse(start.Header().Get("Location"))
	require.NoError(t, err)
	require.Equal(t, "S256", authURL.Query().Get("code_challenge_method"))
	require.NotEmpty(t, authURL.Query().Get("nonce"))
	require.NotEmpty(t, authURL.Query().Get("state"))
	cookie := start.Result().Cookies()[0]
	require.True(t, cookie.HttpOnly)
	require.Equal(t, http.SameSiteLaxMode, cookie.SameSite)
	client := issuer.Server.Client()
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	approved, err := client.Get(authURL.String())
	require.NoError(t, err)
	approved.Body.Close()
	require.Equal(t, 303, approved.StatusCode)
	callbackURL := approved.Header.Get("Location")
	tampered, err := url.Parse(callbackURL)
	require.NoError(t, err)
	values := tampered.Query()
	values.Set("state", "wrong")
	tampered.RawQuery = values.Encode()
	wrong := httptest.NewRequest(http.MethodGet, tampered.String(), nil)
	wrong.AddCookie(cookie)
	denied := httptest.NewRecorder()
	second.ServeHTTP(denied, wrong)
	require.Equal(t, 401, denied.Code)
	callback := httptest.NewRequest(http.MethodGet, callbackURL, nil)
	callback.AddCookie(cookie)
	result := httptest.NewRecorder()
	second.ServeHTTP(result, callback)
	require.Equal(t, 303, result.Code, result.Body.String())
	require.Equal(t, "/", result.Header().Get("Location"))
	var sessionCookie *http.Cookie
	for _, c := range result.Result().Cookies() {
		if c.Name == second.cookieName {
			sessionCookie = c
		}
	}
	require.NotNil(t, sessionCookie)
	infoReq := httptest.NewRequest(http.MethodGet, testOrigin+api+"session", nil)
	infoReq.AddCookie(sessionCookie)
	info := httptest.NewRecorder()
	second.ServeHTTP(info, infoReq)
	require.Equal(t, 200, info.Code, info.Body.String())
	require.Contains(t, info.Body.String(), "company/alice")
	replay := httptest.NewRecorder()
	second.ServeHTTP(replay, callback)
	require.Equal(t, 401, replay.Code)
}
