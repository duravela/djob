package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	consoleapp "github.com/duravela/djob/internal/console"
	"github.com/duravela/djob/pkg/consolesession"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/duravela/djob/internal/oidctest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

func TestOIDCAuthenticationVerifiesCryptographyAudienceExpiryAndRoleBinding(t *testing.T) {
	issuer := oidctest.New(t)
	auth, err := newAPIAuthorizer(&APIAuthConfig{OIDC: &APIOIDCConfig{Issuer: issuer.Server.URL, ClientID: oidctest.ClientID, HTTPClient: issuer.Server.Client(), Bindings: map[string]APICredential{"alice": {Subject: "company/alice", Role: APIWriter, TenantID: "alpha"}}}})
	require.NoError(t, err)
	good := issuer.Token("alice", oidctest.ClientID, issuer.Server.URL, "", time.Now().Add(time.Hour))
	principal, err := auth.authenticate(metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+good)))
	require.NoError(t, err)
	require.Equal(t, "company/alice", principal.subject)
	require.Equal(t, "alpha", principal.tenantID)
	require.Equal(t, APIWriter, principal.role)
	require.Equal(t, sha256.Sum256([]byte(good)), principal.hash)
	for _, token := range []string{issuer.Token("mallory", oidctest.ClientID, issuer.Server.URL, "", time.Now().Add(time.Hour)), issuer.Token("alice", "wrong", issuer.Server.URL, "", time.Now().Add(time.Hour)), issuer.Token("alice", oidctest.ClientID, "https://wrong.example", "", time.Now().Add(time.Hour)), issuer.Token("alice", oidctest.ClientID, issuer.Server.URL, "", time.Now().Add(-time.Hour)), good[:len(good)-10] + "corrupted"} {
		_, err := auth.authenticate(metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token)))
		require.Error(t, err)
	}
}

func TestSharedConsoleOIDCSessionsKeepVerifiedBindingAcrossReplicas(t *testing.T) {
	issuer := oidctest.New(t)
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	cfg.ConsoleSessions = true
	cfg.APIAuth.OIDC = &APIOIDCConfig{Issuer: issuer.Server.URL, ClientID: oidctest.ClientID, HTTPClient: issuer.Server.Client(), Bindings: map[string]APICredential{"alice": {Subject: "company/alice", Role: APIWriter, TenantID: "alpha"}}}
	controller := startAPI(t, cfg)
	activateControllerSessions(t, controller)
	const origin = "http://127.0.0.1:8899"
	first, err := consoleapp.New(consoleapp.Config{Origin: origin, Controllers: []string{"http://" + controller.HTTPAddr()}, SharedSessions: true})
	require.NoError(t, err)
	defer first.Close()
	second, err := consoleapp.New(consoleapp.Config{Origin: origin, Controllers: []string{"http://" + controller.HTTPAddr()}, SharedSessions: true})
	require.NoError(t, err)
	defer second.Close()
	expiry := time.Now().Add(20 * time.Minute).UTC().Truncate(time.Second)
	token := issuer.Token("alice", oidctest.ClientID, issuer.Server.URL, "", expiry)
	body, _ := json.Marshal(map[string]string{"token": token})
	login := httptest.NewRequest(http.MethodPost, origin+"/api/console/v1/session", bytes.NewReader(body))
	login.Header.Set("Origin", origin)
	login.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	first.ServeHTTP(response, login)
	require.Equal(t, 200, response.Code, response.Body.String())
	cookie := response.Result().Cookies()[0]
	id, _, err := consolesession.ParseCredential(cookie.Value)
	require.NoError(t, err)
	record, err := controller.application.Store.GetConsoleSession(id)
	require.NoError(t, err)
	require.Equal(t, "alice", record.Binding.OIDCSubject)
	require.Equal(t, issuer.Server.URL, record.Binding.OIDCIssuer)
	require.Equal(t, expiry, record.ExpiresAt)
	read := func() *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, origin+"/api/console/v1/session", nil)
		r.AddCookie(cookie)
		w := httptest.NewRecorder()
		second.ServeHTTP(w, r)
		return w
	}
	require.Equal(t, 200, read().Code)
	// Removing the issuer subject's configured binding revokes restored sessions.
	delete(controller.jobs.auth.oidcBindings, "alice")
	require.Equal(t, 401, read().Code)
}

func TestSharedOIDCRefreshAcrossReplicasAndBackchannelLogoutSurviveRestart(t *testing.T) {
	issuer := oidctest.New(t)
	issuer.InitialLifetime = 30 * time.Second
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	cfg.ConsoleSessions = true
	cfg.APIAuth.OIDC = &APIOIDCConfig{Issuer: issuer.Server.URL, ClientID: oidctest.ClientID, HTTPClient: issuer.Server.Client(), BackchannelLogout: true, Bindings: map[string]APICredential{"alice": {Subject: "company/alice", Role: APIWriter, TenantID: "alpha"}}}
	controller := startAPI(t, cfg)
	activateControllerSessions(t, controller)
	const origin = "http://127.0.0.1:8899"
	makeConsole := func(s *Server) *consoleapp.Server {
		app, err := consoleapp.New(consoleapp.Config{Origin: origin, Controllers: []string{"http://" + s.HTTPAddr()}, SharedSessions: true, OIDC: &consoleapp.OIDCConfig{Issuer: issuer.Server.URL, ClientID: oidctest.ClientID, ClientSecret: oidctest.Secret, HTTPClient: issuer.Server.Client(), Refresh: true}})
		require.NoError(t, err)
		t.Cleanup(app.Close)
		return app
	}
	first, second := makeConsole(controller), makeConsole(controller)
	start := httptest.NewRecorder()
	first.ServeHTTP(start, httptest.NewRequest("GET", origin+"/api/console/v1/oidc/start", nil))
	client := issuer.Server.Client()
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	response, err := client.Get(start.Header().Get("Location"))
	require.NoError(t, err)
	response.Body.Close()
	callback := httptest.NewRequest("GET", response.Header.Get("Location"), nil)
	callback.AddCookie(start.Result().Cookies()[0])
	logged := httptest.NewRecorder()
	second.ServeHTTP(logged, callback)
	require.Equal(t, 303, logged.Code, logged.Body.String())
	var cookie *http.Cookie
	for _, c := range logged.Result().Cookies() {
		if c.Name == "djob-console" {
			cookie = c
		}
	}
	require.NotNil(t, cookie)
	var csrf string
	call := func(app *consoleapp.Server, path string, body any) *httptest.ResponseRecorder {
		raw, _ := json.Marshal(body)
		method := "POST"
		if path == "session" {
			method = "GET"
		}
		r := httptest.NewRequest(method, origin+"/api/console/v1/"+path, bytes.NewReader(raw))
		r.AddCookie(cookie)
		r.Header.Set("Origin", origin)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-Djob-CSRF", csrf)
		w := httptest.NewRecorder()
		app.ServeHTTP(w, r)
		return w
	}
	info := call(first, "session", nil)
	require.Equal(t, 200, info.Code, info.Body.String())
	var session struct {
		CSRF string `json:"csrf"`
	}
	require.NoError(t, json.Unmarshal(info.Body.Bytes(), &session))
	csrf = session.CSRF
	prepared := call(first, "mutations", map[string]any{"put_definition": map[string]any{"definition": map[string]any{"id": "retained-through-refresh", "runtime": "steps/v1", "handler": "test", "handler_version": "1"}}})
	require.Equal(t, 200, prepared.Code, prepared.Body.String())
	var before map[string]any
	require.NoError(t, json.Unmarshal(prepared.Body.Bytes(), &before))
	id, _, err := consolesession.ParseCredential(cookie.Value)
	require.NoError(t, err)
	old, err := controller.application.Store.GetConsoleSession(id)
	require.NoError(t, err)
	renewed := call(second, "oidc/refresh", map[string]any{})
	require.Equal(t, 200, renewed.Code, renewed.Body.String())
	current, err := controller.application.Store.GetConsoleSession(id)
	require.NoError(t, err)
	require.True(t, current.ExpiresAt.After(old.ExpiresAt))
	require.Equal(t, old.Binding.ID, current.Binding.ID)
	info = call(first, "session", nil)
	require.Equal(t, 200, info.Code, info.Body.String())
	require.Contains(t, info.Body.String(), before["request_id"].(string))
	logout := issuer.Signed(map[string]any{"iss": issuer.Server.URL, "aud": oidctest.ClientID, "sub": "alice", "iat": time.Now().Unix(), "jti": "logout-one", "events": map[string]any{"http://schemas.openid.net/event/backchannel-logout": map[string]any{}}})
	form := url.Values{"logout_token": {logout}}
	request := httptest.NewRequest("POST", origin+"/api/console/v1/oidc/backchannel", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	out := httptest.NewRecorder()
	second.ServeHTTP(out, request)
	require.Equal(t, 200, out.Code, out.Body.String())
	require.Equal(t, 401, call(first, "session", nil).Code)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	require.NoError(t, controller.Close(ctx))
	cancel()
	controller = startAPI(t, cfg)
	third := makeConsole(controller)
	require.Equal(t, 401, call(third, "session", nil).Code)
}

func TestBackchannelLogoutRejectsNonceAndWrongAudienceAndRevokesBearer(t *testing.T) {
	issuer := oidctest.New(t)
	cfg := apiTestConfig(t.TempDir())
	cfg.APIAuth = genericCredentials()
	cfg.APIAuth.OIDC = &APIOIDCConfig{Issuer: issuer.Server.URL, ClientID: oidctest.ClientID, HTTPClient: issuer.Server.Client(), BackchannelLogout: true, Bindings: map[string]APICredential{"alice": {Subject: "company/alice", Role: APIWriter, TenantID: "alpha"}}}
	s := startAPI(t, cfg)
	token := issuer.Token("alice", oidctest.ClientID, issuer.Server.URL, "", time.Now().Add(time.Hour))
	_, err := s.jobs.auth.authenticate(metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token)))
	require.NoError(t, err)
	claims := map[string]any{"iss": issuer.Server.URL, "aud": oidctest.ClientID, "sub": "alice", "iat": time.Now().Unix(), "jti": "logout-security-test", "events": map[string]any{"http://schemas.openid.net/event/backchannel-logout": map[string]any{}}}
	send := func() int {
		response, err := http.PostForm("http://"+s.HTTPAddr()+"/api/jobs/v1/oidc/logout", url.Values{"logout_token": {issuer.Signed(claims)}})
		require.NoError(t, err)
		defer response.Body.Close()
		return response.StatusCode
	}
	claims["nonce"] = nil
	require.Equal(t, 400, send())
	delete(claims, "nonce")
	claims["aud"] = "another-client"
	require.Equal(t, 400, send())
	claims["aud"] = oidctest.ClientID
	require.Equal(t, 200, send())
	_, err = s.jobs.auth.authenticate(metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token)))
	require.Error(t, err)
}
