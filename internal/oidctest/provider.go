// Package oidctest supplies an isolated signing issuer for integration tests.
package oidctest

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

type Provider struct {
	InitialLifetime time.Duration
	Server          *httptest.Server
	key             *rsa.PrivateKey
	mu              sync.Mutex
	codes           map[string]url.Values
	refreshes       map[string]url.Values
	Subject         string
}

const ClientID = "console"
const Secret = "test-confidential-client-secret-0123456789"

func New(t *testing.T) *Provider {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	p := &Provider{key: key, codes: map[string]url.Values{}, refreshes: map[string]url.Values{}, Subject: "alice"}
	p.Server = httptest.NewTLSServer(http.HandlerFunc(p.serve))
	t.Cleanup(p.Server.Close)
	return p
}
func (p *Provider) Signed(claims map[string]any) string {
	signer, _ := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: p.key}, (&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", "test"))
	raw, _ := jwt.Signed(signer).Claims(claims).Serialize()
	return raw
}
func (p *Provider) Token(subject, audience, issuer, nonce string, expires time.Time) string {
	signer, _ := jose.NewSigner(jose.SigningKey{Algorithm: jose.RS256, Key: p.key}, (&jose.SignerOptions{}).WithType("JWT").WithHeader("kid", "test"))
	raw, _ := jwt.Signed(signer).Claims(map[string]any{"iss": issuer, "sub": subject, "aud": audience, "iat": time.Now().Unix(), "exp": expires.Unix(), "nonce": nonce}).Serialize()
	return raw
}
func (p *Provider) serve(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/.well-known/openid-configuration":
		json.NewEncoder(w).Encode(map[string]any{"issuer": p.Server.URL, "authorization_endpoint": p.Server.URL + "/authorize", "token_endpoint": p.Server.URL + "/token", "jwks_uri": p.Server.URL + "/keys", "id_token_signing_alg_values_supported": []string{"RS256"}})
	case "/keys":
		json.NewEncoder(w).Encode(jose.JSONWebKeySet{Keys: []jose.JSONWebKey{{Key: &p.key.PublicKey, KeyID: "test", Algorithm: "RS256", Use: "sig"}}})
	case "/authorize":
		q := r.URL.Query()
		if q.Get("client_id") != ClientID || q.Get("response_type") != "code" || q.Get("code_challenge_method") != "S256" || q.Get("nonce") == "" {
			http.Error(w, "invalid authorization", 400)
			return
		}
		b := make([]byte, 32)
		rand.Read(b)
		code := base64.RawURLEncoding.EncodeToString(b)
		p.mu.Lock()
		p.codes[code] = q
		p.mu.Unlock()
		redirect, _ := url.Parse(q.Get("redirect_uri"))
		values := redirect.Query()
		values.Set("code", code)
		values.Set("state", q.Get("state"))
		redirect.RawQuery = values.Encode()
		http.Redirect(w, r, redirect.String(), http.StatusSeeOther)
	case "/token":
		r.ParseForm()
		p.mu.Lock()
		q, ok := p.codes[r.Form.Get("code")]
		refreshing := r.Form.Get("grant_type") == "refresh_token"
		if refreshing {
			q, ok = p.refreshes[r.Form.Get("refresh_token")]
			delete(p.refreshes, r.Form.Get("refresh_token"))
		} else {
			delete(p.codes, r.Form.Get("code"))
		}
		p.mu.Unlock()
		id, secret, valid := r.BasicAuth()
		hash := sha256.Sum256([]byte(r.Form.Get("code_verifier")))
		if !ok || !valid || id != ClientID || secret != Secret || !refreshing && (base64.RawURLEncoding.EncodeToString(hash[:]) != q.Get("code_challenge") || r.Form.Get("redirect_uri") != q.Get("redirect_uri")) {
			http.Error(w, "invalid exchange", 400)
			return
		}
		b := make([]byte, 32)
		rand.Read(b)
		refresh := base64.RawURLEncoding.EncodeToString(b)
		p.mu.Lock()
		p.refreshes[refresh] = q
		p.mu.Unlock()
		lifetime := time.Hour
		if !refreshing && p.InitialLifetime > 0 {
			lifetime = p.InitialLifetime
		}
		json.NewEncoder(w).Encode(map[string]any{"refresh_token": refresh, "access_token": "isolated-access-token", "token_type": "Bearer", "expires_in": 3600, "id_token": p.Token(p.Subject, ClientID, p.Server.URL, q.Get("nonce"), time.Now().Add(lifetime))})
	default:
		http.NotFound(w, r)
	}
}
