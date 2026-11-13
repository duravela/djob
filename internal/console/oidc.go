package console

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/duravela/djob/internal/oidcauth"
	"golang.org/x/oauth2"
)

type OIDCConfig struct {
	Refresh      bool         `json:"refresh"`
	Issuer       string       `json:"issuer"`
	ClientID     string       `json:"client_id"`
	ClientSecret string       `json:"client_secret"`
	HTTPClient   *http.Client `json:"-"`
}
type oidcLogin struct {
	refresh  bool
	config   oauth2.Config
	verifier *oidc.IDTokenVerifier
	cipher   cipher.AEAD
	client   *http.Client
	aad      []byte
}
type oidcState struct {
	State, Nonce, Verifier string
	Expires                time.Time
}

func LoadOIDCConfig(path string) (*OIDCConfig, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("cannot open OIDC configuration")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("OIDC configuration requires an owner-only regular file")
	}
	data, err := io.ReadAll(io.LimitReader(f, 16385))
	if err != nil || len(data) > 16384 {
		return nil, fmt.Errorf("OIDC configuration exceeds 16 KiB")
	}
	var config OIDCConfig
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if d.Decode(&config) != nil || d.Decode(new(any)) != io.EOF {
		return nil, fmt.Errorf("invalid OIDC configuration")
	}
	return &config, nil
}
func (s *Server) configureOIDC(c *OIDCConfig) error {
	if c == nil {
		return nil
	}
	if len(c.ClientSecret) < 16 {
		return fmt.Errorf("OIDC requires a confidential client secret of at least 16 bytes")
	}
	client := &http.Client{Timeout: 10 * time.Second}
	if c.HTTPClient != nil {
		*client = *c.HTTPClient
	}
	client.Timeout = 10 * time.Second
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	provider, err := oidcauth.Provider(c.Issuer, c.ClientID, client)
	if err != nil {
		return err
	}
	key := sha256.Sum256([]byte("djob/oidc/login-state/v1\x00" + c.ClientSecret))
	block, _ := aes.NewCipher(key[:])
	aead, _ := cipher.NewGCM(block)
	s.oidc = &oidcLogin{refresh: c.Refresh, config: oauth2.Config{ClientID: c.ClientID, ClientSecret: c.ClientSecret, Endpoint: provider.Endpoint(), RedirectURL: s.origin.String() + api + "oidc/callback", Scopes: []string{oidc.ScopeOpenID}}, verifier: provider.Verifier(&oidc.Config{ClientID: c.ClientID}), cipher: aead, client: client, aad: []byte(s.origin.String() + "\x00" + c.Issuer + "\x00" + c.ClientID)}
	if c.Refresh {
		s.oidc.config.Scopes = append(s.oidc.config.Scopes, "offline_access")
	}
	return nil
}
func (s *Server) oidcCookie(value string, age int) *http.Cookie {
	name := "djob-oidc-state"
	if s.origin.Scheme == "https" {
		name = "__Host-" + name
	}
	return &http.Cookie{Name: name, Value: value, Path: "/", Secure: s.origin.Scheme == "https", HttpOnly: true, SameSite: http.SameSiteLaxMode, MaxAge: age}
}
func (s *Server) oidcStart(w http.ResponseWriter, r *http.Request) {
	if s.oidc == nil {
		problem(w, 404, "SSO is not configured")
		return
	}
	state, _ := randomID()
	nonce, _ := randomID()
	if state == "" || nonce == "" {
		problem(w, 500, "cannot create login state")
		return
	}
	proof := oidcState{State: state, Nonce: nonce, Verifier: oauth2.GenerateVerifier(), Expires: time.Now().Add(5 * time.Minute)}
	data, _ := json.Marshal(proof)
	iv := make([]byte, s.oidc.cipher.NonceSize())
	if _, err := rand.Read(iv); err != nil {
		problem(w, 500, "cannot create login state")
		return
	}
	sealed := s.oidc.cipher.Seal(iv, iv, data, s.oidc.aad)
	http.SetCookie(w, s.oidcCookie(base64.RawURLEncoding.EncodeToString(sealed), 300))
	http.Redirect(w, r, s.oidc.config.AuthCodeURL(state, oauth2.S256ChallengeOption(proof.Verifier), oidc.Nonce(nonce)), http.StatusSeeOther)
}

type oidcResponse struct {
	header http.Header
	code   int
	body   bytes.Buffer
}

func (w *oidcResponse) Header() http.Header  { return w.header }
func (w *oidcResponse) WriteHeader(code int) { w.code = code }
func (w *oidcResponse) Write(data []byte) (int, error) {
	if w.code == 0 {
		w.code = 200
	}
	return w.body.Write(data)
}
func (s *Server) oidcCallback(w http.ResponseWriter, r *http.Request) {
	if s.oidc == nil {
		problem(w, 404, "SSO is not configured")
		return
	}
	cookie, err := r.Cookie(s.oidcCookie("", 0).Name)
	http.SetCookie(w, s.oidcCookie("", -1))
	fail := func() { problem(w, 401, "SSO login could not be verified; start a new login") }
	if err != nil || len(cookie.Value) > 4096 || r.URL.Query().Get("error") != "" {
		fail()
		return
	}
	raw, err := base64.RawURLEncoding.DecodeString(cookie.Value)
	if err != nil || len(raw) < s.oidc.cipher.NonceSize() {
		fail()
		return
	}
	iv := raw[:s.oidc.cipher.NonceSize()]
	data, err := s.oidc.cipher.Open(nil, iv, raw[s.oidc.cipher.NonceSize():], s.oidc.aad)
	var proof oidcState
	if err != nil || json.Unmarshal(data, &proof) != nil || !time.Now().Before(proof.Expires) || subtle.ConstantTimeCompare([]byte(proof.State), []byte(r.URL.Query().Get("state"))) != 1 || len(r.URL.Query()["state"]) != 1 || len(r.URL.Query()["code"]) != 1 {
		fail()
		return
	}
	ctx := r.Context()
	if s.oidc.client != nil {
		ctx = oidc.ClientContext(ctx, s.oidc.client)
	}
	token, err := s.oidc.config.Exchange(ctx, r.URL.Query().Get("code"), oauth2.VerifierOption(proof.Verifier))
	if err != nil {
		fail()
		return
	}
	rawID, ok := token.Extra("id_token").(string)
	if !ok || !validToken(rawID) {
		fail()
		return
	}
	verified, err := s.oidc.verifier.Verify(ctx, rawID)
	if err != nil || subtle.ConstantTimeCompare([]byte(proof.Nonce), []byte(verified.Nonce)) != 1 {
		fail()
		return
	}
	if verified.AccessTokenHash != "" && verified.VerifyAccessToken(token.AccessToken) != nil {
		fail()
		return
	}
	payload, _ := json.Marshal(map[string]string{"token": rawID})
	var claims struct {
		SID string `json:"sid"`
	}
	if verified.Claims(&claims) != nil {
		fail()
		return
	}
	continuation := &oidcSession{Subject: verified.Subject, SID: claims.SID, Nonce: verified.Nonce, AbsoluteExpires: time.Now().Add(s.ttl)}
	if s.oidc.refresh {
		if len(token.RefreshToken) > 8192 {
			fail()
			return
		}
		continuation.RefreshToken = token.RefreshToken
		continuation.RefreshAt = verified.Expiry.Add(-time.Minute)
	}
	ctx = context.WithValue(ctx, oidcLoginKey{}, continuation)
	login := r.Clone(ctx)
	login.Method = http.MethodPost
	login.Header = r.Header.Clone()
	login.Header.Set("Content-Type", "application/json")
	login.Body = io.NopCloser(strings.NewReader(string(payload)))
	login.ContentLength = int64(len(payload))
	capture := &oidcResponse{header: w.Header()}
	s.login(capture, login)
	if capture.code != http.StatusOK {
		w.WriteHeader(capture.code)
		w.Write(capture.body.Bytes())
		return
	}
	w.Header().Del("Content-Type")
	http.Redirect(w, r, "/", http.StatusSeeOther)
}
