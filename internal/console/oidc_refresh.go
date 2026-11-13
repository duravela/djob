package console

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/duravela/djob/pkg/consolesession"
	"github.com/duravela/djob/pkg/db"
	"golang.org/x/oauth2"
	"io"
	"net/http"
	"time"
)

type oidcLoginKey struct{}
type oidcRenewKey struct{}
type oidcSession struct {
	Subject, SID, Nonce, RefreshToken string
	AbsoluteExpires, RefreshAt        time.Time
	Refreshing                        bool
}

func refreshAt(sess *session) *time.Time {
	if sess.oidc == nil || sess.oidc.RefreshToken == "" {
		return nil
	}
	at := sess.oidc.RefreshAt
	return &at
}
func (s *Server) oidcRefresh(w http.ResponseWriter, r *http.Request, sess *session) {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	o := sess.oidc
	if s.oidc == nil || !s.oidc.refresh || o == nil || o.RefreshToken == "" {
		problem(w, 400, "SSO refresh is not enabled for this session")
		return
	}
	if o.Refreshing {
		problem(w, 409, "refresh outcome is uncertain; sign in again before expiry and retain your current review")
		return
	}
	if !time.Now().Before(o.AbsoluteExpires) {
		problem(w, 401, "absolute SSO lifetime ended")
		return
	}
	if time.Now().Before(o.RefreshAt) {
		problem(w, 409, "SSO refresh is not due")
		return
	}
	if sess.pending != nil && sess.pending.State == "submitting" {
		problem(w, 409, "wait for the current submission before refresh")
		return
	}
	if sess.shared == nil {
		response, _, err := s.request(r.Context(), sess.credential(), sess.controller, "identity", http.MethodGet, nil, true)
		if err != nil {
			problem(w, 503, "identity verification unavailable before refresh")
			return
		}
		if response.status != 200 {
			problem(w, 401, "SSO session ended before refresh")
			return
		}
	}
	o.Refreshing = true
	if sess.shared != nil {
		if err := s.saveShared(r.Context(), sess); err != nil {
			problem(w, 409, "another replica changed the session; reload before refreshing")
			return
		}
	}
	ctx := oidc.ClientContext(r.Context(), s.oidc.client)
	token, err := s.oidc.config.TokenSource(ctx, &oauth2.Token{RefreshToken: o.RefreshToken, Expiry: time.Unix(1, 0)}).Token()
	if err != nil {
		problem(w, 503, "identity provider refresh was not confirmed; sign in again")
		return
	}
	if len(token.RefreshToken) > 8192 {
		problem(w, 401, "refresh token exceeds the supported bound")
		return
	}
	raw, ok := token.Extra("id_token").(string)
	if !ok || !validToken(raw) {
		problem(w, 401, "refresh did not return an identity token; sign in again")
		return
	}
	verified, err := s.oidc.verifier.Verify(ctx, raw)
	var claims struct {
		SID string `json:"sid"`
	}
	if err != nil || verified.Subject != o.Subject || verified.Nonce != "" && verified.Nonce != o.Nonce || verified.Claims(&claims) != nil || claims.SID != o.SID || verified.AccessTokenHash != "" && verified.VerifyAccessToken(token.AccessToken) != nil {
		problem(w, 401, "refreshed identity changed; sign in again")
		return
	}
	response, index, err := s.request(r.Context(), raw, sess.controller, "identity", http.MethodGet, nil, true)
	var identity Identity
	if err != nil || response.status != 200 || json.Unmarshal(response.body, &identity) != nil || identity.Subject != sess.identity.Subject || identity.Role != sess.identity.Role || identity.TenantID != sess.identity.TenantID {
		problem(w, 401, "refreshed identity no longer has the same permissions")
		return
	}
	expires := identityExpiry(identity, o.AbsoluteExpires)
	if !expires.After(time.Now()) {
		problem(w, 401, "refreshed identity expired")
		return
	}
	o.RefreshToken = token.RefreshToken
	o.RefreshAt = verified.Expiry.Add(-time.Minute)
	o.Refreshing = false
	if sess.shared != nil {
		data, err := reviewBytes(sess)
		if err != nil {
			problem(w, 500, "cannot retain refreshed review")
			return
		}
		encrypted, err := consolesession.SealContinuityReview(sess.token, sess.shared.cluster, sess.shared.version+1, data)
		if err != nil {
			problem(w, 500, "cannot protect refreshed review")
			return
		}
		id, _, _ := consolesession.ParseCredential(sess.token)
		var mutation [32]byte
		rand.Read(mutation[:])
		command := db.ConsoleSessionCommand{ID: id, ExpectedVersion: sess.shared.version, Mutation: mutation, Ciphertext: encrypted, Renewal: &consolesession.Renewal{ExpiresAt: expires}}
		response, index, err = s.sharedCall(context.WithValue(r.Context(), oidcRenewKey{}, raw), sess.token, index, "renew", command)
		var result db.ConsoleSessionResult
		if err != nil || response.status != 200 || json.Unmarshal(response.body, &result) != nil || result.Record == nil || result.Record.Version != sess.shared.version+1 {
			o.Refreshing = true
			problem(w, 503, "shared renewal was not confirmed; reload the retained review")
			return
		}
		sess.shared.version = result.Record.Version
	} else {
		sess.renewedToken.Store(&raw)
		cookie, _ := r.Cookie(s.cookieName)
		s.mu.Lock()
		if sess.expiry != nil {
			sess.expiry.Stop()
		}
		sess.expiry = time.AfterFunc(time.Until(expires), func() { s.mu.Lock(); defer s.mu.Unlock(); s.removeSessionLocked(cookie.Value) })
		s.mu.Unlock()
	}
	sess.identity.ExpiresAt = identity.ExpiresAt
	if sess.shared == nil {
		s.mu.Lock()
		sess.expires = expires
		s.mu.Unlock()
	} else {
		sess.expires = expires
	}
	sess.controller = index
	http.SetCookie(w, s.cookie(sessionCookieValue(r, s.cookieName), expires, int(time.Until(expires).Seconds())))
	// sessionInfo locks the same mutex.
	sess.mu.Unlock()
	s.sessionInfo(w, r, sess)
	sess.mu.Lock()
}
func sessionCookieValue(r *http.Request, name string) string {
	c, err := r.Cookie(name)
	if err != nil {
		return ""
	}
	return c.Value
}
func (s *Server) oidcBackchannel(w http.ResponseWriter, r *http.Request) {
	if s.oidc == nil {
		problem(w, 404, "SSO is not configured")
		return
	}
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 16384))
	if err != nil {
		problem(w, 400, "logout request too large")
		return
	}
	response, _, err := s.request(r.Context(), "", 0, "oidc/logout", http.MethodPost, raw, true)
	if err != nil {
		problem(w, 503, "controller logout confirmation unavailable")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(response.status)
	w.Write(response.body)
}

func (sess *session) credential() string {
	if token := sess.renewedToken.Load(); token != nil {
		return *token
	}
	return sess.token
}
