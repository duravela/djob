package console

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/duravela/djob/pkg/consolesession"
	"github.com/duravela/djob/pkg/db"
)

type sharedSession struct {
	version, cluster uint64
	attemptUntil     time.Time
}

// Byte slices deliberately encode as base64 so exact request bytes survive JSON
// serialization, including whitespace and large integer representations.
type sharedReview struct {
	Replacement   *replacementPlan `json:",omitempty"`
	ReleasePlan   *releasePlan     `json:",omitempty"`
	OIDC          *oidcSession     `json:",omitempty"`
	Draft         []byte
	DraftRevision uint64
	CSRF          string
	Pending       *sharedPending
	AttemptUntil  time.Time
}
type sharedPending struct {
	DraftRevision                   uint64
	Endpoint                        string
	WorkerConfig, Request, Response []byte
	RequestID, State, Error         string
	HTTPCode                        int
	WorkerConfigAvailable           bool
}

func reviewBytes(sess *session) ([]byte, error) {
	v := sharedReview{Replacement: sess.replacement, ReleasePlan: sess.releasePlan, OIDC: sess.oidc, Draft: sess.draft, DraftRevision: sess.draftRevision, CSRF: sess.csrf, AttemptUntil: sess.shared.attemptUntil}
	if p := sess.pending; p != nil {
		v.Pending = &sharedPending{DraftRevision: p.DraftRevision, Endpoint: p.Endpoint, WorkerConfig: p.WorkerConfig, Request: p.Request, Response: p.Response, RequestID: p.RequestID, State: p.State, Error: p.Error, HTTPCode: p.HTTPCode, WorkerConfigAvailable: p.WorkerConfigAvailable}
	}
	return json.Marshal(v)
}

func (s *Server) sharedCall(ctx context.Context, token string, start int, action string, c any) (reply, int, error) {
	ctx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	data, err := json.Marshal(c)
	if err != nil {
		return reply{}, start, err
	}
	return s.request(ctx, token, start, "console-sessions/"+action, http.MethodPost, data, true)
}

func (s *Server) saveShared(ctx context.Context, sess *session) error {
	data, err := reviewBytes(sess)
	if err != nil {
		return err
	}
	seal := consolesession.SealReview
	if sess.draftRevision > 0 {
		seal = consolesession.SealEditorReview
	}
	if sess.oidc != nil || sess.releasePlan != nil {
		seal = consolesession.SealContinuityReview
	}
	encrypted, err := seal(sess.token, sess.shared.cluster, sess.shared.version+1, data)
	if err != nil {
		return err
	}
	id, _, err := consolesession.ParseCredential(sess.token)
	if err != nil {
		return err
	}
	var mutation [32]byte
	if _, err := rand.Read(mutation[:]); err != nil {
		return err
	}
	c := db.ConsoleSessionCommand{ID: id, ExpectedVersion: sess.shared.version, Mutation: mutation, Ciphertext: encrypted}
	response, index, err := s.sharedCall(ctx, sess.token, sess.controller, "replace", c)
	sess.controller = index
	if err != nil {
		return err
	}
	var result db.ConsoleSessionResult
	if response.status != 200 || json.Unmarshal(response.body, &result) != nil || result.Record == nil || result.Record.Version != sess.shared.version+1 || !bytes.Equal(result.Record.Ciphertext, encrypted) {
		return fmt.Errorf("shared session update was not confirmed")
	}
	sess.shared.version = result.Record.Version
	return nil
}

func (s *Server) sharedLogin(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Token string `json:"token"`
	}
	if decode(w, r, 12288, &input) != nil || !validToken(input.Token) {
		problem(w, 400, "provide a valid djob API credential")
		return
	}
	identityContext, cancelIdentity := context.WithTimeout(r.Context(), 6*time.Second)
	response, index, err := s.request(identityContext, input.Token, 0, "identity", http.MethodGet, nil, true)
	cancelIdentity()
	var identity Identity
	if err != nil {
		problem(w, 502, "controller identity service unavailable")
		return
	}
	if response.status != 200 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(response.status)
		_, _ = w.Write(response.body)
		return
	}
	if json.Unmarshal(response.body, &identity) != nil || identity.Subject == "" || (identity.Role != "reader" && identity.Role != "writer" && identity.Role != "operator") || identity.TenantID == "" && identity.Role != "operator" {
		problem(w, 502, "controller returned an invalid console identity")
		return
	}
	response, index, err = s.sharedCall(r.Context(), input.Token, index, "policy", db.ConsoleSessionCommand{})
	var policy db.ConsoleSessionPolicy
	if err != nil || response.status != 200 || json.Unmarshal(response.body, &policy) != nil {
		problem(w, 503, "shared console session storage is unavailable or not activated")
		return
	}
	token, err := consolesession.NewCredential()
	if err != nil {
		problem(w, 500, "cannot create session")
		return
	}
	id, _, _ := consolesession.ParseCredential(token)
	csrf := sharedCSRF(token)

	ttl := min(s.ttl, policy.Limits.MaxLifetime)
	now := time.Now().UTC()
	sess := &session{token: token, csrf: csrf, identity: identity, expires: identityExpiry(identity, now.Add(ttl)), controller: index, shared: &sharedSession{cluster: policy.ClusterID, version: 1}}
	sess.oidc, _ = r.Context().Value(oidcLoginKey{}).(*oidcSession)
	if sess.oidc != nil {
		sess.oidc.AbsoluteExpires = now.Add(ttl)
	}
	data, err := reviewBytes(sess)
	if err != nil {
		problem(w, 500, "cannot encode session")
		return
	}
	seal := consolesession.SealReview
	if sess.oidc != nil || sess.releasePlan != nil {
		seal = consolesession.SealContinuityReview
	}
	encrypted, err := seal(token, policy.ClusterID, 1, data)
	if err != nil {
		problem(w, 500, "cannot encrypt session")
		return
	}
	creation := consolesession.Creation{Binding: consolesession.Binding{ID: id, CredentialHash: sha256.Sum256([]byte(token)), ClusterID: policy.ClusterID}, CreatedAt: now, ExpiresAt: sess.expires, Ciphertext: encrypted}
	response, index, err = s.sharedCall(r.Context(), input.Token, index, "create", db.ConsoleSessionCommand{Creation: &creation})
	var result db.ConsoleSessionResult
	if err != nil || response.status != 200 || json.Unmarshal(response.body, &result) != nil || result.Record == nil || result.Record.Binding.ID != id || !bytes.Equal(result.Record.Ciphertext, encrypted) {
		problem(w, 503, "shared session creation could not be confirmed")
		return
	}
	sess.controller = index
	if old, e := r.Cookie(s.cookieName); e == nil {
		if oldID, _, e := consolesession.ParseCredential(old.Value); e == nil {
			out, _, e := s.sharedCall(r.Context(), old.Value, index, "revoke", db.ConsoleSessionCommand{ID: oldID})
			if e != nil || out.status != 200 && out.status != 401 {
				problem(w, 503, "previous session logout could not be confirmed")
				return
			}
		}
	}
	http.SetCookie(w, s.cookie(token, sess.expires, int(ttl.Seconds())))
	s.sessionInfo(w, r, sess)
}

func (s *Server) sharedLogout(w http.ResponseWriter, r *http.Request) {
	cookie, err := r.Cookie(s.cookieName)
	if err != nil {
		problem(w, 401, "sign in to the console")
		return
	}
	id, _, err := consolesession.ParseCredential(cookie.Value)
	if err != nil {
		problem(w, 401, "invalid session")
		return
	}
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Djob-CSRF")), []byte(sharedCSRF(cookie.Value))) != 1 {
		problem(w, 403, "invalid console CSRF token")
		return
	}
	response, _, err := s.sharedCall(r.Context(), cookie.Value, 0, "revoke", db.ConsoleSessionCommand{ID: id})
	if err != nil || response.status != 200 {
		problem(w, 503, "shared logout could not be confirmed")
		return
	}
	http.SetCookie(w, s.cookie("", time.Unix(1, 0), -1))
	writeJSON(w, 200, map[string]bool{"signed_out": true})
}

type bufferedResponse struct {
	header http.Header
	status int
	body   bytes.Buffer
}

func (b *bufferedResponse) Header() http.Header { return b.header }
func (b *bufferedResponse) WriteHeader(code int) {
	if b.status == 0 {
		b.status = code
	}
}
func (b *bufferedResponse) Write(p []byte) (int, error) {
	if b.body.Len()+len(p) > maxReply {
		return 0, fmt.Errorf("console response exceeds limit")
	}
	if b.status == 0 {
		b.status = 200
	}
	return b.body.Write(p)
}

func (s *Server) sharedAuth(next func(http.ResponseWriter, *http.Request, *session)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie(s.cookieName)
		if err != nil {
			problem(w, 401, "sign in to the console")
			return
		}
		if _, _, err = consolesession.ParseCredential(cookie.Value); err != nil {
			problem(w, 401, "invalid session")
			return
		}
		// Logout has its own idempotent authentication path for retained tombstones.
		if r.Method == http.MethodDelete && r.URL.Path == api+"session" {
			s.sharedLogout(w, r)
			return
		}
		response, index, err := s.sharedCall(r.Context(), cookie.Value, 0, "read", db.ConsoleSessionCommand{})
		if err != nil || response.status != 200 {
			code := 503
			if response.status == 401 {
				code = 401
			}
			if response.status == 429 {
				code = 429
			}
			problem(w, code, "shared session unavailable; retained review has not been discarded")
			return
		}
		var record consolesession.Record
		if json.Unmarshal(response.body, &record) != nil {
			problem(w, 502, "invalid shared session record")
			return
		}
		data, err := consolesession.OpenReview(cookie.Value, record.Binding.ClusterID, record.Version, record.Ciphertext)
		var review sharedReview
		if err != nil || json.Unmarshal(data, &review) != nil || review.CSRF == "" {
			problem(w, 502, "shared review cannot be authenticated or decoded")
			return
		}
		if r.Method != http.MethodGet && subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Djob-CSRF")), []byte(review.CSRF)) != 1 {
			problem(w, 403, "invalid console CSRF token")
			return
		}
		sess := &session{replacement: review.Replacement, releasePlan: review.ReleasePlan, oidc: review.OIDC, draft: review.Draft, draftRevision: review.DraftRevision, token: cookie.Value, csrf: review.CSRF, identity: Identity{Subject: record.Binding.Subject, Role: record.Binding.Role, TenantID: record.Binding.TenantID}, expires: record.ExpiresAt, controller: index, shared: &sharedSession{version: record.Version, cluster: record.Binding.ClusterID, attemptUntil: review.AttemptUntil}}
		if p := review.Pending; p != nil {
			sess.pending = &pendingMutation{DraftRevision: p.DraftRevision, Endpoint: p.Endpoint, WorkerConfig: p.WorkerConfig, Request: p.Request, Response: p.Response, RequestID: p.RequestID, State: p.State, Error: p.Error, HTTPCode: p.HTTPCode, WorkerConfigAvailable: p.WorkerConfigAvailable}
			if p.State == "submitting" && !time.Now().Before(review.AttemptUntil) {
				sess.pending.State = "uncertain"
			}
		}
		before, _ := reviewBytes(sess)
		captured := &bufferedResponse{header: make(http.Header)}
		next(captured, r, sess)
		after, _ := reviewBytes(sess)
		if !bytes.Equal(before, after) {
			sess.mu.Lock()
			err := s.saveShared(r.Context(), sess)
			sess.mu.Unlock()
			if err != nil {
				problem(w, 409, "shared review changed or update was not confirmed; reload the retained review")
				return
			}
		}
		for key, values := range captured.header {
			w.Header()[key] = values
		}
		code := captured.status
		if code == 0 {
			code = 200
		}
		w.WriteHeader(code)
		_, _ = w.Write(captured.body.Bytes())
	}
}

func sharedCSRF(token string) string {
	sum := sha256.Sum256([]byte("djob/console-csrf/v1/" + token))
	return hex.EncodeToString(sum[:])
}
