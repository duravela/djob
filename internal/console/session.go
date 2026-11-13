package console

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"
)

func validToken(token string) bool {
	if len(token) < 32 || len(token) > 8192 {
		return false
	}
	for _, ch := range token {
		if ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || strings.ContainsRune("-._~+/=", ch) {
			continue
		}
		return false
	}
	return true
}

func (s *Server) cookie(value string, expires time.Time, age int) *http.Cookie {
	return &http.Cookie{Name: s.cookieName, Value: value, Path: "/", HttpOnly: true, Secure: s.origin.Scheme == "https", SameSite: http.SameSiteStrictMode, Expires: expires, MaxAge: age}
}

// Call with Server.mu held. Every removal path stops the timer so logout,
// replacement and shutdown do not retain a callback until the original expiry.
func (s *Server) removeSessionLocked(id string) {
	if sess := s.sessions[id]; sess != nil && sess.expiry != nil {
		sess.expiry.Stop()
	}
	delete(s.sessions, id)
}

func (s *Server) login(w http.ResponseWriter, r *http.Request) {
	if s.shared {
		s.sharedLogin(w, r)
		return
	}
	var input struct {
		Token string `json:"token"`
	}
	if err := decode(w, r, 12288, &input); err != nil || !validToken(input.Token) {
		problem(w, 400, "provide a valid djob API credential")
		return
	}
	response, controller, err := s.request(r.Context(), input.Token, 0, "identity", http.MethodGet, nil, true)
	if err != nil {
		problem(w, 502, err.Error())
		return
	}
	if response.status != 200 {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(response.status)
		_, _ = w.Write(response.body)
		return
	}
	var identity Identity
	if err := json.Unmarshal(response.body, &identity); err != nil || identity.Subject == "" || (identity.Role != "reader" && identity.Role != "writer" && identity.Role != "operator") || identity.TenantID == "" && identity.Role != "operator" {
		problem(w, 502, "controller returned an invalid console identity")
		return
	}
	id, err := randomID()
	if err != nil {
		problem(w, 500, "cannot create a session")
		return
	}
	csrf, err := randomID()
	if err != nil {
		problem(w, 500, "cannot create a CSRF token")
		return
	}
	now := time.Now()
	sess := &session{token: input.Token, csrf: csrf, identity: identity, expires: identityExpiry(identity, now.Add(s.ttl)), controller: controller}
	sess.oidc, _ = r.Context().Value(oidcLoginKey{}).(*oidcSession)
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		problem(w, 503, "console is shutting down")
		return
	}
	for key, old := range s.sessions {
		if !now.Before(old.expires) {
			s.removeSessionLocked(key)
		}
	}
	if old, err := r.Cookie(s.cookieName); err == nil {
		s.removeSessionLocked(old.Value)
	}
	if len(s.sessions) >= s.maxSessions {
		s.mu.Unlock()
		problem(w, 503, "console session capacity reached; try again later")
		return
	}
	s.sessions[id] = sess
	// Capture only the opaque ID, not the credential-bearing session. Expiry
	// drops the registry reference even when the browser makes no more requests.
	sess.expiry = time.AfterFunc(time.Until(sess.expires), func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.removeSessionLocked(id)
	})
	s.mu.Unlock()
	http.SetCookie(w, s.cookie(id, sess.expires, int(s.ttl.Seconds())))
	s.sessionInfo(w, r, sess)
}

func (s *Server) sessionInfo(w http.ResponseWriter, r *http.Request, sess *session) {
	if sess.oidc != nil && sess.shared == nil {
		sess.mu.Lock()
		index := sess.controller
		sess.mu.Unlock()
		response, _, err := s.request(r.Context(), sess.credential(), index, "identity", http.MethodGet, nil, true)
		if err != nil {
			problem(w, 503, "identity verification unavailable")
			return
		}
		var current Identity
		if response.status != 200 || json.Unmarshal(response.body, &current) != nil || current.Subject != sess.identity.Subject || current.Role != sess.identity.Role || current.TenantID != sess.identity.TenantID {
			problem(w, 401, "SSO session ended or permissions changed")
			return
		}
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	writeJSON(w, 200, struct {
		Identity      Identity         `json:"identity"`
		CSRF          string           `json:"csrf"`
		Expires       time.Time        `json:"expires_at"`
		Controller    string           `json:"controller"`
		Pending       *pendingMutation `json:"pending"`
		Draft         json.RawMessage  `json:"draft,omitempty"`
		DraftRevision uint64           `json:"draft_revision,string"`
		SSO           bool             `json:"sso,omitempty"`
		RefreshAt     *time.Time       `json:"oidc_refresh_at,omitempty"`
	}{sess.identity, sess.csrf, sess.expires, s.controllers[sess.controller].String(), sess.pending, sess.draft, sess.draftRevision, sess.oidc != nil, refreshAt(sess)})
}

func (s *Server) logout(w http.ResponseWriter, r *http.Request, _ *session) {
	if s.shared {
		s.sharedLogout(w, r)
		return
	}
	cookie, _ := r.Cookie(s.cookieName)
	s.mu.Lock()
	s.removeSessionLocked(cookie.Value)
	s.mu.Unlock()
	http.SetCookie(w, s.cookie("", time.Unix(1, 0), -1))
	writeJSON(w, 200, map[string]bool{"signed_out": true})
}

func identityExpiry(identity Identity, expires time.Time) time.Time {
	if !identity.ExpiresAt.IsZero() && identity.ExpiresAt.Before(expires) {
		return identity.ExpiresAt
	}
	return expires
}
