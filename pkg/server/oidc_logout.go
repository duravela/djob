package server

import (
	"context"
	"encoding/json"
	"github.com/duravela/djob/pkg/db"
	"golang.org/x/time/rate"
	"net/http"
	"time"
)

var logoutBudget = rate.NewLimiter(2, 8)
var logoutSlots = make(chan struct{}, 4)

func (s *jobsServer) oidcLogoutHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery != "" {
		logoutProblem(w, 400, "logout tokens belong in the POST form")
		return
	}
	if s.auth == nil || s.auth.oidcLogoutVerifier == nil {
		logoutProblem(w, 404, "backchannel logout is not configured")
		return
	}
	if !logoutBudget.Allow() {
		logoutProblem(w, 429, "logout rate limit")
		return
	}
	select {
	case logoutSlots <- struct{}{}:
		defer func() { <-logoutSlots }()
	default:
		logoutProblem(w, 503, "logout busy")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16384)
	if r.ParseForm() != nil || len(r.PostForm["logout_token"]) != 1 {
		logoutProblem(w, 400, "invalid logout")
		return
	}
	token, err := s.auth.oidcLogoutVerifier.Verify(r.Context(), r.PostForm.Get("logout_token"))
	if err != nil {
		logoutProblem(w, 400, "invalid logout token")
		return
	}
	var c struct {
		SID    string                    `json:"sid"`
		JTI    string                    `json:"jti"`
		Nonce  json.RawMessage           `json:"nonce"`
		Events map[string]map[string]any `json:"events"`
	}
	if token.Claims(&c) != nil || len(c.Nonce) > 0 || c.JTI == "" || len(c.JTI) > 256 || token.Subject == "" && c.SID == "" || c.Events["http://schemas.openid.net/event/backchannel-logout"] == nil || !token.Expiry.IsZero() && !time.Now().Before(token.Expiry) || token.IssuedAt.Before(time.Now().Add(-5*time.Minute)) || token.IssuedAt.After(time.Now().Add(5*time.Minute)) {
		logoutProblem(w, 400, "invalid logout claims")
		return
	}
	backend, ok := s.backend.(interface {
		ProposeConsoleSession(context.Context, db.ConsoleSessionCommand) (db.ConsoleSessionResult, error)
	})
	if !ok {
		logoutProblem(w, 503, "logout storage unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	if s.oidcLogoutGuard != nil {
		if err := s.oidcLogoutGuard(ctx); err != nil {
			writeAPIError(w, apiError(err))
			return
		}
	}
	_, err = backend.ProposeConsoleSession(ctx, db.ConsoleSessionCommand{Action: "oidc_logout", At: time.Now().UTC(), Logout: &db.OIDCLogout{Issuer: s.auth.oidcIssuer, Subject: token.Subject, SID: c.SID, IssuedAt: token.IssuedAt}})
	if err != nil {
		writeAPIError(w, apiError(err))
		return
	}
	writeConsoleJSON(w, map[string]bool{"logged_out": true})
}

func logoutProblem(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}
