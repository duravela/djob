package server

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/duravela/djob/internal/oidcauth"
)

// Roles are bound to issuer subjects by the operator, never taken from a
// caller-supplied role/tenant claim or an unverified proxy header.
type APIOIDCConfig struct {
	BackchannelLogout bool                     `json:"backchannel_logout"`
	Issuer            string                   `json:"issuer"`
	ClientID          string                   `json:"client_id"`
	Bindings          map[string]APICredential `json:"bindings"`
	HTTPClient        *http.Client             `json:"-"` // Verified transport for embedded/private-CA deployments.
}

func (a *apiAuthorizer) configureOIDC(c *APIOIDCConfig) error {
	if c == nil {
		return nil
	}
	if len(c.Bindings) == 0 || len(c.Bindings) > 128 {
		return fmt.Errorf("OIDC requires 1 to 128 subject bindings")
	}
	a.oidcBindings = make(map[string]apiCredential, len(c.Bindings))
	for sub, binding := range c.Bindings {
		if sub == "" || len(sub) > 256 || binding.Token != "" || (binding.Role != APIReader && binding.Role != APIWriter && binding.Role != APIOperator) {
			return fmt.Errorf("invalid OIDC subject binding")
		}
		binding.Token = "oidc-validation-placeholder-credential-000000"
		checked, err := newAPIAuthorizer(&APIAuthConfig{Credentials: []APICredential{binding}})
		if err != nil {
			return fmt.Errorf("invalid OIDC role binding")
		}
		a.oidcBindings[sub] = checked.credentials[0]
	}
	provider, err := oidcauth.Provider(c.Issuer, c.ClientID, c.HTTPClient)
	if err != nil {
		return err
	}
	a.oidcIssuer = c.Issuer
	a.oidcVerifier = provider.Verifier(&oidc.Config{ClientID: c.ClientID})
	if c.BackchannelLogout {
		a.oidcLogoutVerifier = provider.Verifier(&oidc.Config{ClientID: c.ClientID, SkipExpiryCheck: true})
	}
	return nil
}
func (a *apiAuthorizer) oidcPrincipal(ctx context.Context, raw string) *apiCredential {
	if a.oidcVerifier == nil {
		return nil
	}
	token, err := a.oidcVerifier.Verify(ctx, raw)
	if err != nil {
		return nil
	}
	principal, ok := a.oidcBindings[token.Subject]
	if !ok {
		return nil
	}
	if token.IssuedAt.IsZero() || token.Expiry.Sub(token.IssuedAt) > 24*time.Hour || token.IssuedAt.After(time.Now().Add(5*time.Minute)) {
		return nil
	}
	var claims struct {
		SID string `json:"sid"`
	}
	if token.Claims(&claims) != nil || len(claims.SID) > 256 {
		return nil
	}
	if a.oidcRevoked != nil {
		revoked, err := a.oidcRevoked(ctx, a.oidcIssuer, token.Subject, claims.SID, token.IssuedAt.UTC())
		if err != nil || revoked {
			return nil
		}
	}
	principal.oidcSID, principal.oidcIssuedAt = claims.SID, token.IssuedAt.UTC()
	principal.hash = sha256.Sum256([]byte(raw))
	principal.oidcSubject, principal.oidcIssuer, principal.oidcExpires = token.Subject, a.oidcIssuer, token.Expiry.UTC()
	return &principal
}
func validBearerToken(token string) bool {
	if validAPIToken(token) {
		return true
	}
	if len(token) < 32 || len(token) > 8192 {
		return false
	}
	dots := 0
	for _, c := range token {
		if c == '.' {
			dots++
			continue
		}
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' {
			continue
		}
		return false
	}
	return dots == 2
}
