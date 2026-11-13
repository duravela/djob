// Package oidcauth shares verified OpenID Connect discovery between API and BFF.
package oidcauth

import (
	"context"
	"fmt"
	"github.com/coreos/go-oidc/v3/oidc"
	"net"
	"net/http"
	"net/url"
	"time"
)

func Endpoint(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("invalid OIDC endpoint")
	}
	ip := net.ParseIP(u.Hostname())
	if u.Scheme != "https" && !(u.Scheme == "http" && ip != nil && ip.IsLoopback()) {
		return fmt.Errorf("OIDC requires HTTPS except on literal loopback addresses")
	}
	return nil
}
func Provider(issuer, clientID string, client *http.Client) (*oidc.Provider, error) {
	if Endpoint(issuer) != nil || clientID == "" {
		return nil, fmt.Errorf("configure an OIDC issuer and client ID")
	}
	bounded := &http.Client{}
	if client != nil {
		*bounded = *client
	}
	bounded.Timeout = 10 * time.Second
	bounded.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	client = bounded
	ctx, cancel := context.WithTimeout(oidc.ClientContext(context.Background(), client), 10*time.Second)
	defer cancel()
	provider, err := oidc.NewProvider(ctx, issuer)
	if err != nil {
		return nil, fmt.Errorf("OIDC discovery failed")
	}
	var metadata struct {
		JWKS string `json:"jwks_uri"`
	}
	if provider.Claims(&metadata) != nil || Endpoint(metadata.JWKS) != nil || Endpoint(provider.Endpoint().AuthURL) != nil || Endpoint(provider.Endpoint().TokenURL) != nil {
		return nil, fmt.Errorf("OIDC provider advertised an unsafe endpoint")
	}
	return provider, nil
}
