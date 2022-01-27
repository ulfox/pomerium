// Package onelogin implements OpenID Connect for OneLogin
//
// https://www.pomerium.io/docs/identity-providers/one-login.html
package onelogin

import (
	"context"
	"fmt"
	"strings"

	oidc "github.com/coreos/go-oidc/v3/oidc"

	pom_oidc "github.com/pomerium/pomerium/internal/identity/oidc"
	"github.com/pomerium/pomerium/pkg/grpc/session"
)

const (
	// Name identifies the OneLogin identity provider
	Name = "onelogin"

	defaultProviderURL = "https://openid-connect.onelogin.com/oidc"
)

var (
	defaultV1Scopes = []string{oidc.ScopeOpenID, "profile", "email", "groups", "offline_access"}
	defaultV2Scopes = []string{oidc.ScopeOpenID, "profile", "email", "groups"} // v2 does not support offline_access
)

// Provider is an OneLogin implementation of the Authenticator interface.
type Provider struct {
	*pom_oidc.Provider
}

// New instantiates an OpenID Connect (OIDC) provider for OneLogin.
func New(ctx context.Context, cfg *session.OAuthConfig) (*Provider, error) {
	var p Provider
	var err error
	if cfg.GetProviderUrl() == "" {
		cfg = cfg.Clone()
		cfg.ProviderUrl = defaultProviderURL
	}
	if strings.Contains(cfg.GetProviderUrl(), "/oidc/2") {
		cfg = cfg.Clone()
		cfg.Scopes = defaultV2Scopes
	} else {
		cfg = cfg.Clone()
		cfg.Scopes = defaultV1Scopes
	}
	genericOidc, err := pom_oidc.New(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("%s: failed creating oidc provider: %w", Name, err)
	}
	p.Provider = genericOidc
	return &p, nil
}

// Name returns the provider name.
func (p *Provider) Name() string {
	return Name
}
