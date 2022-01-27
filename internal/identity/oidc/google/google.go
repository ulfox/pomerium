// Package google implements OpenID Connect for Google and GSuite.
//
// https://www.pomerium.io/docs/identity-providers/google.html
// https://developers.google.com/identity/protocols/oauth2/openid-connect
package google

import (
	"context"
	"fmt"

	oidc "github.com/coreos/go-oidc/v3/oidc"
	"google.golang.org/protobuf/proto"

	pom_oidc "github.com/pomerium/pomerium/internal/identity/oidc"
	"github.com/pomerium/pomerium/pkg/grpc/session"
)

const (
	// Name identifies the Google identity provider
	Name = "google"

	defaultProviderURL = "https://accounts.google.com"
)

var defaultScopes = []string{oidc.ScopeOpenID, "profile", "email"}

// unlike other identity providers, google does not support the `offline_access` scope and instead
// requires we set this on a custom uri param. Also, ` prompt` must be set to `consent`to ensure
// that our application always receives a refresh token (ask google). And finally, we default to
// having the user select which Google account they'd like to use.
//
// For more details, please see google's documentation:
// 	https://developers.google.com/identity/protocols/oauth2/web-server#offline
// 	https://developers.google.com/identity/protocols/oauth2/openid-connect#authenticationuriparameters
var defaultAuthCodeOptions = map[string]string{"prompt": "select_account consent", "access_type": "offline"}

// Provider is a Google implementation of the Authenticator interface.
type Provider struct {
	*pom_oidc.Provider
}

// New instantiates an OpenID Connect (OIDC) session with Google.
func New(ctx context.Context, cfg *session.OAuthConfig) (*Provider, error) {
	var p Provider
	var err error
	if cfg.GetProviderUrl() == "" {
		cfg = proto.Clone(cfg).(*session.OAuthConfig)
		cfg.ProviderUrl = defaultProviderURL
	}
	if len(cfg.GetScopes()) == 0 {
		cfg = proto.Clone(cfg).(*session.OAuthConfig)
		cfg.Scopes = defaultScopes
	}
	genericOidc, err := pom_oidc.New(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("%s: failed creating oidc provider: %w", Name, err)
	}
	p.Provider = genericOidc

	p.AuthCodeOptions = defaultAuthCodeOptions
	if len(cfg.GetAuthCodeOptions()) != 0 {
		p.AuthCodeOptions = cfg.GetAuthCodeOptions()
	}
	return &p, nil
}

// Name returns the provider name.
func (p *Provider) Name() string {
	return Name
}
