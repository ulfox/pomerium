// Package okta implements OpenID Connect for okta
//
// https://www.pomerium.io/docs/identity-providers/okta.html
package okta

import (
	"context"
	"fmt"

	pom_oidc "github.com/pomerium/pomerium/internal/identity/oidc"
	"github.com/pomerium/pomerium/pkg/grpc/session"
)

const (
	// Name identifies the Okta identity provider
	Name = "okta"
)

// Provider is an Okta implementation of the Authenticator interface.
type Provider struct {
	*pom_oidc.Provider
}

// New instantiates an OpenID Connect (OIDC) provider for Okta.
func New(ctx context.Context, cfg *session.OAuthConfig) (*Provider, error) {
	var p Provider
	var err error
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
