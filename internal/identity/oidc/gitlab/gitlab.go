// Package gitlab implements OpenID Connect for Gitlab
//
// https://www.pomerium.io/docs/identity-providers/gitlab.html
package gitlab

import (
	"context"
	"fmt"

	"github.com/coreos/go-oidc/v3/oidc"
	"google.golang.org/protobuf/proto"

	pom_oidc "github.com/pomerium/pomerium/internal/identity/oidc"
	"github.com/pomerium/pomerium/pkg/grpc/session"
)

// Name identifies the GitLab identity provider.
const Name = "gitlab"

var defaultScopes = []string{oidc.ScopeOpenID, "profile", "email"}

const (
	defaultProviderURL = "https://gitlab.com"
)

// Provider is a Gitlab implementation of the Authenticator interface.
type Provider struct {
	*pom_oidc.Provider
}

// New instantiates an OpenID Connect (OIDC) provider for Gitlab.
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

	return &p, nil
}

// Name returns the provider name.
func (p *Provider) Name() string {
	return Name
}
