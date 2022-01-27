// Package identity provides support for making OpenID Connect (OIDC)
// and OAuth2 authenticated HTTP requests with third party identity providers.
package identity

import (
	"context"
	"fmt"
	"net/url"

	"golang.org/x/oauth2"

	"github.com/pomerium/pomerium/internal/identity/identity"
	"github.com/pomerium/pomerium/internal/identity/oauth/github"
	"github.com/pomerium/pomerium/internal/identity/oidc"
	"github.com/pomerium/pomerium/internal/identity/oidc/auth0"
	"github.com/pomerium/pomerium/internal/identity/oidc/azure"
	"github.com/pomerium/pomerium/internal/identity/oidc/gitlab"
	"github.com/pomerium/pomerium/internal/identity/oidc/google"
	"github.com/pomerium/pomerium/internal/identity/oidc/okta"
	"github.com/pomerium/pomerium/internal/identity/oidc/onelogin"
	"github.com/pomerium/pomerium/internal/identity/oidc/ping"
	"github.com/pomerium/pomerium/pkg/grpc/session"
)

// Authenticator is an interface representing the ability to authenticate with an identity provider.
type Authenticator interface {
	Authenticate(context.Context, string, identity.State) (*oauth2.Token, error)
	Refresh(context.Context, *oauth2.Token, identity.State) (*oauth2.Token, error)
	Revoke(context.Context, *oauth2.Token) error
	GetSignInURL(state string) (string, error)
	Name() string
	LogOut() (*url.URL, error)
	UpdateUserInfo(ctx context.Context, t *oauth2.Token, v interface{}) error
}

// newAuthenticator returns a new identity provider based on its name.
func newAuthenticator(cfg *session.OAuthConfig) (a Authenticator, err error) {
	ctx := context.Background()
	switch cfg.GetProviderName() {
	case auth0.Name:
		a, err = auth0.New(ctx, cfg)
	case azure.Name:
		a, err = azure.New(ctx, cfg)
	case gitlab.Name:
		a, err = gitlab.New(ctx, cfg)
	case github.Name:
		a, err = github.New(ctx, cfg)
	case google.Name:
		a, err = google.New(ctx, cfg)
	case oidc.Name:
		a, err = oidc.New(ctx, cfg)
	case okta.Name:
		a, err = okta.New(ctx, cfg)
	case onelogin.Name:
		a, err = onelogin.New(ctx, cfg)
	case ping.Name:
		a, err = ping.New(ctx, cfg)
	default:
		return nil, fmt.Errorf("identity: unknown provider: %s", cfg.GetProviderName())
	}
	if err != nil {
		return nil, err
	}
	return a, nil
}

func Authenticate(ctx context.Context, cfg *session.OAuthConfig, authorizationCode string, state identity.State) (*oauth2.Token, error) {
	a, err := newAuthenticator(cfg)
	if err != nil {
		return nil, err
	}
	return a.Authenticate(ctx, authorizationCode, state)
}

func GetSignInURL(cfg *session.OAuthConfig, state string) (string, error) {
	a, err := newAuthenticator(cfg)
	if err != nil {
		return "", err
	}
	return a.GetSignInURL(state)
}

func LogOut(cfg *session.OAuthConfig) (*url.URL, error) {
	a, err := newAuthenticator(cfg)
	if err != nil {
		return nil, err
	}
	return a.LogOut()
}

func Name(cfg *session.OAuthConfig) (string, error) {
	a, err := newAuthenticator(cfg)
	if err != nil {
		return "", err
	}
	return a.Name(), nil
}

func Refresh(ctx context.Context, cfg *session.OAuthConfig, oauthToken *oauth2.Token, state identity.State) (*oauth2.Token, error) {
	a, err := newAuthenticator(cfg)
	if err != nil {
		return nil, err
	}
	return a.Refresh(ctx, oauthToken, state)
}

func Revoke(ctx context.Context, cfg *session.OAuthConfig, oauthToken *oauth2.Token) error {
	a, err := newAuthenticator(cfg)
	if err != nil {
		return err
	}
	return a.Revoke(ctx, oauthToken)
}

func UpdateUserInfo(ctx context.Context, cfg *session.OAuthConfig, t *oauth2.Token, v interface{}) error {
	a, err := newAuthenticator(cfg)
	if err != nil {
		return err
	}
	return a.UpdateUserInfo(ctx, t, v)
}
