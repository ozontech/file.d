package xoauth

import (
	"context"

	"github.com/Shopify/sarama"
	"golang.org/x/oauth2"
	cred "golang.org/x/oauth2/clientcredentials"
)

// saramaTokenProvider implements sarama.AccessTokenProvider
type saramaTokenProvider struct {
	tokenSource oauth2.TokenSource
}

// NewSaramaTokenProvider creates a new sarama.AccessTokenProvider with the provided clientID and clientSecret.
// The provided tokenURL is used to perform the 2 legged client credentials flow.
func NewSaramaTokenProvider(cfg Config) (sarama.AccessTokenProvider, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	credCfg := cred.Config{
		ClientID:     cfg.ClientID,
		ClientSecret: cfg.ClientSecret,
		TokenURL:     cfg.TokenURL,
	}

	return &saramaTokenProvider{
		tokenSource: credCfg.TokenSource(context.Background()),
	}, nil
}

// Token returns a new sarama.AccessToken
func (t *saramaTokenProvider) Token() (*sarama.AccessToken, error) {
	token, err := t.tokenSource.Token()
	if err != nil {
		return nil, err
	}

	return &sarama.AccessToken{Token: token.AccessToken}, nil
}
