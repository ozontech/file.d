package cfg

import (
	"context"
	"errors"
	"os"

	"github.com/ozontech/file.d/xoauth"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/plugin/kzap"
	"github.com/twmb/tlscfg"
	"go.uber.org/zap"
)

type KafkaClientConfig interface {
	GetBrokers() []string
	GetClientID() string

	IsSaslEnabled() bool
	GetSaslConfig() KafkaClientSaslConfig

	IsSslEnabled() bool
	GetSslConfig() KafkaClientSslConfig
}

type KafkaClientSaslConfig struct {
	SaslMechanism string
	SaslUsername  string
	SaslPassword  string

	SaslOAuth KafkaClientOAuthConfig
}

type KafkaClientSslConfig struct {
	CACert        string
	ClientCert    string
	ClientKey     string
	SslSkipVerify bool
}

type KafkaClientOAuthConfig struct {
	// static
	Token string `json:"token"`

	// dynamic
	ClientID     string   `json:"client_id"`
	ClientSecret string   `json:"client_secret"`
	TokenURL     string   `json:"token_url"`
	Scopes       []string `json:"scopes" slice:"true"`
	AuthStyle    string   `json:"auth_style" default:"params" options:"params|header"`
}

func (c *KafkaClientOAuthConfig) isStatic() bool {
	return c.Token != ""
}

func (c *KafkaClientOAuthConfig) isDynamic() bool {
	return c.ClientID != "" && c.ClientSecret != "" && c.TokenURL != ""
}

func (c *KafkaClientOAuthConfig) isValid() bool {
	return c.isStatic() || c.isDynamic()
}

func GetKafkaClientOAuthTokenSource(ctx context.Context, cfg KafkaClientConfig) (xoauth.TokenSource, error) {
	saslCfg := cfg.GetSaslConfig()

	if !cfg.IsSaslEnabled() || saslCfg.SaslMechanism != "OAUTHBEARER" {
		return nil, nil
	}

	saslOAuth := saslCfg.SaslOAuth
	if !saslOAuth.isValid() {
		return nil, errors.New("invalid SASL OAUTHBEARER config")
	}

	if saslOAuth.isDynamic() {
		authStyle := xoauth.AuthStyleInParams
		if saslOAuth.AuthStyle == "header" {
			authStyle = xoauth.AuthStyleInHeader
		}

		return xoauth.NewReuseTokenSource(ctx, &xoauth.Config{
			ClientID:     saslOAuth.ClientID,
			ClientSecret: saslOAuth.ClientSecret,
			TokenURL:     saslOAuth.TokenURL,
			Scopes:       saslOAuth.Scopes,
			AuthStyle:    authStyle,
		})
	}

	return xoauth.NewStaticTokenSource(&xoauth.Token{
		AccessToken: saslOAuth.Token,
	}), nil
}

func GetKafkaClientOptions(c KafkaClientConfig, l *zap.Logger, tokenSource xoauth.TokenSource) []kgo.Opt {
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.GetBrokers()...),
		kgo.ClientID(c.GetClientID()),
		kgo.WithLogger(kzap.New(l)),
	}

	if c.IsSaslEnabled() {
		saslConfig := c.GetSaslConfig()
		switch saslConfig.SaslMechanism {
		case "PLAIN":
			opts = append(opts, kgo.SASL(plain.Auth{
				User: saslConfig.SaslUsername,
				Pass: saslConfig.SaslPassword,
			}.AsMechanism()))
		case "SCRAM-SHA-256":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: saslConfig.SaslUsername,
				Pass: saslConfig.SaslPassword,
			}.AsSha256Mechanism()))
		case "SCRAM-SHA-512":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: saslConfig.SaslUsername,
				Pass: saslConfig.SaslPassword,
			}.AsSha512Mechanism()))
		case "AWS_MSK_IAM":
			opts = append(opts, kgo.SASL(aws.Auth{
				AccessKey: saslConfig.SaslUsername,
				SecretKey: saslConfig.SaslPassword,
			}.AsManagedStreamingIAMMechanism()))
		case "OAUTHBEARER":
			authFn := func(ctx context.Context) (oauth.Auth, error) {
				if tokenSource == nil {
					return oauth.Auth{}, errors.New("uninitialized token source")
				}
				t := tokenSource.Token(ctx)
				if t == nil {
					return oauth.Auth{}, errors.New("empty token from token source")
				}
				return oauth.Auth{Token: t.AccessToken}, nil
			}
			opts = append(opts, kgo.SASL(oauth.Oauth(authFn)))
		}
	}

	if c.IsSslEnabled() {
		sslConfig := c.GetSslConfig()
		tlsOpts := []tlscfg.Opt{}
		if sslConfig.CACert != "" || sslConfig.ClientCert != "" || sslConfig.ClientKey != "" {
			if sslConfig.CACert != "" {
				if _, err := os.Stat(sslConfig.CACert); err != nil {
					tlsOpts = append(tlsOpts,
						tlscfg.WithCA(
							[]byte(sslConfig.CACert), tlscfg.ForClient,
						),
					)
				} else {
					tlsOpts = append(tlsOpts,
						tlscfg.MaybeWithDiskCA(sslConfig.CACert, tlscfg.ForClient),
					)
				}
			}

			if _, err := os.Stat(sslConfig.ClientCert); err != nil {
				tlsOpts = append(tlsOpts,
					tlscfg.WithKeyPair(
						[]byte(sslConfig.ClientCert), []byte(sslConfig.ClientKey),
					),
				)
			} else {
				tlsOpts = append(tlsOpts,
					tlscfg.MaybeWithDiskKeyPair(sslConfig.ClientCert, sslConfig.ClientKey),
				)
			}
		}
		tc, err := tlscfg.New(tlsOpts...)
		if err != nil {
			l.Fatal("unable to create tls config", zap.Error(err))
		}
		tc.InsecureSkipVerify = sslConfig.SslSkipVerify
		opts = append(opts, kgo.DialTLSConfig(tc))
	}

	return opts
}
