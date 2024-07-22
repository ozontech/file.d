package cfg

import (
	"crypto/tls"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/aws"
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
}

type KafkaClientSslConfig struct {
	CACert        string
	ClientCert    string
	ClientKey     string
	SslSkipVerify bool
}

func GetKafkaClientOptions(c KafkaClientConfig, l *zap.Logger) []kgo.Opt {
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
		}
		opts = append(opts, kgo.DialTLSConfig(new(tls.Config)))
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
