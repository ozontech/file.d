package kafka_auth

import (
	"context"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	kafka_in "github.com/ozontech/file.d/plugin/input/kafka"
	kafka_out "github.com/ozontech/file.d/plugin/output/kafka"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	outTopic = "test_out_topic"
	inTopic  = "test_in_topic"
)

// Config for kafka_auth test
type Config struct {
	Brokers []string
}

func (c *Config) Configure(t *testing.T, _ *cfg.Config, _ string) {
	type saslData struct {
		Enabled          bool
		Mechanism        string
		Username         string
		Password         string
		AuthByClientCert bool
	}

	type tCase struct {
		sasl       saslData
		authorized bool
	}

	cases := []tCase{
		{
			sasl: saslData{
				Enabled:          true,
				Mechanism:        "PLAIN",
				Username:         "user",
				Password:         "pass",
				AuthByClientCert: true,
			},
			authorized: true,
		},
		{
			sasl: saslData{
				Enabled:          true,
				Mechanism:        "SCRAM-SHA-256",
				Username:         "user",
				Password:         "pass",
				AuthByClientCert: true,
			},
			authorized: true,
		},
		{
			sasl: saslData{
				Enabled:          true,
				Mechanism:        "SCRAM-SHA-512",
				Username:         "user",
				Password:         "pass",
				AuthByClientCert: true,
			},
			authorized: true,
		},
		{
			sasl: saslData{
				Enabled: false,
			},
			authorized: false,
		},
		{
			sasl: saslData{
				Enabled:          true,
				Mechanism:        "PLAIN",
				Username:         "user",
				Password:         "pass123",
				AuthByClientCert: false,
			},
			authorized: false,
		},
	}

	r := require.New(t)

	for _, tt := range cases {
		testFns := []func(){
			func() {
				config := &kafka_out.Config{
					Brokers:          c.Brokers,
					DefaultTopic:     outTopic,
					ClientID:         "test-auth-out",
					BatchSize_:       10,
					MaxMessageBytes_: 1000000,
					SslEnabled:       true,
					SslSkipVerify:    true,
				}
				if tt.sasl.Enabled {
					config.SaslEnabled = true
					config.SaslMechanism = tt.sasl.Mechanism
					config.SaslUsername = tt.sasl.Username
					config.SaslPassword = tt.sasl.Password
				}

				if tt.sasl.AuthByClientCert {
					config.ClientKey = "./kafka_auth/certs/client_key.pem"
					config.ClientCert = "./kafka_auth/certs/client_cert.pem"
				}

				kafka_out.NewClient(config,
					zap.NewNop().WithOptions(zap.WithFatalHook(zapcore.WriteThenPanic)),
				)
			},
			func() {
				config := &kafka_in.Config{
					Brokers:              c.Brokers,
					Topics:               []string{inTopic},
					ConsumerGroup:        "test-auth",
					ClientID:             "test-auth-in",
					ChannelBufferSize:    256,
					Offset_:              kafka_in.OffsetTypeNewest,
					ConsumerMaxWaitTime_: 250 * time.Millisecond,
					SslEnabled:           true,
					SslSkipVerify:        true,
					SessionTimeout_:      10 * time.Second,
					AutoCommitInterval_:  1 * time.Second,
				}
				if tt.sasl.Enabled {
					config.SaslEnabled = true
					config.SaslMechanism = tt.sasl.Mechanism
					config.SaslUsername = tt.sasl.Username
					config.SaslPassword = tt.sasl.Password
				}

				if tt.sasl.AuthByClientCert {
					config.ClientKey = "./kafka_auth/certs/client_key.pem"
					config.ClientCert = "./kafka_auth/certs/client_cert.pem"
				}

				kafka_in.NewClient(config,
					zap.NewNop().WithOptions(zap.WithFatalHook(zapcore.WriteThenPanic)),
					Consumer{},
				)
			},
		}

		for _, fn := range testFns {
			if tt.authorized {
				r.NotPanics(fn, "func shouldn't panic")
			} else {
				r.Panics(fn, "func should panic")
			}
		}
	}
}

func (c *Config) Send(_ *testing.T) {}

func (c *Config) Validate(_ *testing.T) {}

type Consumer struct{}

func (c Consumer) Assigned(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {}

func (c Consumer) Lost(_ context.Context, _ *kgo.Client, lost map[string][]int32) {}
