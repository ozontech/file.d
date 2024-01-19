package kafka_auth

import (
	"testing"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/plugin/output/kafka"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Config for kafka_auth test
type Config struct {
	Brokers []string
	Topic   string
}

func (c *Config) Configure(t *testing.T, _ *cfg.Config, _ string) {
	type saslData struct {
		Enabled   bool
		Mechanism string
		Username  string
		Password  string
	}

	type tCase struct {
		sasl       saslData
		authorized bool
	}

	cases := []tCase{
		{
			sasl: saslData{
				Enabled:   true,
				Mechanism: "PLAIN",
				Username:  "user",
				Password:  "pass",
			},
			authorized: true,
		},
		{
			sasl: saslData{
				Enabled:   true,
				Mechanism: "SCRAM-SHA-256",
				Username:  "user",
				Password:  "pass",
			},
			authorized: true,
		},
		{
			sasl: saslData{
				Enabled:   true,
				Mechanism: "SCRAM-SHA-512",
				Username:  "user",
				Password:  "pass",
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
				Enabled:   true,
				Mechanism: "PLAIN",
				Username:  "user",
				Password:  "pass123",
			},
			authorized: false,
		},
	}
	for _, tt := range cases {
		go func(tt tCase) {
			config := &kafka.Config{
				Brokers:      c.Brokers,
				DefaultTopic: c.Topic,
				BatchSize_:   10,
			}

			if tt.sasl.Enabled {
				config.SaslEnabled = true
				config.SaslMechanism = tt.sasl.Mechanism
				config.SaslUsername = tt.sasl.Username
				config.SaslPassword = tt.sasl.Password
			}

			panicTestFn := func() {
				kafka.NewProducer(config,
					zap.NewNop().WithOptions(zap.WithFatalHook(zapcore.WriteThenPanic)).Sugar(),
				)
			}
			if tt.authorized {
				require.NotPanics(t, panicTestFn, "func shouldn't panic")
			} else {
				require.Panics(t, panicTestFn, "func should panic")
			}
		}(tt)
	}
}

func (c *Config) Send(_ *testing.T) {}

func (c *Config) Validate(_ *testing.T) {}
