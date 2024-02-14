package kafka_auth

import (
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	kafka_in "github.com/ozontech/file.d/plugin/input/kafka"
	kafka_out "github.com/ozontech/file.d/plugin/output/kafka"
	"github.com/stretchr/testify/require"
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
				}
				if tt.sasl.Enabled {
					config.SaslEnabled = true
					config.SaslMechanism = tt.sasl.Mechanism
					config.SaslUsername = tt.sasl.Username
					config.SaslPassword = tt.sasl.Password
				}

				kafka_out.NewProducer(config,
					zap.NewNop().WithOptions(zap.WithFatalHook(zapcore.WriteThenPanic)).Sugar(),
				)
			},
			func() {
				config := &kafka_in.Config{
					Brokers:                    c.Brokers,
					Topics:                     []string{inTopic},
					ConsumerGroup:              "test-auth",
					ClientID:                   "test-auth-in",
					ChannelBufferSize:          256,
					Offset_:                    kafka_in.OffsetTypeNewest,
					ConsumerMaxProcessingTime_: 200 * time.Millisecond,
					ConsumerMaxWaitTime_:       250 * time.Millisecond,
				}
				if tt.sasl.Enabled {
					config.SaslEnabled = true
					config.SaslMechanism = tt.sasl.Mechanism
					config.SaslUsername = tt.sasl.Username
					config.SaslPassword = tt.sasl.Password
				}

				kafka_in.NewConsumerGroup(config,
					zap.NewNop().WithOptions(zap.WithFatalHook(zapcore.WriteThenPanic)).Sugar(),
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
