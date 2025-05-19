package redis_clients

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/xredis"
)

type Config struct{}

func (c *Config) Configure(t *testing.T, _ *cfg.Config, _ string) {
	cases := []struct {
		name    string
		options *xredis.Options
	}{
		{
			name: "base",
			options: &xredis.Options{
				ClientType: xredis.ClientTypeBase,
				Addrs:      []string{":6380"},
			},
		},
		{
			name: "ring",
			options: &xredis.Options{
				ID:         "e2e_test",
				ClientType: xredis.ClientTypeRing,
				Addrs:      []string{":6381", ":6382"},
			},
		},
		{
			name: "cluster",
			options: &xredis.Options{
				ClientType: xredis.ClientTypeCluster,
				Addrs:      []string{":6379"},
			},
		},
	}

	r := require.New(t)
	ctx := context.Background()
	for _, tt := range cases {
		client := xredis.NewClient(tt.options)
		r.NoError(client.Ping(ctx).Err())

		r.NoError(client.Set(ctx, "test_key", "test_value", 0).Err())
	}
}

func (c *Config) Send(_ *testing.T) {}

func (c *Config) Validate(_ *testing.T) {}
