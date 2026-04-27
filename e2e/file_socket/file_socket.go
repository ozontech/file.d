package file_socket

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/ozontech/file.d/cfg"
	"github.com/stretchr/testify/require"
)

type Config struct {
	Count   int
	Network string
	Address string
	dir     string
	server  *testServer
}

func (c *Config) Configure(t *testing.T, conf *cfg.Config, pipelineName string) {
	t.Helper()

	c.dir = t.TempDir()
	offsetsDir := t.TempDir()

	input := conf.Pipelines[pipelineName].Raw.Get("input")
	input.Set("watching_dir", c.dir)
	input.Set("filename_pattern", "messages.log")
	input.Set("offsets_file", filepath.Join(offsetsDir, "offsets.yaml"))

	output := conf.Pipelines[pipelineName].Raw.Get("output")
	output.Set("network", c.Network)
	output.Set("address", c.Address)

	srv, err := newTestServer(c.Network, c.Address)
	require.NoError(t, err)

	c.server = srv
	t.Cleanup(func() { srv.close() })
}

func (c *Config) Send(t *testing.T) {
	t.Helper()

	file, err := os.Create(path.Join(c.dir, "messages.log"))
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	for i := 0; i < c.Count; i++ {
		_, err = fmt.Fprintf(file, "{\"id\":%d,\"message\":\"test\"}\n", i)
		require.NoError(t, err)
	}
}

func (c *Config) Validate(t *testing.T) {
	t.Helper()

	msgs, err := c.server.waitForMessages(c.Count, 10, retryDelay)
	require.NoError(t, err, "timed out waiting for messages from socket output plugin")
	require.Len(t, msgs, c.Count)

	for i, msg := range msgs {
		require.Contains(t, msg, "message", "message #%d is missing 'message' field: %s", i, msg)
	}
}
