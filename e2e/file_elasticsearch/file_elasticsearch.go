package file_elasticsearch

import (
	"fmt"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/stretchr/testify/require"
)

// This test verifies that messages sent to Elasticsearch are correctly processed by the ingest pipeline
// and that each message is assigned a 'processed_at' field containing a timestamp.

// Config for file-elasticsearch plugin e2e test
type Config struct {
	Count    int
	Endpoint string
	Pipeline string
	Username string
	Password string
	dir      string
	index    string
}

// Configure sets additional fields for input and output plugins
func (c *Config) Configure(t *testing.T, conf *cfg.Config, pipelineName string) {
	c.dir = t.TempDir()
	offsetsDir := t.TempDir()

	input := conf.Pipelines[pipelineName].Raw.Get("input")
	input.Set("watching_dir", c.dir)
	input.Set("filename_pattern", "messages.log")
	input.Set("offsets_file", filepath.Join(offsetsDir, "offsets.yaml"))

	output := conf.Pipelines[pipelineName].Raw.Get("output")
	c.index = fmt.Sprintf("my-index-%d", rand.Intn(1000))
	output.Set("index_format", c.index)
	output.Set("ingest_pipeline", c.Pipeline)
	output.Set("username", c.Username)
	output.Set("password", c.Password)
	output.Set("endpoints", []string{c.Endpoint})

	err := createIngestPipeline(c.Endpoint, c.Pipeline, c.Username, c.Password)
	require.NoError(t, err)
}

// Send creates file and writes messages
func (c *Config) Send(t *testing.T) {
	file, err := os.Create(path.Join(c.dir, "messages.log"))
	require.NoError(t, err)
	defer func() { _ = file.Close() }()

	for i := 0; i < c.Count; i++ {
		_, err = file.WriteString("{\"message\":\"test\"}\n")
		require.NoError(t, err)
	}
}

// Validate waits for the message processing to complete
func (c *Config) Validate(t *testing.T) {
	err := waitUntilIndexReady(c.Endpoint, c.index, c.Username, c.Password, c.Count, 10, 250*time.Millisecond)
	require.NoError(t, err)
	docs, err := getDocumentsFromIndex(c.Endpoint, c.index, c.Username, c.Password)
	require.NoError(t, err)
	require.Len(t, docs, c.Count)
	for _, doc := range docs {
		if _, ok := doc["processed_at"]; !ok {
			t.Errorf("doc %v doesn't have processed_at field", doc)
		}
	}
}
