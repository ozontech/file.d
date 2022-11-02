package http_file

import (
	"bytes"
	"log"
	"net/http"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// In this test creates Count clients in parallel. Each client writes Lines of messages to the server.
// They are processed by the pipeline. We wait for the end of processing and fix the number of processed messages.

// Config for http-file plugin e2e test
type Config struct {
	FilesDir string
	Count    int
	Lines    int
	RetTime  string
}

// Configure sets additional fields for input and output plugins
func (c *Config) Configure(t *testing.T, conf *cfg.Config, pipelineName string) {
	c.FilesDir = t.TempDir()
	output := conf.Pipelines[pipelineName].Raw.Get("output")
	output.Set("target_file", path.Join(c.FilesDir, "file-d.log"))
	output.Set("retention_interval", c.RetTime)
}

// Send creates Count http clients and sends Lines of requests from each
func (c *Config) Send(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(c.Count)
	for i := 0; i < c.Count; i++ {
		go func() {
			defer wg.Done()
			cl := http.DefaultClient
			for j := 0; j < c.Lines; j++ {
				rd := bytes.NewReader([]byte(`{"first_field":"second_field"}`))
				req, err := http.NewRequest(http.MethodPost, "http://localhost:9200/", rd)
				assert.Nil(t, err, "bad format http request")
				if _, err = cl.Do(req); err != nil {
					log.Fatalf("failed to make request: %s", err.Error())
				}
			}
		}()
	}
	wg.Wait()
}

// Validate waits for the message processing to complete
func (c *Config) Validate(t *testing.T) {
	logFilePattern := path.Join(c.FilesDir, "file-d*.log")
	test.WaitProcessEvents(t, c.Count*c.Lines, 3*time.Second, 20*time.Second, logFilePattern)
	matches := test.GetMatches(t, logFilePattern)
	assert.True(t, len(matches) > 0, "there are no files")
	require.Equal(t, c.Count*c.Lines, test.CountLines(t, logFilePattern))
}
