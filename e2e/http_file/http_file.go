package http_file

import (
	"bytes"
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

var samples = [][]byte{
	[]byte(`{"ok":"google"}`),
	[]byte(`{"ping":"pong"}`),
	[]byte(`{"hello":"world"}`),
}

// Send creates Count http clients and sends Lines of requests from each
func (c *Config) Send(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(c.Count)
	for i := 0; i < c.Count; i++ {
		go func() {
			defer wg.Done()

			cl := http.Client{}
			for j := 0; j < c.Lines; j++ {
				rd := bytes.NewReader(samples[j%len(samples)])
				req, err := http.NewRequest(http.MethodPost, "http://localhost:9200/?login=e2e-test", rd)
				require.NoError(t, err)
				_, err = cl.Do(req)
				require.NoError(t, err)
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
	assert.True(t, len(matches) > 0, "no files with processed events")
	require.Equal(t, c.Count*c.Lines, test.CountLines(t, logFilePattern), "wrong number of processed events")
}
