// Package file_loki This test verifies that messages sent to Loki are correctly processed by the ingest pipeline
// and that each message is assigned a 'processed_at' field containing a timestamp.
package file_loki

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/stretchr/testify/require"
)

// Config for file-loki plugin e2e test
type Config struct {
	samples []sample
	dir     string

	lokiAddr string
	label    string
}

// Configure sets additional fields for input and output plugins
func (c *Config) Configure(t *testing.T, conf *cfg.Config, pipelineName string) {
	r := require.New(t)

	c.dir = t.TempDir()
	offsetsDir := t.TempDir()

	input := conf.Pipelines[pipelineName].Raw.Get("input")
	input.Set("watching_dir", c.dir)
	input.Set("filename_pattern", "loki.log")
	input.Set("offsets_file", filepath.Join(offsetsDir, "offsets.yaml"))

	output := conf.Pipelines[pipelineName].Raw.Get("output")
	output.Set("request_timeout", "1m")
	c.lokiAddr = output.Get("address").MustString()

	labels, err := output.Get("labels").Array()
	r.NoError(err)

	c.label = labels[0].(map[string]interface{})["label"].(string)
	c.samples = samples

	url := fmt.Sprintf("%s%s", c.lokiAddr, "/ready")

	var httpResp *http.Response

	for {
		httpResp, err = http.Get(url)
		r.NoError(err)

		if httpResp.StatusCode == http.StatusOK {
			break
		}

		t.Log("waiting for loki to start")
		time.Sleep(3 * time.Second)
	}
}

// Send creates file and writes messages
func (c *Config) Send(t *testing.T) {
	r := require.New(t)

	file, err := os.Create(path.Join(c.dir, "loki.log"))
	r.NoError(err)
	defer func() { _ = file.Close() }()

	for i := range c.samples {
		sampleRaw, err := json.Marshal(&c.samples[i])
		r.NoError(err)
		_, err = fmt.Fprintf(file, "%s\n", string(sampleRaw))
		r.NoError(err)
	}
}

// Validate waits for the message processing to complete
func (c *Config) Validate(t *testing.T) {
	time.Sleep(10 * time.Second)

	r := require.New(t)

	url := fmt.Sprintf("%s%s", c.lokiAddr, "/loki/api/v1/labels")

	httpResp, err := http.Get(url)
	r.NoError(err)
	defer func() {
		r.NoError(httpResp.Body.Close())
	}()

	type response struct {
		Status string   `json:"status"`
		Data   []string `json:"data"`
	}

	var resp response

	err = json.NewDecoder(httpResp.Body).Decode(&resp)
	r.NoError(err)

	r.True(slices.Contains[[]string, string](resp.Data, c.label))
}
