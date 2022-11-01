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

// Config for http-file plugin e2e test
type Config struct {
	FilesDir string
	Count    int
	Lines    int
	RetTime  string
}

func (h *Config) Configure(t *testing.T, conf *cfg.Config, pipelineName string) {
	h.FilesDir = t.TempDir()
	output := conf.Pipelines[pipelineName].Raw.Get("output")
	output.Set("target_file", path.Join(h.FilesDir, "file-d.log"))
	output.Set("retention_interval", h.RetTime)
}

func (h *Config) Send(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(h.Count)
	for i := 0; i < h.Count; i++ {
		go func() {
			cl := http.DefaultClient
			for i := 0; i < h.Lines; i++ {
				rd := bytes.NewReader([]byte(`{"first_field":"second_field"}`))
				req, err := http.NewRequest(http.MethodPost, "http://localhost:9200/", rd)
				assert.Nil(t, err, "bad format http request")
				_, err = cl.Do(req)
				if err != nil {
					log.Fatalf("failed to make request: %s", err.Error())
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func (h *Config) Validate(t *testing.T) {
	logFilePattern := path.Join(h.FilesDir, "file-d*.log")
	test.WaitProcessEvents(t, h.Count*h.Lines, time.Second, 10*time.Second, logFilePattern)
	matches := test.GetMatches(t, logFilePattern)
	assert.True(t, len(matches) > 0, "There are no files")
	require.Equal(t, h.Count*h.Lines, test.CountLines(t, logFilePattern))
}
