package http_file

import (
	"bytes"
	"fmt"
	http2 "net/http"
	"sync"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Struct for http-file plugin e2e test
type Config struct {
	FilesDir          string
	OutputNamePattern string
	Count             int
	Lines             int
	RetTime           string
}

func (h *Config) AddConfigSettings(t *testing.T, conf *cfg.Config, pipeName string) {
	h.FilesDir = t.TempDir()
	output := conf.Pipelines[pipeName].Raw.Get("output")
	output.Set("target_file", h.FilesDir+h.OutputNamePattern)
	output.Set("retention_interval", h.RetTime)
}

func (h *Config) Send(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(h.Count)
	for i := 0; i < h.Count; i++ {
		go func() {
			cl := http2.DefaultClient
			for i := 0; i < h.Lines; i++ {
				rd := bytes.NewReader([]byte(`{"first_field":"second_field"}`))
				req, err := http2.NewRequest(http2.MethodPost, "http://localhost:9200/", rd)
				assert.Nil(t, err, "bad format http request")
				_, err = cl.Do(req)
				if err != nil {
					fmt.Println(err)
				}
			}
			wg.Done()
		}()
	}
	wg.Wait()

	// waiting for files to be processed
	time.Sleep(time.Second * 10)
}

func (h *Config) Validate(t *testing.T) {
	logFilePattern := fmt.Sprintf("%s/%s*%s", h.FilesDir, "file-d", ".log")
	matches := test.GetMatches(t, logFilePattern)
	assert.True(t, len(matches) > 0, "There are no files")
	require.Equal(t, h.Count*h.Lines, test.CountLines(t, logFilePattern))
}
