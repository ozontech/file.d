package file_file

import (
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/test"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// In this test Count files are created and populated in parallel. Lines of messages are written to each of the files.
// They are processed by the pipeline. We wait for the end of processing and fix the number of processed messages.

// Config for file-file plugin e2e test
type Config struct {
	FilesDir string
	Count    int
	Lines    int
	RetTime  string
}

// Configure sets additional fields for input and output plugins
func (c *Config) Configure(t *testing.T, conf *cfg.Config, pipelineName string) {
	c.FilesDir = t.TempDir()
	offsetsDir := t.TempDir()
	input := conf.Pipelines[pipelineName].Raw.Get("input")
	input.Set("watching_dir", c.FilesDir)
	input.Set("filename_pattern", "pod_ns_container-*")
	input.Set("offsets_file", filepath.Join(offsetsDir, "offsets.yaml"))

	output := conf.Pipelines[pipelineName].Raw.Get("output")
	output.Set("target_file", path.Join(c.FilesDir, "file-d.log"))
	output.Set("retention_interval", c.RetTime)
}

// Send creates Count files and writes Lines of lines to each
func (c *Config) Send(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(c.Count)
	for i := 0; i < c.Count; i++ {
		go func() {
			defer wg.Done()
			u1 := strings.ReplaceAll(uuid.NewV4().String(), "-", "")
			u2 := strings.ReplaceAll(uuid.NewV4().String(), "-", "")
			name := path.Join(c.FilesDir, "pod_ns_container-"+u1+u2+".log")
			file, err := os.Create(name)
			if err != nil {
				log.Fatalf("failed to create file: %s", err.Error())
			}
			_, err = file.WriteString(strings.Repeat(`{"key":"value"}`+"\n", c.Lines))
			if err != nil {
				log.Fatalf("failed to write to file: %s", err.Error())
			}
			if err = file.Close(); err != nil {
				log.Fatalf("failed to close file: %s", err.Error())
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
