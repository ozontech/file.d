package file_file_split

import (
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	uuid "github.com/satori/go.uuid"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Config for file-file-split plugin e2e test
type Config struct {
	FilesDir  string
	Count     int
	Lines     int
	BatchSize int
	RetTime   string
}

// Configure sets additional fields for input and output plugins
func (c *Config) Configure(t *testing.T, conf *cfg.Config, pipelineName string) {
	c.FilesDir = t.TempDir()
	offsetsDir := t.TempDir()

	input := conf.Pipelines[pipelineName].Raw.Get("input")
	input.Set("watching_dir", c.FilesDir)
	input.Set("filename_pattern", "split-input-*.log")
	input.Set("offsets_file", filepath.Join(offsetsDir, "offsets.yaml"))

	output := conf.Pipelines[pipelineName].Raw.Get("output")
	output.Set("target_file", path.Join(c.FilesDir, "file-d.log"))
	output.Set("retention_interval", c.RetTime)
}

func (c *Config) Send(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(c.Count)
	for i := 0; i < c.Count; i++ {
		go func() {
			defer wg.Done()
			u := strings.ReplaceAll(uuid.NewV4().String(), "-", "")
			name := path.Join(c.FilesDir, fmt.Sprintf("split-input-%s.log", u))
			file, err := os.Create(name)
			if err != nil {
				log.Fatalf("failed to create file: %s", err.Error())
			}

			var sb strings.Builder
			for j := 0; j < c.Lines; j++ {
				sb.WriteString(buildBatch(j, c.BatchSize))
				sb.WriteByte('\n')
			}

			if _, err = file.WriteString(sb.String()); err != nil {
				log.Fatalf("failed to write to file: %s", err.Error())
			}
			if err = file.Close(); err != nil {
				log.Fatalf("failed to close file: %s", err.Error())
			}
		}()
	}
	wg.Wait()
}

func (c *Config) Validate(t *testing.T) {
	logFilePattern := path.Join(c.FilesDir, "file-d*.log")
	expected := c.Count * c.Lines * c.BatchSize
	test.WaitProcessEvents(t, expected, 3*time.Second, 30*time.Second, logFilePattern)
	matches := test.GetMatches(t, logFilePattern)
	assert.True(t, len(matches) > 0, "no files with processed events")
	require.Equal(t, expected, test.CountLines(t, logFilePattern), "wrong number of processed events after split")
}

func buildBatch(lineIdx, size int) string {
	var sb strings.Builder
	sb.WriteString(`{"data":[`)
	for i := range size {
		if i > 0 {
			sb.WriteByte(',')
		}
		fmt.Fprintf(&sb, `{"m":"line-%d-%d"}`, lineIdx, i)
	}
	sb.WriteString(`]}`)
	return sb.String()
}
