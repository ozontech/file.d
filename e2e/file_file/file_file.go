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

// Config for file-file plugin e2e test
type Config struct {
	FilesDir string
	Count    int
	Lines    int
	RetTime  string
}

func (f *Config) Configure(t *testing.T, conf *cfg.Config, pipelineName string) {
	f.FilesDir = t.TempDir()
	offsetsDir := t.TempDir()
	input := conf.Pipelines[pipelineName].Raw.Get("input")
	input.Set("watching_dir", f.FilesDir)
	input.Set("filename_pattern", "pod_ns_container-*")
	input.Set("offsets_file", filepath.Join(offsetsDir, "offsets.yaml"))

	output := conf.Pipelines[pipelineName].Raw.Get("output")
	output.Set("target_file", path.Join(f.FilesDir, "file-d.log"))
	output.Set("retention_interval", f.RetTime)
}

func (f *Config) Send(t *testing.T) {
	wg := &sync.WaitGroup{}
	wg.Add(f.Count)
	for i := 0; i < f.Count; i++ {
		go func() {
			u1 := strings.ReplaceAll(uuid.NewV4().String(), "-", "")
			u2 := strings.ReplaceAll(uuid.NewV4().String(), "-", "")
			name := path.Join(f.FilesDir, "pod_ns_container-"+u1+u2+".log")
			file, err := os.Create(name)
			if err != nil {
				log.Fatalf("failed to create file: %s", err.Error())
			}
			_, err = file.WriteString(strings.Repeat(`{"key":"value"}`+"\n", f.Lines))
			if err != nil {
				log.Fatalf("failed to write to file: %s", err.Error())
			}
			err = file.Close()
			if err != nil {
				log.Fatalf("failed to close file: %s", err.Error())
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func (f *Config) Validate(t *testing.T) {
	logFilePattern := path.Join(f.FilesDir, "file-d*.log")
	test.WaitProcessEvents(t, f.Count*f.Lines, time.Second, 10*time.Second, logFilePattern)
	matches := test.GetMatches(t, logFilePattern)
	assert.True(t, len(matches) > 0, "There are no files")
	require.Equal(t, f.Count*f.Lines, test.CountLines(t, logFilePattern))
}
