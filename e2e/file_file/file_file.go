package file_file

import (
	"fmt"
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

type Config struct {
	FilesDir          string
	FilePattern       string
	OutputNamePattern string
	Count             int
	Lines             int
	RetTime           string
}

func (f *Config) AddConfigSettings(t *testing.T, conf *cfg.Config, pipeName string) {
	f.FilesDir = t.TempDir()
	offsetsDir := t.TempDir()
	input := conf.Pipelines[pipeName].Raw.Get("input")
	input.Set("watching_dir", f.FilesDir)
	input.Set("filename_pattern", f.FilePattern)
	input.Set("offsets_file", filepath.Join(offsetsDir, "offsets.yaml"))

	output := conf.Pipelines[pipeName].Raw.Get("output")
	output.Set("target_file", f.FilesDir+f.OutputNamePattern)
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
				assert.NoError(t, err, "unexpected error")
			}

			file.WriteString(strings.Repeat("{\"first_field\":\"second_field\"}\n", f.Lines))
			_ = file.Close()
			wg.Done()
		}()
	}
	wg.Wait()

	// waiting for files to be processed
	time.Sleep(time.Second * 10)
}

func (f *Config) Validate(t *testing.T) {
	logFilePattern := fmt.Sprintf("%s/%s*%s", f.FilesDir, "file-d", ".log")
	matches := test.GetMatches(t, logFilePattern)
	assert.True(t, len(matches) > 0, "There are no files")
	require.Equal(t, f.Count*f.Lines, test.CountLines(t, logFilePattern))
}
