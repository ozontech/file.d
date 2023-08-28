package join_throttle

import (
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type Config struct {
	inputDir  string
	outputDir string
	Count     int
}

func (c *Config) Configure(t *testing.T, conf *cfg.Config, pipelineName string) {
	c.inputDir = t.TempDir()
	c.outputDir = t.TempDir()
	offsetsDir := t.TempDir()

	input := conf.Pipelines[pipelineName].Raw.Get("input")
	input.Set("watching_dir", c.inputDir)
	input.Set("filename_pattern", "input.log")
	input.Set("offsets_file", filepath.Join(offsetsDir, "offsets.yaml"))

	output := conf.Pipelines[pipelineName].Raw.Get("output")
	output.Set("target_file", path.Join(c.outputDir, "output.log"))
}

func (c *Config) Send(t *testing.T) {
	file, err := os.Create(path.Join(c.inputDir, "input.log"))
	require.NoError(t, err)
	defer func(file *os.File) {
		_ = file.Close()
	}(file)

	for i := 0; i < c.Count; i++ {
		_, err = file.WriteString(`{"message":"start "}` + "\n")
		_ = file.Sync()
		require.NoError(t, err)
	}
}

func (c *Config) Validate(t *testing.T) {
	logFilePattern := path.Join(c.outputDir, "*")

	expectedEvents := 100 // because we are set default_limit: 100 in the throttle plugin

	test.WaitProcessEvents(t, expectedEvents, 3*time.Second, 50*time.Second, logFilePattern)
	matches := test.GetMatches(t, logFilePattern)
	assert.True(t, len(matches) > 0, "no files with processed events")

	got := test.CountLines(t, logFilePattern)

	throttleAccuracy := got >= expectedEvents && got <= expectedEvents*2 // we don't know how many events we will get
	assert.True(t, throttleAccuracy)
}
