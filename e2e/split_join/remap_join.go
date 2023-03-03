package split_join

import (
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/require"
)

type Config struct {
	inputDir  string
	outputDir string
	count     int
}

func (c *Config) Configure(t *testing.T, conf *cfg.Config, pipelineName string) {
	c.count = 100
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
	defer file.Close()

	for i := 0; i < c.count; i++ {
		_, err = file.WriteString(`{ "data": [ { "hello": "world" }, { "message": "start " }, { "message": "continue" }, { "file": ".d" }, { "open": "source" } ] }` + "\n")
		_ = file.Sync()
		require.NoError(t, err)
	}
}

func (c *Config) Validate(t *testing.T) {
	logFilePattern := path.Join(c.outputDir, "*")

	expectedEvents := c.count * 4

	test.WaitProcessEvents(t, expectedEvents, 50*time.Millisecond, 50*time.Second, logFilePattern)
	got := test.CountLines(t, logFilePattern)

	files := test.GetMatches(t, logFilePattern)

	require.Equal(t, 1, len(files))
	outputFile := files[0]
	outputFileContent, err := os.ReadFile(outputFile)
	require.NoError(t, err)
	require.Equal(t, strings.Repeat(`{"hello":"world","k8s_pod_label_app":"splitter"}`+"\n"+
		`{"message":"start continue","k8s_pod_label_app":"splitter"}`+"\n"+
		`{"file":".d","k8s_pod_label_app":"splitter"}`+"\n"+
		`{"open":"source","k8s_pod_label_app":"splitter"}`+"\n",
		c.count), string(outputFileContent))

	require.Equal(t, expectedEvents, got)
}
