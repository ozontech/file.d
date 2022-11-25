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

const panicExample = `{"log": "panic: assignment to entry in nil map"}
{"log": ""}
{"log": "goroutine 1 [running]:"}
{"log": "example.com/tariffication/tarifficatorGoApi/services/cache.(*Cache).getGeoRules(0xc420438780, 0xef36b8, 0xc42bb7e600, 0xc42bb77ce0, 0x0, 0x0)"}
{"log": "	/builds/tariffication/tarifficatorGoApi/services/cache/index.go:69 +0x538"}
{"log": "example.com/tariffication/tarifficatorGoApi/services/cache.(*Cache).createAddressIndex(0xc420438780, 0x0, 0x0)"}
{"log": "	/builds/tariffication/tarifficatorGoApi/services/cache/index.go:166 +0x5e"}
{"log": "example.com/tariffication/tarifficatorGoApi/services/cache.(*Cache).createIndexes(0xc420438780, 0x0, 0xc44ec607d0)"}
{"log": "	/builds/tariffication/tarifficatorGoApi/services/cache/index.go:211 +0x8a"}
{"log": "example.com/tariffication/tarifficatorGoApi/services/cache.(*Cache).updateDbCache(0xc420438780, 0xc420438780, 0xc4200845c0)"}
{"log": "	/builds/tariffication/tarifficatorGoApi/services/cache/cache.go:84 +0x182"}
{"log": "example.com/tariffication/tarifficatorGoApi/services/cache.NewCache(0xc4200985f0, 0xc4203fdad0, 0xc420084440, 0x0, 0x0, 0x0)"}
{"log": "	/builds/tariffication/tarifficatorGoApi/services/cache/cache.go:66 +0xa7"}
{"log": "main.initialize(0xec3270, 0x1d, 0xee7133, 0x9e, 0xec53d7, 0x1f, 0xc40000000a, 0x14, 0xc420086e00)"}
{"log": "	/builds/tariffication/tarifficatorGoApi/cmd/tarifficator/main.go:41 +0x389"}
{"log": "main.main()"}
{"log": "	/builds/tariffication/tarifficatorGoApi/cmd/tarifficator/main.go:65 +0x2ae"}
`

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
	wg.Add(c.Count * 2)
	for i := 0; i < c.Count; i++ {
		go func() {
			defer wg.Done()
			u1 := strings.ReplaceAll(uuid.NewV4().String(), "-", "")
			u2 := strings.ReplaceAll(uuid.NewV4().String(), "-", "")
			name := path.Join(c.FilesDir, fmt.Sprintf("pod_ns_container-%s%s.log", u1, u2))
			file, err := os.Create(name)
			assert.NoError(t, err)
			defer file.Close()

			_, err = file.WriteString(strings.Repeat(`{"key":"value"}`+"\n", c.Lines))
			assert.NoError(t, err)

			_, err = file.WriteString(panicExample)
			assert.NoError(t, err)
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

	logsCount := c.Count * c.Lines                                // how many {"key":"value"} repeats
	logsCount += c.Count * len(strings.Split(panicExample, "\n")) // how many panic repeats

	assert.Equal(t, logsCount, test.CountLines(t, logFilePattern), "wrong number of processed events")
}
