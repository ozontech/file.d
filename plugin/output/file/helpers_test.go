package file

import (
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ozonru/file.d/pipeline"
	"github.com/ozonru/file.d/plugin/input/fake"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
)

func createFile(t *testing.T, fileName string, data *[]byte) *os.File {
	t.Helper()
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.FileMode(0666))
	if err != nil {
		t.Fatalf("could not open or create file, error: %s", err.Error())
	}
	if data != nil {
		if _, err := file.Write(*data); err != nil {
			t.Fatalf("could not write cintent into the file, err: %s", err.Error())
		}
	}
	return file
}

func createDir(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		t.Fatalf("could not create target dir: %s, error: %s", dir, err.Error())
	}
}

func clearDir(t *testing.T, dir string) {
	t.Helper()
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("coudl not delete dirs and files adter tests, error: %s", err.Error())
	}
}

func getMatches(t *testing.T, pattern string) []string {
	t.Helper()
	matches, err := filepath.Glob(pattern)
	assert.NoError(t, err)
	return matches
}

func checkDirFiles(t *testing.T, matches []string, totalSent int64, msg string) {
	t.Helper()
	totalSize := int64(0)
	for _, m := range matches {

		info, err := os.Stat(m)
		assert.NoError(t, err, msg)
		assert.NotNil(t, info, msg)

		totalSize += info.Size()
	}
	assert.Equal(t, totalSent, totalSize, msg)
}

func sendPack(t *testing.T, p *pipeline.Pipeline, msgs []msg) int64 {
	t.Helper()
	var sent int64 = 0
	for _, m := range msgs {
		p.In(0, "test", 0, m, false)
		//count \n
		sent += int64(len(m)) + 1
	}
	return sent
}

func newPipeline(t *testing.T, configOutput *Config) *pipeline.Pipeline {
	t.Helper()
	settings := &pipeline.Settings{
		Capacity:            4096,
		MaintenanceInterval: time.Second * 100000,
		AntispamThreshold:   0,
		AvgLogSize:          2048,
		StreamField:         "stream",
		Decoder:             "json",
	}

	http.DefaultServeMux = &http.ServeMux{}
	p := pipeline.New("test_pipeline", settings, prometheus.NewRegistry(), http.DefaultServeMux)
	p.DisableParallelism()
	p.EnableEventLog()

	anyPlugin, _ := fake.Factory()
	inputPlugin := anyPlugin.(*fake.Plugin)
	p.SetInput(&pipeline.InputPluginInfo{
		PluginStaticInfo: &pipeline.PluginStaticInfo{
			Type: "fake",
		},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: inputPlugin,
		},
	})

	//output plugin
	anyPlugin, _ = Factory()
	outputPlugin := anyPlugin.(*Plugin)
	p.SetOutput(&pipeline.OutputPluginInfo{
		PluginStaticInfo: &pipeline.PluginStaticInfo{
			Type:   "file",
			Config: configOutput,
		},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: outputPlugin,
		},
	},
	)
	return p
}

func checkZero(t *testing.T, target string, msg string) int64 {
	t.Helper()
	info, err := os.Stat(target)
	assert.NoErrorf(t, err, "there is no target: %s", target, msg)
	assert.NotNil(t, info, msg)
	assert.Zero(t, info.Size(), msg)
	return info.Size()
}

func checkNotZero(t *testing.T, target string, msg string) int64 {
	t.Helper()
	info, err := os.Stat(target)
	assert.NoError(t, err, msg)
	assert.NotNil(t, info, msg)
	assert.NotZero(t, info.Size(), msg)
	return info.Size()
}
