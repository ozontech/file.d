package file

import (
	"os"
	"testing"
	"time"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/input/fake"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func createFile(t *testing.T, fileName string, data *[]byte) *os.File {
	t.Helper()
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.FileMode(0o666))
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

func newPipeline(t *testing.T, configOutput *Config) *pipeline.Pipeline {
	t.Helper()
	settings := &pipeline.Settings{
		Capacity:            4096,
		MaintenanceInterval: time.Second * 10,
		AntispamThreshold:   0,
		AvgEventSize:        2048,
		StreamField:         "stream",
		Decoder:             "json",
		MetricHoldDuration:  pipeline.DefaultMetricHoldDuration,
	}

	p := pipeline.New("test_pipeline", settings, prometheus.NewRegistry(), zap.NewNop())
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

	// output plugin
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
