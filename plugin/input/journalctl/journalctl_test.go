//go:build linux

package journalctl

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/output/devnull"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
)

func setInput(p *pipeline.Pipeline, config *Config, t *testing.T) {
	p.SetInput(&pipeline.InputPluginInfo{
		PluginStaticInfo: &pipeline.PluginStaticInfo{
			Config: config,
		},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: &Plugin{},
		},
	})
}

func setOutput(p *pipeline.Pipeline, out func(event *pipeline.Event)) {
	plugin, config := devnull.Factory()
	outputPlugin := plugin.(*devnull.Plugin)

	p.SetOutput(&pipeline.OutputPluginInfo{
		PluginStaticInfo: &pipeline.PluginStaticInfo{
			Config: config,
		},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: outputPlugin,
		},
	})

	outputPlugin.SetOutFn(out)
}

func TestPipeline(t *testing.T) {
	p := test.NewPipeline(nil, "passive")
	config := &Config{OffsetsFile: filepath.Join(t.TempDir(), "offset.yaml"), MaxLines: 10}
	err := cfg.Parse(config, nil)
	assert.NoError(t, err)
	assert.Equal(t, []string{"-f", "-a"}, config.JournalArgs)

	setInput(p, config, t)

	total := 0
	setOutput(p, func(event *pipeline.Event) {
		assert.Equal(t, int(event.Offset), total)
		total++
	})

	p.Start()
	time.Sleep(time.Millisecond * 300)
	p.Stop()

	assert.Equal(t, 10, total)
}

func TestOffsets(t *testing.T) {
	offsetPath := filepath.Join(t.TempDir(), "offset.yaml")

	config := &Config{OffsetsFile: offsetPath, MaxLines: 5}
	err := cfg.Parse(config, nil)
	assert.NoError(t, err)

	cursors := map[string]int{}

	for i := 0; i < 2; i++ {
		p := test.NewPipeline(nil, "passive")

		setInput(p, config, t)
		setOutput(p, func(event *pipeline.Event) {
			cursors[event.Root.Dig("__CURSOR").AsString()]++
		})

		p.Start()
		time.Sleep(time.Millisecond * 300)
		p.Stop()
	}

	assert.Equal(t, 10, len(cursors))
	for _, cnt := range cursors {
		assert.Equal(t, 1, cnt)
	}
}
