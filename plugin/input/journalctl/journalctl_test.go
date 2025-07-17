//go:build linux

package journalctl

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/output/devnull"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
)

func setInput(p *pipeline.Pipeline, config *Config) {
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
	const lines = 10
	config := &Config{OffsetsFile: filepath.Join(t.TempDir(), "offset.yaml"), MaxLines: lines}
	test.NewConfig(config, nil)
	assert.Equal(t, []string{"-f"}, config.JournalArgs)

	setInput(p, config)

	total := 0
	wg := sync.WaitGroup{}
	wg.Add(lines)
	setOutput(p, func(event *pipeline.Event) {
		assert.Equal(t, int(event.Offset), total)
		total++
		if total > lines {
			t.Fatal("'total' more than lines")
		}
		wg.Done()
	})

	p.Start()
	wg.Wait()
	p.Stop()

	assert.Equal(t, 10, total)
}

func TestOffsets(t *testing.T) {
	offsetPath := filepath.Join(t.TempDir(), "offset.yaml")

	const (
		lines = 5
		iters = 2
		total = lines * iters
	)

	config := &Config{OffsetsFile: offsetPath, MaxLines: lines}
	test.NewConfig(config, nil)

	cursors := map[string]int{}
	cursorsGuard := new(sync.Mutex)

	for range iters {
		p := test.NewPipeline(nil, "passive")

		wg := new(sync.WaitGroup)
		wg.Add(lines)

		config.cursors = cursors
		config.cursorsGuard = cursorsGuard
		config.wg = wg

		setInput(p, config)
		setOutput(p, func(event *pipeline.Event) {})

		p.Start()
		wg.Wait()
		p.Stop()
	}

	assert.Equal(t, total, len(cursors))
	for _, cnt := range cursors {
		assert.Equal(t, 1, cnt)
	}
}
