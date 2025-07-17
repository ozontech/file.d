//go:build linux

package journalctl

import (
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/output/devnull"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
)

func setInput(p *pipeline.Pipeline, in pipeline.InputPlugin, cfg pipeline.AnyConfig) {
	p.SetInput(&pipeline.InputPluginInfo{
		PluginStaticInfo: &pipeline.PluginStaticInfo{
			Config: cfg,
		},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: in,
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

	setInput(p, new(Plugin), config)

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

type SafePlugin struct {
	*Plugin
	safeConfig *SafeConfig
}

type SafeConfig struct {
	*Config

	cursors      map[string]int
	cursorsGuard *sync.Mutex
	commitWG     *sync.WaitGroup
}

func (p *SafePlugin) Start(config pipeline.AnyConfig, params *pipeline.InputPluginParams) {
	p.safeConfig = config.(*SafeConfig)
	p.Plugin.Start(p.safeConfig.Config, params)
}

func (p *SafePlugin) Commit(event *pipeline.Event) {
	p.safeConfig.cursorsGuard.Lock()
	p.safeConfig.cursors[strings.Clone(event.Root.Dig("__CURSOR").AsString())]++
	p.safeConfig.cursorsGuard.Unlock()
	defer p.safeConfig.commitWG.Done()

	p.Plugin.Commit(event)
}

func TestOffsets(t *testing.T) {
	offsetPath := filepath.Join(t.TempDir(), "offset.yaml")

	const (
		lines = 5
		iters = 2
		total = lines * iters
	)

	cursors := make(map[string]int)

	safeConfig := &SafeConfig{
		Config:       &Config{OffsetsFile: offsetPath, MaxLines: lines},
		cursors:      cursors,
		cursorsGuard: new(sync.Mutex),
	}

	test.NewConfig(safeConfig.Config, nil)

	for range iters {
		p := test.NewPipeline(nil, "passive")

		wg := new(sync.WaitGroup)
		wg.Add(lines)

		safeConfig.commitWG = wg

		setInput(p, &SafePlugin{Plugin: &Plugin{}}, safeConfig)
		setOutput(p, func(_ *pipeline.Event) {})

		p.Start()
		wg.Wait()
		p.Stop()
	}

	assert.Equal(t, total, len(cursors))
	for _, cnt := range cursors {
		assert.Equal(t, 1, cnt)
	}
}
