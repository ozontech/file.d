//go:build linux

package journalctl

import (
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/output/devnull"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
	insaneJSON "github.com/vitkovskii/insane-json"
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
	err := cfg.Parse(config, nil)
	assert.NoError(t, err)
	assert.Equal(t, []string{"-f", "-a"}, config.JournalArgs)

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

	const lines = 5

	config := &Config{OffsetsFile: offsetPath, MaxLines: lines}
	err := cfg.Parse(config, nil)
	assert.NoError(t, err)

	cursors := map[string]int{}

	const iters = 2
	const total = lines * iters

	for i := 0; i < iters; i++ {
		p := test.NewPipeline(nil, "passive")

		wg := sync.WaitGroup{}
		wg.Add(lines)

		setInput(p, config)
		setOutput(p, func(event *pipeline.Event) {
			cursors[strings.Clone(event.Root.Dig("__CURSOR").AsString())]++
			wg.Done()
		})

		p.Start()
		wg.Wait()
		p.Stop()
	}

	assert.Equal(t, total, len(cursors))
	for _, cnt := range cursors {
		assert.Equal(t, 1, cnt)
	}
}

func BenchmarkName(b *testing.B) {
	p := Plugin{config: &Config{
		OffsetsFile: path.Join(b.TempDir(), "offsets.yaml"),
	}, offsetsDebouncer: NewDebouncer(time.Millisecond * 200)}
	p.offInfo.Store(&offsetInfo{})

	event := &pipeline.Event{Root: insaneJSON.Spawn()}
	defer insaneJSON.Release(event.Root)

	p.config.PersistenceMode_ = persistenceModeSync
	b.Run("sync", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			p.Commit(event)
		}
	})

	p.config.PersistenceMode_ = persistenceModeAsync
	b.Run("async", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			p.Commit(event)
		}
	})
}
