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

	setInput(p, &Plugin{}, config)

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
	/*
		This test falls sometimes because commit of last event of iteration
		doesn't have enough time to finish before pipeline stop.
		So last event duplicates during next iteration
		and total number of unique events is less then required.
		Compare it with TestOffsetsFixed.
	*/

	t.Skip()

	offsetPath := filepath.Join(t.TempDir(), "offset.yaml")

	const (
		lines = 5
		iters = 2
		total = lines * iters
	)

	config := &Config{OffsetsFile: offsetPath, MaxLines: lines}
	test.NewConfig(config, nil)

	cursors := map[string]int{}
	mu := sync.Mutex{}

	for range iters {
		p := test.NewPipeline(nil, "passive")

		wg := sync.WaitGroup{}
		wg.Add(lines)

		setInput(p, &Plugin{}, config)
		setOutput(p, func(event *pipeline.Event) {
			mu.Lock()
			cursors[strings.Clone(event.Root.Dig("__CURSOR").AsString())]++
			mu.Unlock()
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

type SafePlugin struct {
	Plugin
	commitFn func(event *pipeline.Event)
}

func (p *SafePlugin) Commit(event *pipeline.Event) {
	p.Plugin.Commit(event)
	p.commitFn(event)
}

func TestOffsetsFixed(t *testing.T) {
	offsetPath := filepath.Join(t.TempDir(), "offset.yaml")

	const (
		lines = 5
		iters = 2
		total = lines * iters
	)

	config := &Config{OffsetsFile: offsetPath, MaxLines: lines}
	test.NewConfig(config, nil)

	cursors := map[string]int{}
	mu := sync.Mutex{}

	for range iters {
		p := test.NewPipeline(nil, "passive")

		wg := sync.WaitGroup{}
		wg.Add(lines)

		in := &SafePlugin{
			commitFn: func(event *pipeline.Event) {
				mu.Lock()
				cursors[strings.Clone(event.Root.Dig("__CURSOR").AsString())]++
				mu.Unlock()
				wg.Done()
			},
		}

		setInput(p, in, config)
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
