package http

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.ozon.ru/sre/filed/pipeline"
	"gitlab.ozon.ru/sre/filed/plugin/output/devnull"
)

func startPipeline(persistenceMode string, enableEventLog bool, config *Config) (*pipeline.Pipeline, *Plugin, *devnull.Plugin) {
	p := pipeline.NewTestPipeLine(true)
	if enableEventLog {
		p.EnableEventLog()
	}

	if config == nil {
		config = &Config{Address: "off"}
	}
	anyPlugin, _ := Factory()
	inputPlugin := anyPlugin.(*Plugin)
	p.SetInputPlugin(&pipeline.InputPluginData{Plugin: inputPlugin, PluginDesc: pipeline.PluginDesc{Config: config}})

	anyPlugin, _ = devnull.Factory()
	outputPlugin := anyPlugin.(*devnull.Plugin)
	p.SetOutputPlugin(&pipeline.OutputPluginData{Plugin: outputPlugin, PluginDesc: pipeline.PluginDesc{Config: config}})

	p.Start()

	return p, inputPlugin, outputPlugin
}

func TestProcessChunksMany(t *testing.T) {
	pipe, input, output := startPipeline("async", true, nil)
	wg := &sync.WaitGroup{}
	wg.Add(3)

	outEvents := make([]string, 0, 0)
	output.SetOutFn(func(event *pipeline.Event) {
		outEvents = append(outEvents, event.Root.EncodeToString())
		wg.Done()
	})

	chunk := []byte(`{"a":"1"}
{"a":"2"}
{"a":"3"}
`)
	eventBuff := make([]byte, 0, 0)
	eventBuff = input.processChunk(0, chunk, eventBuff)

	wg.Wait()
	pipe.Stop()

	assert.Equal(t, 3, len(outEvents), "wrong events count")
	assert.Equal(t, `{"a":"1"}`, outEvents[0], "wrong event")
	assert.Equal(t, `{"a":"2"}`, outEvents[1], "wrong event")
	assert.Equal(t, `{"a":"3"}`, outEvents[2], "wrong event")
	assert.Equal(t, 0, len(eventBuff), "wrong event")
}

func TestProcessChunksEventBuff(t *testing.T) {
	pipe, input, output := startPipeline("async", true, nil)
	wg := &sync.WaitGroup{}
	wg.Add(2)

	outEvents := make([]string, 0, 0)
	output.SetOutFn(func(event *pipeline.Event) {
		outEvents = append(outEvents, event.Root.EncodeToString())
		wg.Done()
	})

	chunk := []byte(`{"a":"1"}
{"a":"2"}
{"a":"3"}`)
	eventBuff := make([]byte, 0, 0)
	eventBuff = input.processChunk(0, chunk, eventBuff)

	wg.Wait()
	pipe.Stop()

	assert.Equal(t, 2, len(outEvents), "wrong out events count")
	assert.Equal(t, `{"a":"1"}`, outEvents[0], "wrong event")
	assert.Equal(t, `{"a":"2"}`, outEvents[1], "wrong event")
	assert.Equal(t, `{"a":"3"}`, string(eventBuff), "wrong event buffer")
}

func TestProcessChunksContinue(t *testing.T) {
	pipe, input, output := startPipeline("async", true, nil)
	wg := &sync.WaitGroup{}
	wg.Add(3)

	outEvents := make([]string, 0, 0)
	output.SetOutFn(func(event *pipeline.Event) {
		outEvents = append(outEvents, event.Root.EncodeToString())
		wg.Done()
	})

	chunk := []byte(`"1"}
{"a":"2"}
{"a":"3"}
`)
	eventBuff := []byte(`{"a":`)
	eventBuff = input.processChunk(0, chunk, eventBuff)

	pipe.Stop()
	wg.Wait()

	assert.Equal(t, 3, len(outEvents), "wrong out events count")
	assert.Equal(t, `{"a":"1"}`, outEvents[0], "wrong event")
	assert.Equal(t, `{"a":"2"}`, outEvents[1], "wrong event")
	assert.Equal(t, `{"a":"3"}`, outEvents[2], "wrong event")
	assert.Equal(t, 0, len(eventBuff), "wrong event")
}

func TestProcessChunksContinueMany(t *testing.T) {
	pipe, input, output := startPipeline("async", true, nil)
	wg := &sync.WaitGroup{}
	wg.Add(1)

	outEvents := make([]string, 0, 0)
	output.SetOutFn(func(event *pipeline.Event) {
		outEvents = append(outEvents, event.Root.EncodeToString())
		wg.Done()
	})

	eventBuff := []byte(``)

	eventBuff = input.processChunk(0, []byte(`{`), eventBuff)
	eventBuff = input.processChunk(0, []byte(`"a"`), eventBuff)
	eventBuff = input.processChunk(0, []byte(`:`), eventBuff)
	eventBuff = input.processChunk(0, []byte(`"1"}`+"\n"), eventBuff)

	wg.Wait()
	pipe.Stop()

	assert.Equal(t, 1, len(outEvents), "wrong events count")
	assert.Equal(t, `{"a":"1"}`, outEvents[0], "wrong event")
	assert.Equal(t, 0, len(eventBuff), "wrong event")
}
