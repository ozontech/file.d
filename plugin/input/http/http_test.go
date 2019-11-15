package http

import (
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
	c, p, o := startPipeline("async", true, nil)
	defer c.Stop()

	events := make([]string, 0, 0)
	o.SetOutFn(func(event *pipeline.Event) {
		events = append(events, event.Root.EncodeToString())
	})

	chunk := []byte(`{"a":"1"}
{"a":"2"}
{"a":"3"}
`)
	eventBuff := make([]byte, 0, 0)
	eventBuff = p.processChunk(chunk, eventBuff)

	c.HandleEventFlowFinish(false)
	c.WaitUntilDone(false)

	assert.Equal(t, 3, len(events), "wrong events count")
	assert.Equal(t, `{"a":"1"}`, events[0], "wrong event")
	assert.Equal(t, `{"a":"2"}`, events[1], "wrong event")
	assert.Equal(t, `{"a":"3"}`, events[2], "wrong event")
	assert.Equal(t, 0, len(eventBuff), "wrong event")
}

func TestProcessChunksEventBuff(t *testing.T) {
	c, p, o := startPipeline("async", true, nil)
	defer c.Stop()

	events := make([]string, 0, 0)
	o.SetOutFn(func(event *pipeline.Event) {
		events = append(events, event.Root.EncodeToString())
	})

	chunk := []byte(`{"a":"1"}
{"a":"2"}
{"a":"3"}`)
	eventBuff := make([]byte, 0, 0)
	eventBuff = p.processChunk(chunk, eventBuff)

	c.HandleEventFlowFinish(false)
	c.WaitUntilDone(false)

	assert.Equal(t, 2, len(events), "wrong events count")
	assert.Equal(t, `{"a":"1"}`, events[0], "wrong event")
	assert.Equal(t, `{"a":"2"}`, events[1], "wrong event")
	assert.Equal(t, `{"a":"3"}`, string(eventBuff), "wrong event buffer")
}

func TestProcessChunksContinue(t *testing.T) {
	c, p, o := startPipeline("async", true, nil)
	defer c.Stop()

	events := make([]string, 0, 0)
	o.SetOutFn(func(event *pipeline.Event) {
		events = append(events, event.Root.EncodeToString())
	})

	chunk := []byte(`"1"}
{"a":"2"}
{"a":"3"}
`)
	eventBuff := []byte(`{"a":`)
	eventBuff = p.processChunk(chunk, eventBuff)

	c.HandleEventFlowFinish(false)
	c.WaitUntilDone(false)

	assert.Equal(t, 3, len(events), "wrong events count")
	assert.Equal(t, `{"a":"1"}`, events[0], "wrong event")
	assert.Equal(t, `{"a":"2"}`, events[1], "wrong event")
	assert.Equal(t, `{"a":"3"}`, events[2], "wrong event")
	assert.Equal(t, 0, len(eventBuff), "wrong event")
}

func TestProcessChunksContinueMany(t *testing.T) {
	c, p, o := startPipeline("async", true, nil)
	defer c.Stop()

	events := make([]string, 0, 0)
	o.SetOutFn(func(event *pipeline.Event) {
		events = append(events, event.Root.EncodeToString())
	})

	eventBuff := []byte(``)

	eventBuff = p.processChunk([]byte(`{`), eventBuff)
	eventBuff = p.processChunk([]byte(`"a"`), eventBuff)
	eventBuff = p.processChunk([]byte(`:`), eventBuff)
	eventBuff = p.processChunk([]byte(`"1"}`+"\n"), eventBuff)

	c.HandleEventFlowFinish(false)
	c.WaitUntilDone(false)

	assert.Equal(t, 1, len(events), "wrong events count")
	assert.Equal(t, `{"a":"1"}`, events[0], "wrong event")
	assert.Equal(t, 0, len(eventBuff), "wrong event")
}
