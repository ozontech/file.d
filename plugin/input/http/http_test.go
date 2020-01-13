package http

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.ozon.ru/sre/filed/pipeline"
	"gitlab.ozon.ru/sre/filed/test"
)

func getInputInfo() *pipeline.InputPluginInfo {
	input, _ := Factory()
	return &pipeline.InputPluginInfo{
		PluginStaticInfo: &pipeline.PluginStaticInfo{
			Type:    "",
			Factory: nil,
			Config:  &Config{Address: "off"},
		},
		PluginRuntimeInfo: &pipeline.PluginRuntimeInfo{
			Plugin: input,
			ID:     "",
		},
	}
}

func TestProcessChunksMany(t *testing.T) {
	p, _, output := test.NewPipelineMock(nil, "passive")
	p.SetInput(getInputInfo())
	input := p.GetInput().(*Plugin)
	p.Start()

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
	p.Stop()

	assert.Equal(t, 3, len(outEvents), "wrong events count")
	assert.Equal(t, `{"a":"1"}`, outEvents[0], "wrong event")
	assert.Equal(t, `{"a":"2"}`, outEvents[1], "wrong event")
	assert.Equal(t, `{"a":"3"}`, outEvents[2], "wrong event")
	assert.Equal(t, 0, len(eventBuff), "wrong event")
}

func TestProcessChunksEventBuff(t *testing.T) {
	p, _, output := test.NewPipelineMock(nil, "passive")
	p.SetInput(getInputInfo())
	input := p.GetInput().(*Plugin)
	p.Start()

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
	p.Stop()

	assert.Equal(t, 2, len(outEvents), "wrong out events count")
	assert.Equal(t, `{"a":"1"}`, outEvents[0], "wrong event")
	assert.Equal(t, `{"a":"2"}`, outEvents[1], "wrong event")
	assert.Equal(t, `{"a":"3"}`, string(eventBuff), "wrong event buffer")
}

func TestProcessChunksContinue(t *testing.T) {
	p, _, output := test.NewPipelineMock(nil, "passive")
	p.SetInput(getInputInfo())
	input := p.GetInput().(*Plugin)
	p.Start()

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

	p.Stop()
	wg.Wait()

	assert.Equal(t, 3, len(outEvents), "wrong out events count")
	assert.Equal(t, `{"a":"1"}`, outEvents[0], "wrong event")
	assert.Equal(t, `{"a":"2"}`, outEvents[1], "wrong event")
	assert.Equal(t, `{"a":"3"}`, outEvents[2], "wrong event")
	assert.Equal(t, 0, len(eventBuff), "wrong event")
}

func TestProcessChunksContinueMany(t *testing.T) {
	p, _, output := test.NewPipelineMock(nil, "passive")
	p.SetInput(getInputInfo())
	input := p.GetInput().(*Plugin)
	p.Start()

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
	p.Stop()

	assert.Equal(t, 1, len(outEvents), "wrong events count")
	assert.Equal(t, `{"a":"1"}`, outEvents[0], "wrong event")
	assert.Equal(t, 0, len(eventBuff), "wrong event")
}
