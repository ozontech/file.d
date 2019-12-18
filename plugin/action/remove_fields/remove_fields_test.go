package remove_fields

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.ozon.ru/sre/filed/pipeline"
	"gitlab.ozon.ru/sre/filed/plugin/input/fake"
	"gitlab.ozon.ru/sre/filed/plugin/output/devnull"
)

func startPipeline() (*pipeline.Pipeline, *fake.Plugin, *devnull.Plugin) {
	p := pipeline.NewTestPipeLine(false)

	anyPlugin, _ := fake.Factory()
	inputPlugin := anyPlugin.(*fake.Plugin)
	p.SetInputPlugin(&pipeline.InputPluginData{Plugin: inputPlugin, PluginDesc: pipeline.PluginDesc{Config: fake.Config{}}})

	anyPlugin, _ = factory()
	plugin := anyPlugin.(*Plugin)
	config := &Config{Fields: []string{"field_1", "field_2"}}
	p.Processors[0].AddActionPlugin(&pipeline.ActionPluginData{Plugin: plugin, PluginDesc: pipeline.PluginDesc{Config: config}})

	anyPlugin, _ = devnull.Factory()
	outputPlugin := anyPlugin.(*devnull.Plugin)
	p.SetOutputPlugin(&pipeline.OutputPluginData{Plugin: outputPlugin, PluginDesc: pipeline.PluginDesc{Config: config}})

	p.Start()

	return p, inputPlugin, outputPlugin
}

func TestRemoveFields(t *testing.T) {
	p, input, output := startPipeline()
	wg := &sync.WaitGroup{}
	wg.Add(3)

	outEvents := make([]*pipeline.Event, 0, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e)
		wg.Done()
	})

	input.In(0, "test.log", 0, 0, []byte(`{"field_1":"value_1","a":"b"}`))
	input.In(0, "test.log", 0, 0, []byte(`{"field_2":"value_2","b":"c"}`))
	input.In(0, "test.log", 0, 0, []byte(`{"field_3":"value_3","a":"b"}`))

	wg.Wait()
	p.Stop()

	assert.Equal(t, 3, len(outEvents), "wrong out events count")
	assert.Equal(t, `{"a":"b"}`, outEvents[0].Root.EncodeToString(), "wrong event")
	assert.Equal(t, `{"b":"c"}`, outEvents[1].Root.EncodeToString(), "wrong event")
	assert.Equal(t, `{"field_3":"value_3","a":"b"}`, outEvents[2].Root.EncodeToString(), "wrong event")
}
