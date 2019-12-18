package rename

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
	config := &Config{"field_1": "renamed_field_1", "field_2": "renamed_field_2", "field_4.field_5": "renamed_field_5"}
	p.Processors[0].AddActionPlugin(&pipeline.ActionPluginData{Plugin: plugin, PluginDesc: pipeline.PluginDesc{Config: config}})

	anyPlugin, _ = devnull.Factory()
	outputPlugin := anyPlugin.(*devnull.Plugin)
	p.SetOutputPlugin(&pipeline.OutputPluginData{Plugin: outputPlugin, PluginDesc: pipeline.PluginDesc{Config: config}})

	p.Start()

	return p, inputPlugin, outputPlugin
}

func TestRename(t *testing.T) {
	p, input, output := startPipeline()
	wg := &sync.WaitGroup{}
	wg.Add(4)

	outEvents := make([]*pipeline.Event, 0, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e)
		wg.Done()
	})

	input.In(0, "test.log", 0, 0, []byte(`{"field_1":"value_1"}`))
	input.In(0, "test.log", 0, 0, []byte(`{"field_2":"value_2"}`))
	input.In(0, "test.log", 0, 0, []byte(`{"field_3":"value_3"}`))
	input.In(0, "test.log", 0, 0, []byte(`{"field_4":{"field_5":"value_5"}}`))

	wg.Wait()
	p.Stop()

	assert.Equal(t, 4, len(outEvents), "wrong out events count")
	assert.Equal(t, "value_1", outEvents[0].Root.Dig("renamed_field_1").AsString(), "wrong field value")
	assert.Equal(t, "value_2", outEvents[1].Root.Dig("renamed_field_2").AsString(), "wrong field value")
	assert.Equal(t, "value_3", outEvents[2].Root.Dig("field_3").AsString(), "wrong field value")
	assert.Equal(t, "value_5", outEvents[3].Root.Dig("renamed_field_5").AsString(), "wrong field value")
	assert.Nil(t, outEvents[0].Root.Dig("field_1"), "field isn't nil")
	assert.Nil(t, outEvents[1].Root.Dig("field_2"), "field isn't nil")
	assert.Nil(t, outEvents[2].Root.Dig("renamed_field_3"), "field isn't nil")
	assert.Nil(t, outEvents[3].Root.Dig("field_4", "field_5"), "field isn't nil")
}
