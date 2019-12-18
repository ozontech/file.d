package json_decode

import (
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
	config := &Config{Field: "log", Prefix:"prefix."}
	p.Processors[0].AddActionPlugin(&pipeline.ActionPluginData{Plugin: plugin, PluginDesc: pipeline.PluginDesc{Config: config}})

	anyPlugin, _ = devnull.Factory()
	outputPlugin := anyPlugin.(*devnull.Plugin)
	p.SetOutputPlugin(&pipeline.OutputPluginData{Plugin: outputPlugin, PluginDesc: pipeline.PluginDesc{Config: config}})

	p.Start()

	return p, inputPlugin, outputPlugin
}

func TestDecode(t *testing.T) {
	p, input, output := startPipeline()
	defer p.Stop()

	acceptedEvents := make([]*pipeline.Event, 0, 0)
	input.SetAcceptFn(func(e *pipeline.Event) {
		acceptedEvents = append(acceptedEvents, e)
	})

	dumpedEvents := make([]*pipeline.Event, 0, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		dumpedEvents = append(dumpedEvents, e)
	})

	input.In(0, "test.log", 0, 0, []byte(`{"log":"{\"field2\":\"value2\",\"field3\":\"value3\"}"}`))

	p.Stop()

	assert.Equal(t, 1, len(acceptedEvents), "wrong accepted events count")
	assert.Equal(t, 1, len(dumpedEvents), "wrong dumped events count")
	assert.Equal(t, `{"prefix.field2":"value2","prefix.field3":"value3"}`, dumpedEvents[0].Root.EncodeToString(), "wrong dumped events count")
}
