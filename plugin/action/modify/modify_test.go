package modify

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"gitlab.ozon.ru/sre/filed/pipeline"
	"gitlab.ozon.ru/sre/filed/plugin/input/fake"
	"gitlab.ozon.ru/sre/filed/plugin/output/devnull"
)

func startPipeline() (*pipeline.Pipeline, *fake.Plugin, *devnull.Plugin) {
	p := pipeline.New("modify_pipeline", 2048, 1, prometheus.NewRegistry())

	anyPlugin, _ := fake.Factory()
	inputPlugin := anyPlugin.(*fake.Plugin)
	p.SetInputPlugin(&pipeline.InputPluginData{Plugin: inputPlugin, PluginDesc: pipeline.PluginDesc{Config: fake.Config{}}})

	anyPlugin, _ = factory()
	plugin := anyPlugin.(*Plugin)
	config := &Config{"new_field": "new_value", "field_pattern": "$existing_field"}
	p.Processors[0].AddActionPlugin(&pipeline.ActionPluginData{Plugin: plugin, PluginDesc: pipeline.PluginDesc{Config: config}})

	anyPlugin, _ = devnull.Factory()
	outputPlugin := anyPlugin.(*devnull.Plugin)
	p.SetOutputPlugin(&pipeline.OutputPluginData{Plugin: outputPlugin, PluginDesc: pipeline.PluginDesc{Config: config}})

	p.Start()

	return p, inputPlugin, outputPlugin
}

func TestModify(t *testing.T) {
	p, input, output := startPipeline()
	defer p.Stop()

	dumpedEvents := make([]*pipeline.Event, 0, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		dumpedEvents = append(dumpedEvents, e)
	})

	input.In(0, "test.log", 0, 0, []byte(`{"existing_field":"existing_value"}`))
	input.Wait()

	assert.Equal(t, 1, len(dumpedEvents), "wrong accepted events count")
	assert.Equal(t, "new_value", dumpedEvents[0].Root.Dig("new_field").AsString(), "wrong field value")
	assert.Equal(t, "existing_value", dumpedEvents[0].Root.Dig("field_pattern").AsString(), "wrong field value")
}
