package keep_fields

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"gitlab.ozon.ru/sre/filed/pipeline"
	"gitlab.ozon.ru/sre/filed/plugin/input/fake"
	"gitlab.ozon.ru/sre/filed/plugin/output/devnull"
)

func startPipeline() (*pipeline.Pipeline, *fake.Plugin, *devnull.Plugin) {
	p := pipeline.New("rename_pipeline", 1, 0, prometheus.NewRegistry())

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

func TestKeepFields(t *testing.T) {
	p, input, output := startPipeline()
	defer p.Stop()

	dumpedEvents := make([]*pipeline.Event, 0, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		dumpedEvents = append(dumpedEvents, e)
	})

	input.In(0, "test.log", 0, 0, []byte(`{"field_1":"value_1","a":"b"}`))
	input.In(0, "test.log", 0, 0, []byte(`{"field_2":"value_2","b":"c"}`))
	input.In(0, "test.log", 0, 0, []byte(`{"field_3":"value_3","a":"b"}`))
	input.Wait()

	assert.Equal(t, 3, len(dumpedEvents), "wrong accepted events count")
	assert.Equal(t, `{"field_1":"value_1"}`, string(dumpedEvents[0].JSON.MarshalTo(nil)), "wrong event")
	assert.Equal(t, `{"field_2":"value_2"}`, string(dumpedEvents[1].JSON.MarshalTo(nil)), "wrong event")
	assert.Equal(t, `{}`, string(dumpedEvents[2].JSON.MarshalTo(nil)), "wrong event")
}
