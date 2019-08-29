package discard

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.ozon.ru/sre/filed/pipeline"
	"gitlab.ozon.ru/sre/filed/plugin/input/inputfake"
	"gitlab.ozon.ru/sre/filed/plugin/outputdevnull"
)

func startPipeline(conds pipeline.MatchConditions, condMode pipeline.MatchMode) (*pipeline.Pipeline, *fake.FakePlugin, *devnull.DevNullPlugin) {
	p := pipeline.New("k8s_pipeline", 16, 1)

	anyPlugin, _ := fake.Factory()
	inputPlugin := anyPlugin.(*fake.FakePlugin)
	p.SetInputPlugin(&pipeline.InputPluginDescription{Plugin: inputPlugin, Config: fake.Config{}})

	anyPlugin, _ = factory()
	plugin := anyPlugin.(*DiscardPlugin)
	config := &Config{}
	p.Tracks[0].AddActionPlugin(&pipeline.ActionPluginDescription{Plugin: plugin, Config: config, MatchConditions: conds, MatchMode: condMode})

	anyPlugin, _ = devnull.Factory()
	outputPlugin := anyPlugin.(*devnull.DevNullPlugin)
	p.SetOutputPlugin(&pipeline.OutputPluginDescription{Plugin: outputPlugin, Config: config})

	p.Start()

	return p, inputPlugin, outputPlugin
}

func TestDiscardAnd(t *testing.T) {
	conds := pipeline.MatchConditions{
		pipeline.MatchCondition{
			Field: "field1",
			Value: "value1",
		},
		pipeline.MatchCondition{
			Field: "field2",
			Value: "value2",
		},
	}

	p, input, output := startPipeline(conds, pipeline.ModeAnd)
	defer p.Stop()

	acceptedEvents := make([]*pipeline.Event, 0, 0)
	input.SetAcceptFn(func(e *pipeline.Event) {
		acceptedEvents = append(acceptedEvents, e)
	})

	dumpedEvents := make([]*pipeline.Event, 0, 0)
	output.SetDumpFn(func(e *pipeline.Event) {
		dumpedEvents = append(dumpedEvents, e)
	})

	input.Accept(0, "test.log", 0, 0, []byte(`{"field1":"not_value1"}`))
	input.Accept(0, "test.log", 0, 0, []byte(`{"field2":"not_value2"}`))
	input.Accept(0, "test.log", 0, 0, []byte(`{"field1":"value1"}`))
	input.Accept(0, "test.log", 0, 0, []byte(`{"field2":"value2"}`))
	input.Accept(0, "test.log", 0, 0, []byte(`{"field1":"value1","field2":"value2"}`))
	input.Accept(0, "test.log", 0, 0, []byte(`{"field3":"value3","field1":"value1","field2":"value2"}`))
	input.Wait()

	assert.Equal(t, 6, len(acceptedEvents), "wrong accepted events count")
	assert.Equal(t, 4, len(dumpedEvents), "wrong dumped events count")
}

func TestDiscardOr(t *testing.T) {
	conds := pipeline.MatchConditions{
		pipeline.MatchCondition{
			Field: "field1",
			Value: "value1",
		},
		pipeline.MatchCondition{
			Field: "field2",
			Value: "value2",
		},
	}

	p, input, output := startPipeline(conds, pipeline.ModeOr)
	defer p.Stop()

	acceptedEvents := make([]*pipeline.Event, 0, 0)
	input.SetAcceptFn(func(e *pipeline.Event) {
		acceptedEvents = append(acceptedEvents, e)
	})

	dumpedEvents := make([]*pipeline.Event, 0, 0)
	output.SetDumpFn(func(e *pipeline.Event) {
		dumpedEvents = append(dumpedEvents, e)
	})

	input.Accept(0, "test.log", 0, 0, []byte(`{"field1":"not_value1"}`))
	input.Accept(0, "test.log", 0, 0, []byte(`{"field2":"not_value2"}`))
	input.Accept(0, "test.log", 0, 0, []byte(`{"field1":"value1"}`))
	input.Accept(0, "test.log", 0, 0, []byte(`{"field2":"value2"}`))
	input.Accept(0, "test.log", 0, 0, []byte(`{"field1":"value1","field2":"value2"}`))
	input.Accept(0, "test.log", 0, 0, []byte(`{"field3":"value3","field1":"value1","field2":"value2"}`))
	input.Wait()

	assert.Equal(t, 6, len(acceptedEvents), "wrong accepted events count")
	assert.Equal(t, 2, len(dumpedEvents), "wrong dumped events count")
}

func TestDiscardRegex(t *testing.T) {
	conds := pipeline.MatchConditions{
		pipeline.MatchCondition{
			Field:  "field1",
			Regexp: regexp.MustCompile("(one|two|three)"),
		},
		pipeline.MatchCondition{
			Field:  "field2",
			Regexp: regexp.MustCompile("four"),
		},
	}

	p, input, output := startPipeline(conds, pipeline.ModeOr)
	defer p.Stop()

	acceptedEvents := make([]*pipeline.Event, 0, 0)
	input.SetAcceptFn(func(e *pipeline.Event) {
		acceptedEvents = append(acceptedEvents, e)
	})

	dumpedEvents := make([]*pipeline.Event, 0, 0)
	output.SetDumpFn(func(e *pipeline.Event) {
		dumpedEvents = append(dumpedEvents, e)
	})

	input.Accept(0, "test.log", 0, 0, []byte(`{"field1":"0000 one 0000"}`))
	input.Accept(0, "test.log", 0, 0, []byte(`{"field2":"0000 one 0000"}`))
	input.Accept(0, "test.log", 0, 0, []byte(`{"field1":". two ."}`))
	input.Accept(0, "test.log", 0, 0, []byte(`{"field1":"four"}`))
	input.Accept(0, "test.log", 0, 0, []byte(`{"field2":"... four ....","field2":"value2"}`))
	input.Accept(0, "test.log", 0, 0, []byte(`{"field3":"value3","field1":"value1","field2":"value2"}`))
	input.Wait()

	assert.Equal(t, 6, len(acceptedEvents), "wrong accepted events count")
	assert.Equal(t, 3, len(dumpedEvents), "wrong dumped events count")
}
