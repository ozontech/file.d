package rename

import (
	"sync"
	"testing"

	"github.com/ozonru/file.d/pipeline"
	"github.com/ozonru/file.d/test"
	"github.com/stretchr/testify/assert"
)

func TestRename(t *testing.T) {
	config := &Config{
		"field_1":         "renamed_field_1",
		"field_2":         "renamed_field_2",
		"field_4.field_5": "renamed_field_5",
		"field_5":         "renamed_field_2",
		"k8s_node_label_topology\\.kubernetes\\.io/zone": "renamed_field.escaped",
		"override": false,
	}
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}
	wg.Add(5)

	outEvents := make([]*pipeline.Event, 0, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e)
		wg.Done()
	})

	input.In(0, "test.log", 0, []byte(`{"field_1":"value_1"}`))
	input.In(0, "test.log", 0, []byte(`{"field_2":"value_2"}`))
	input.In(0, "test.log", 0, []byte(`{"field_3":"value_3"}`))
	input.In(0, "test.log", 0, []byte(`{"field_4":{"field_5":"value_5"}}`))
	input.In(0, "test.log", 0, []byte(`{"k8s_node_label_topology.kubernetes.io/zone":"value_6"}`))

	wg.Wait()
	p.Stop()

	assert.Equal(t, 5, len(outEvents), "wrong out events count")
	assert.Equal(t, "value_1", outEvents[0].Root.Dig("renamed_field_1").AsString(), "wrong field value")
	assert.Equal(t, "value_2", outEvents[1].Root.Dig("renamed_field_2").AsString(), "wrong field value")
	assert.Equal(t, "value_3", outEvents[2].Root.Dig("field_3").AsString(), "wrong field value")
	assert.Equal(t, "value_5", outEvents[3].Root.Dig("renamed_field_5").AsString(), "wrong field value")
	assert.Equal(t, "value_6", outEvents[4].Root.Dig("renamed_field.escaped").AsString(), "wrong field value")
	assert.Nil(t, outEvents[0].Root.Dig("field_1"), "field isn't nil")
	assert.Nil(t, outEvents[1].Root.Dig("field_2"), "field isn't nil")
	assert.Nil(t, outEvents[2].Root.Dig("renamed_field_3"), "field isn't nil")
	assert.Nil(t, outEvents[3].Root.Dig("field_4", "field_5"), "field isn't nil")
	assert.Nil(t, outEvents[4].Root.Dig("k8s_node_label_topology\\.kubernetes\\.io/zone"), "field isn't nil")
}
