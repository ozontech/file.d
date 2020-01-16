package rename

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.ozon.ru/sre/file-d/pipeline"
	"gitlab.ozon.ru/sre/file-d/test"
)

func TestRename(t *testing.T) {
	config := &Config{"field_1": "renamed_field_1", "field_2": "renamed_field_2", "field_4.field_5": "renamed_field_5"}
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil))
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
