package modify

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.ozon.ru/sre/file-d/pipeline"
	"gitlab.ozon.ru/sre/file-d/test"
)

func TestModify(t *testing.T) {
	config := test.NewConfig(&Config{"new_field": "new_value", "substitution_field": "${existing_field}"}, nil)
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil))
	wg := &sync.WaitGroup{}
	wg.Add(1)

	outEvents := make([]*pipeline.Event, 0, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e)
		wg.Done()
	})

	input.In(0, "test.log", 0,  []byte(`{"existing_field":"existing_value"}`))

	wg.Wait()
	p.Stop()

	assert.Equal(t, 1, len(outEvents), "wrong out events count")
	assert.Equal(t, "new_value", outEvents[0].Root.Dig("new_field").AsString(), "wrong field value")
	assert.Equal(t, "existing_value", outEvents[0].Root.Dig("substitution_field").AsString(), "wrong field value")
}
