package modify

import (
	"sync"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
)

func TestModify(t *testing.T) {
	config := test.NewConfig(&Config{
		"new_field":                "new_value",
		"my_object.field.subfield": "${existing_field}",
	}, nil)
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}
	wg.Add(1)

	output.SetOutFn(func(e *pipeline.Event) {
		assert.Equal(t, "new_value", e.Root.Dig("new_field").AsString(), "wrong event field")
		assert.Equal(t, "existing_value", e.Root.Dig("my_object", "field", "subfield").AsString(), "wrong event field")
		wg.Done()
	})

	input.In(0, "test.log", 0, []byte(`{"existing_field":"existing_value","my_object":{"field":{"subfield":"subfield_value"}}}`))

	wg.Wait()
	p.Stop()
}
