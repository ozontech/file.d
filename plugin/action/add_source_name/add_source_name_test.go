package add_source_name

import (
	"sync"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
)

func TestModify(t *testing.T) {
	config := test.NewConfig(&Config{Field: "source"}, nil)
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}
	wg.Add(2)

	outEvents := make([]*pipeline.Event, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e)
		wg.Done()
	})

	input.In(0, "my_source", 0, []byte(`{"error":"info about error"}`))
	input.In(0, "my_source", 0, []byte(`{"source":"not_my_source"}`))

	wg.Wait()
	p.Stop()

	assert.Equal(t, 2, len(outEvents), "wrong out events count")
	assert.Equal(t, "my_source", outEvents[0].Root.Dig("source").AsString(), "wrong field value")
	assert.Equal(t, "my_source", outEvents[1].Root.Dig("source").AsString(), "wrong field value")
}
