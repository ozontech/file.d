package add_file_name

import (
	"sync"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
)

func TestModify(t *testing.T) {
	config := test.NewConfig(&Config{Field: "file"}, nil)
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}
	wg.Add(2)

	sourceName := "my_file"
	output.SetOutFn(func(e *pipeline.Event) {
		assert.Equal(t, sourceName, e.Root.Dig("file").AsString(), "wrong field value")
		wg.Done()
	})

	input.In(0, sourceName, 0, []byte(`{"error":"info about error"}`))
	input.In(0, sourceName, 0, []byte(`{"file":"not_my_file"}`))

	wg.Wait()
	p.Stop()
}
