package remap

import (
	"strings"
	"sync"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/require"
)

func TestPlugin_Do(t *testing.T) {
	const field = "data"
	config := test.NewConfig(&Config{Field: field}, nil)
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}

	const children = 6
	const parents = 2
	const total = children + parents
	wg.Add(total)

	outEvents := make([]*pipeline.Event, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e)
		wg.Done()
	})

	input.In(0, "test.log", 0, []byte(`{ 
	"data": [
		{ "message": "go" }, 
		{ "message": "rust" },
		{ "message": "c++" }
	]
}`))
	input.In(0, "test.log", 0, []byte(`{ 
	"data": [
		{ "message": "python" }, 
		{ "message": "ruby" },
		{ "message": "js" }
	]
}`))

	wg.Wait()
	p.Stop()

	require.Equal(t, total, len(outEvents))

	result := make([]string, 0, len(outEvents))
	for _, e := range outEvents {
		if e.IsChildParentKind() {
			continue
		}
		result = append(result, strings.Clone(e.Root.Dig("message").AsString()))
	}
	require.Equal(t, []string{"go", "rust", "c++", "python", "ruby", "js"}, result)
}
