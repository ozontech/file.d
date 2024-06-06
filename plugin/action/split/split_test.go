package split

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

	splitted := make([]string, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		defer wg.Done()
		if e.IsChildParentKind() {
			return
		}

		splitted = append(splitted, strings.Clone(e.Root.Dig("message").AsString()))
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

	require.Equal(t, children, len(splitted))
	require.Equal(t, []string{"go", "rust", "c++", "python", "ruby", "js"}, splitted)
}

func TestPlugin_DoArray(t *testing.T) {
	config := test.NewConfig(&Config{Field: ""}, nil)
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}

	const children = 3
	const parents = 1
	const total = children + parents
	wg.Add(total)

	sourceName := "test.log"

	splitted := make([]string, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		defer wg.Done()
		if e.IsChildParentKind() {
			return
		}

		if sourceName != e.SourceName {
			return
		}

		splitted = append(splitted, strings.Clone(e.Root.Dig("message").AsString()))
	})

	input.In(0, sourceName, 0, []byte(`[
		{ "message": "go" }, 
		{ "message": "rust" },
		{ "message": "c++" }
	]`))

	wg.Wait()
	p.Stop()

	require.Equal(t, children, len(splitted))
	require.Equal(t, []string{"go", "rust", "c++"}, splitted)
}
