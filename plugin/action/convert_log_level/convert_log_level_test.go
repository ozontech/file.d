package convert_log_level

import (
	"sync"
	"testing"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/require"
)

func TestDo(t *testing.T) {
	config := &Config{
		Field:        "info.level",
		Style:        "string",
		DefaultLevel: "",
		RemoveOnFail: false,
	}

	err := cfg.Parse(config, nil)
	require.NoError(t, err)

	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}
	wg.Add(1)

	var outEvents []string
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e.Root.EncodeToString())
		wg.Done()
	})

	input.In(0, "test.log", 0, []byte(`{"info":{"level": "6"}}`))

	wg.Wait()
	p.Stop()

	require.Equal(t, []string{`{"info":{"level":"informational"}}`}, outEvents)
}
