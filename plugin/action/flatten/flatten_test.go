package flatten

import (
	"sync"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
)

func TestFlatten(t *testing.T) {
	config := test.NewConfig(&Config{Field: "complex", Prefix: "flat_"}, nil)
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))

	wg := &sync.WaitGroup{}
	wg.Add(2)

	input.SetCommitFn(func(e *pipeline.Event) {
		wg.Done()
	})

	output.SetOutFn(func(e *pipeline.Event) {
		assert.Equal(t, `{"flat_a":"b","flat_c":"d"}`, e.Root.EncodeToString(), "wrong out event")
		wg.Done()
	})

	input.In(0, "test.log", test.Offset(0), []byte(`{"complex":{"a":"b","c":"d"}}`))

	wg.Wait()
	p.Stop()
}
