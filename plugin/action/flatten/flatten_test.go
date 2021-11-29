package flatten

import (
	"sync"
	"testing"

	"github.com/ozonru/file.d/cfg"
	"github.com/ozonru/file.d/logger"
	"github.com/ozonru/file.d/pipeline"
	"github.com/ozonru/file.d/test"
	"github.com/stretchr/testify/assert"
)

func TestFlatten(t *testing.T) {
	config := &Config{Field: "complex", Prefix: "flat_"}
	err := cfg.Parse(config, nil)
	if err != nil {
		logger.Panicf("wrong config")
	}

	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))

	wg := &sync.WaitGroup{}
	acceptedEvents := make([]*pipeline.Event, 0, 0)
	input.SetCommitFn(func(e *pipeline.Event) {
		wg.Done()
		acceptedEvents = append(acceptedEvents, e)
	})

	dumpedEvents := make([]*pipeline.Event, 0, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		wg.Done()
		dumpedEvents = append(dumpedEvents, e)
	})

	input.In(0, "test.log", 0, []byte(`{"complex":{"a":"b","c":"d"}}`))

	wg.Add(2)
	wg.Wait()
	p.Stop()

	assert.Equal(t, 1, len(acceptedEvents), "wrong in events count")
	assert.Equal(t, 1, len(dumpedEvents), "wrong out events count")
	assert.Equal(t, `{"flat_a":"b","flat_c":"d"}`, dumpedEvents[0].Root.EncodeToString(), "wrong out events count")
}
