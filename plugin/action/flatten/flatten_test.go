package flatten

import (
	"sync"
	"testing"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
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
	acceptedEvents := 0
	input.SetCommitFn(func(e *pipeline.Event) {
		acceptedEvents++
		wg.Done()
	})

	dumpedEvents := 0
	rawEvent := ""
	output.SetOutFn(func(e *pipeline.Event) {
		rawEvent = e.Root.EncodeToString()
		dumpedEvents++
		wg.Done()
	})

	input.In(0, "test.log", 0, []byte(`{"complex":{"a":"b","c":"d"}}`))

	wg.Add(2)
	wg.Wait()
	p.Stop()

	assert.Equal(t, 1, acceptedEvents)
	assert.Equal(t, 1, dumpedEvents)
	assert.Equal(t, `{"flat_a":"b","flat_c":"d"}`, rawEvent, "wrong out events count")
}
