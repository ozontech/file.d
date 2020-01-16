package flatten

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.ozon.ru/sre/file-d/pipeline"
	"gitlab.ozon.ru/sre/file-d/test"
)

func TestFlatten(t *testing.T) {
	config := &Config{Field: "complex", Prefix: "flat_"}
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil))

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

	input.In(0, "test.log", 0, 0, []byte(`{"complex":{"a":"b","c":"d"}}`))

	wg.Add(2)
	wg.Wait()
	p.Stop()

	assert.Equal(t, 1, len(acceptedEvents), "wrong in events count")
	assert.Equal(t, 1, len(dumpedEvents), "wrong out events count")
	assert.Equal(t, `{"flat_a":"b","flat_c":"d"}`, dumpedEvents[0].Root.EncodeToString(), "wrong out events count")
}
