package json_decode

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"gitlab.ozon.ru/sre/filed/pipeline"
	"gitlab.ozon.ru/sre/filed/test"
)

func TestDecode(t *testing.T) {
	config := &Config{Field: "log", Prefix: "prefix."}
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil))
	wg := &sync.WaitGroup{}
	wg.Add(1)

	inEvents := 0
	input.SetInFn(func() {
		inEvents++
	})

	outEvents := make([]*pipeline.Event, 0, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e)
		wg.Done()
	})

	input.In(0, "test.log", 0, 0, []byte(`{"log":"{\"field2\":\"value2\",\"field3\":\"value3\"}"}`))

	wg.Wait()
	p.Stop()

	assert.Equal(t, 1, inEvents, "wrong in events count")
	assert.Equal(t, 1, len(outEvents), "wrong out events count")
	assert.Equal(t, `{"prefix.field2":"value2","prefix.field3":"value3"}`, outEvents[0].Root.EncodeToString(), "wrong out event")
}
