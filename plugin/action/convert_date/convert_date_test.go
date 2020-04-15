package convert_date

import (
	"sync"
	"testing"

	"github.com/ozonru/file.d/cfg"
	"github.com/ozonru/file.d/logger"
	"github.com/ozonru/file.d/pipeline"
	"github.com/ozonru/file.d/test"
	"github.com/stretchr/testify/assert"
)

func TestConvert(t *testing.T) {
	config := &Config{SourceFormats: []string{"rfc3339nano", "rfc3339", "ansic"}}

	err := cfg.Parse(config, nil)
	if err != nil {
		logger.Panicf("wrong config")
	}

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

	input.In(0, "test.log", 0, []byte(`{"time":"Thu Aug 23 14:55:02 2001"}`))

	wg.Wait()
	p.Stop()

	assert.Equal(t, 1, inEvents, "wrong in events count")
	assert.Equal(t, 1, len(outEvents), "wrong out events count")
	assert.Equal(t, `{"time":"998578502"}`, outEvents[0].Root.EncodeToString(), "wrong out event")
}
