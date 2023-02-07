package convert_date

import (
	"sync"
	"testing"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
)

func TestConvert(t *testing.T) {
	config := &Config{SourceFormats: []string{"rfc3339nano", "rfc3339", "ansic", "timestamp", "nginx_errorlog"}}

	err := cfg.Parse(config, nil)
	if err != nil {
		logger.Panicf("wrong config")
	}

	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}
	wg.Add(3)

	inEvents := 0
	input.SetInFn(func() {
		inEvents++
	})

	outEvents := make([]*pipeline.Event, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e)
		wg.Done()
	})

	input.In(0, "test.log", 0, []byte(`{"time":998578502}`))
	input.In(0, "test.log", 0, []byte(`{"time":998578999.1346}`))
	input.In(0, "test.log", 0, []byte(`{"time":"2022/02/07 13:06:14"}`))

	wg.Wait()
	p.Stop()

	assert.Equal(t, 3, inEvents, "wrong in events count")
	assert.Equal(t, 3, len(outEvents), "wrong out events count")

	assert.Equal(t, `{"time":998578502}`, outEvents[0].Root.EncodeToString(), "wrong out event")
	assert.Equal(t, `{"time":998578999}`, outEvents[1].Root.EncodeToString(), "wrong out event")
	assert.Equal(t, `{"time":1644239174}`, outEvents[2].Root.EncodeToString(), "wrong out event")
}

func TestConvertFail(t *testing.T) {
	config := &Config{SourceFormats: []string{"rfc3339nano", "rfc3339", "ansic"}, RemoveOnFail: true}

	err := cfg.Parse(config, nil)
	if err != nil {
		logger.Panicf("wrong config")
	}

	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}
	wg.Add(1)

	inEvents := 0
	input.SetInFn(func() {
		inEvents++
	})

	outEvents := make([]*pipeline.Event, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e)
		wg.Done()
	})

	input.In(0, "test.log", 0, []byte(`{"time":"XXX"}`))

	wg.Wait()
	p.Stop()

	assert.Equal(t, 1, inEvents, "wrong in events count")
	assert.Equal(t, 1, len(outEvents), "wrong out events count")
	assert.Equal(t, `{}`, outEvents[0].Root.EncodeToString(), "wrong out event")
}
