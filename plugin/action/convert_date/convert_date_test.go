package convert_date

import (
	"sync"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
)

func TestConvert(t *testing.T) {
	config := &Config{SourceFormats: []string{"rfc3339nano", "rfc3339", "ansic", pipeline.UnixTime, "nginx_errorlog"}}
	test.NewConfig(config, nil)

	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}
	wg.Add(3)

	outEvents := make([]string, 0, 3)
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e.Root.EncodeToString())
		wg.Done()
	})

	input.In(0, "test.log", test.Offset(0), []byte(`{"time":998578502}`))
	input.In(0, "test.log", test.Offset(0), []byte(`{"time":998578999.1346}`))
	input.In(0, "test.log", test.Offset(0), []byte(`{"time":"2022/02/07 13:06:14"}`))

	wg.Wait()
	p.Stop()

	assert.Equal(t, 3, len(outEvents), "wrong out events count")
	assert.Equal(t, `{"time":998578502}`, outEvents[0], "wrong out event")
	assert.Equal(t, `{"time":998578999}`, outEvents[1], "wrong out event")
	assert.Equal(t, `{"time":1644239174}`, outEvents[2], "wrong out event")
}

func TestConvertFail(t *testing.T) {
	config := &Config{
		SourceFormats: []string{"rfc3339nano", "rfc3339", "ansic"},
		RemoveOnFail:  true,
	}
	test.NewConfig(config, nil)

	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}
	wg.Add(1)

	output.SetOutFn(func(e *pipeline.Event) {
		assert.Equal(t, `{}`, e.Root.EncodeToString(), "wrong out event")
		wg.Done()
	})

	input.In(0, "test.log", test.Offset(0), []byte(`{"time":"XXX"}`))

	wg.Wait()
	p.Stop()
}
