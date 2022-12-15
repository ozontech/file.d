package add_host

import (
	"os"
	"sync"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
)

func TestModify(t *testing.T) {
	config := test.NewConfig(&Config{Field: "hostname"}, nil)
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}
	wg.Add(1)

	outEvents := make([]*pipeline.Event, 0)
	output.SetOutFn(func(e *pipeline.Event) {
		outEvents = append(outEvents, e)
		wg.Done()
	})

	input.In(0, "test.log", 0, []byte(`{}`), pipeline.NewMeta())

	wg.Wait()
	p.Stop()

	host, _ := os.Hostname()

	assert.Equal(t, 1, len(outEvents), "wrong out events count")
	assert.Equal(t, host, outEvents[0].Root.Dig("hostname").AsString(), "wrong field value")
}
