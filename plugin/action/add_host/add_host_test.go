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

	host, _ := os.Hostname()
	output.SetOutFn(func(e *pipeline.Event) {
		assert.Equal(t, host, e.Root.Dig("hostname").AsString(), "wrong field value")
		wg.Done()
	})

	input.In(0, "test.log", test.Offset(0), []byte(`{}`))

	wg.Wait()
	p.Stop()
}
