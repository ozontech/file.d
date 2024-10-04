package json_decode

import (
	"sync"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
)

func TestDecode(t *testing.T) {
	config := test.NewConfig(&Config{Field: "log", Prefix: "prefix."}, nil)
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}
	wg.Add(1)
	output.SetOutFn(func(e *pipeline.Event) {
		assert.Equal(t, `{"prefix.field2":"value2","prefix.field3":"value3"}`, e.Root.EncodeToString(), "wrong out event")
		wg.Done()
	})

	input.In(0, "test.log", test.Offset(0), []byte(`{"log":"{\"field2\":\"value2\",\"field3\":\"value3\"}"}`))

	wg.Wait()
	p.Stop()
}
