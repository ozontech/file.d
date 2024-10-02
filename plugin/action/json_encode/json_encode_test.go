package json_encode

import (
	"sync"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
)

func TestEncode(t *testing.T) {
	config := test.NewConfig(&Config{Field: "server"}, nil)
	p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))
	wg := &sync.WaitGroup{}
	wg.Add(1)

	output.SetOutFn(func(e *pipeline.Event) {
		assert.Equal(t, `{"server":"{\"os\":\"linux\",\"arch\":\"amd64\"}"}`, e.Root.EncodeToString(), "wrong out event")
		wg.Done()
	})

	input.In(0, "test.log", test.Offset(0), []byte(`{"server":{"os":"linux","arch":"amd64"}}`))

	wg.Wait()
	p.Stop()
}
