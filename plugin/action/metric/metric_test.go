package metric

import (
	"sync"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
)

func TestPlugin(t *testing.T) {
	suits := []struct {
		name    string
		input   []string
		comment string
	}{
		{
			name:    "count errors",
			input:   []string{`{"level":"error"}`},
			comment: "card number masked",
		},
	}

	config := test.NewConfig(&Config{
		Name:   "errors_total",
		Labels: []string{"level"},
	}, nil)
	actions := test.NewActionPluginStaticInfo(factory, config,
		pipeline.MatchModeAnd,
		nil,
		false,
	)
	for _, s := range suits {
		t.Run(s.name, func(t *testing.T) {
			sut, input, output := test.NewPipelineMock(actions)
			wg := sync.WaitGroup{}
			wg.Add(len(s.input))

			// outEvents := make([]string, 0, len(s.expected))
			output.SetOutFn(func(e *pipeline.Event) {
				// outEvents = append(outEvents, e.Root.EncodeToString())
				wg.Done()
			})

			for _, in := range s.input {
				input.In(0, "test.log", 0, []byte(in))
			}

			wg.Wait()
			sut.Stop()

			// for i := range s.expected {
			// 	assert.Equal(t, s.expected[i], outEvents[i], s.comment)
			// 	assert.True(t, json.Valid([]byte(outEvents[i])))
			// }
		})
	}
}
