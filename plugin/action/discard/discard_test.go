package discard

import (
	"regexp"
	"sync"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
)

func TestDiscard(t *testing.T) {
	cases := []struct {
		name string

		matchMode       pipeline.MatchMode
		matchConditions pipeline.MatchConditions
		matchInvert     bool

		passEvents    []string
		discardEvents []string
	}{
		{
			name:      "match_and",
			matchMode: pipeline.MatchModeAnd,
			matchConditions: pipeline.MatchConditions{
				pipeline.MatchCondition{
					Field:  []string{"field1"},
					Values: []string{"value1"},
				},
				pipeline.MatchCondition{
					Field:  []string{"field2"},
					Values: []string{"value2"},
				},
			},
			passEvents: []string{
				`{"field1":"not_value1"}`,
				`{"field2":"not_value2"}`,
				`{"field1":"value1"}`,
				`{"field2":"value2"}`,
			},
			discardEvents: []string{
				`{"field1":"value1","field2":"value2"}`,
				`{"field3":"value3","field1":"value1","field2":"value2"}`,
			},
		},
		{
			name:      "match_or",
			matchMode: pipeline.MatchModeOr,
			matchConditions: pipeline.MatchConditions{
				pipeline.MatchCondition{
					Field:  []string{"field1"},
					Values: []string{"value1"},
				},
				pipeline.MatchCondition{
					Field:  []string{"field2"},
					Values: []string{"value2"},
				},
			},
			passEvents: []string{
				`{"field1":"not_value1"}`,
				`{"field2":"not_value2"}`,
			},
			discardEvents: []string{
				`{"field1":"value1"}`,
				`{"field2":"value2"}`,
				`{"field1":"value1","field2":"value2"}`,
				`{"field3":"value3","field1":"value1","field2":"value2"}`,
			},
		},
		{
			name:      "match_or_regex",
			matchMode: pipeline.MatchModeOr,
			matchConditions: pipeline.MatchConditions{
				pipeline.MatchCondition{
					Field:  []string{"field1"},
					Regexp: regexp.MustCompile("(one|two|three)"),
				},
				pipeline.MatchCondition{
					Field:  []string{"field2", "field3"},
					Regexp: regexp.MustCompile("four"),
				},
			},
			passEvents: []string{
				`{"field2":{"field3":"0000 one 0000"}}`,
				`{"field1":"four"}`,
				`{"field2":"... four ....","field3":"value2"}`,
				`{"field3":"value3","field1":"value1","field2":"value2"}`,
			},
			discardEvents: []string{
				`{"field1":"0000 one 0000"}`,
				`{"field2":{"field3":"0000 four 0000"}}`,
				`{"field1":". two ."}`,
			},
		},
		{
			name:      "match_and_invert",
			matchMode: pipeline.MatchModeAnd,
			matchConditions: pipeline.MatchConditions{
				pipeline.MatchCondition{
					Field:  []string{"field2"},
					Values: []string{"value2"},
				},
			},
			matchInvert: true,
			passEvents: []string{
				`{"field2":"value2"}`,
				`{"field1":"value1","field2":"value2"}`,
				`{"field3":"value3","field1":"value1","field2":"value2"}`,
			},
			discardEvents: []string{
				`{"field1":"not_value1"}`,
				`{"field2":"not_value2"}`,
				`{"field1":"value1"}`,
			},
		},
	}
	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, nil, tt.matchMode, tt.matchConditions, tt.matchInvert))

			wg := &sync.WaitGroup{}
			wg.Add(len(tt.discardEvents) + 2*len(tt.passEvents))

			inEvents := 0
			input.SetInFn(func() {
				inEvents++
				wg.Done()
			})

			outEvents := make([]string, 0, len(tt.passEvents))
			output.SetOutFn(func(e *pipeline.Event) {
				outEvents = append(outEvents, e.Root.EncodeToString())
				wg.Done()
			})

			for _, e := range tt.passEvents {
				input.In(0, "test", test.Offset(0), []byte(e))
			}
			for _, e := range tt.discardEvents {
				input.In(0, "test", test.Offset(0), []byte(e))
			}

			wg.Wait()
			p.Stop()

			assert.Equal(t, len(tt.passEvents)+len(tt.discardEvents), inEvents, "wrong in events count")
			assert.Equal(t, len(tt.passEvents), len(outEvents), "wrong out events count")

			for i := range outEvents {
				assert.Equal(t, tt.passEvents[i], outEvents[i], "wrong event json")
			}
		})
	}
}
