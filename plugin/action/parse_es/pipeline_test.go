package parse_es

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ozonru/file.d/cfg"
	"github.com/ozonru/file.d/logger"
	"github.com/ozonru/file.d/pipeline"
	"github.com/ozonru/file.d/test"
)

func TestPipeline(t *testing.T) {
	type eventIn struct {
		sourceID   pipeline.SourceID
		sourceName string
		offset     int64
		bytes      []byte
	}

	validMsg := `{"message":"valid_mgs_to_parse","kind":"normal"}`
	invalidMsg := `BaDsYmBoLs{"message":"valid_mgs_to_parse","kind":"normal"}`

	cases := []struct {
		name           string
		eventsIn       []eventIn
		eventsInCount  int
		eventsOutCount int
		firstEventOut  string
	}{
		{
			name: "wrong_events",
			eventsIn: []eventIn{
				{
					sourceID:   pipeline.SourceID(1),
					sourceName: "parse_es_test_source.log",
					offset:     0,
					bytes:      []byte(invalidMsg),
				},
				{
					sourceID:   pipeline.SourceID(1),
					sourceName: "parse_es_test_source.log",
					offset:     0,
					bytes:      []byte(invalidMsg),
				},
			},
			eventsInCount:  2,
			eventsOutCount: 0,
		},
		{
			name: "correct_events",
			eventsIn: []eventIn{
				{
					sourceID:   pipeline.SourceID(1),
					sourceName: "parse_es_test_source.log",
					offset:     0,
					bytes:      []byte(validMsg),
				},
				{
					sourceID:   pipeline.SourceID(1),
					sourceName: "parse_es_test_source.log",
					offset:     0,
					bytes:      []byte(validMsg),
				},
			},
			eventsInCount:  2,
			eventsOutCount: 2,
		},
		{
			name: "mixed_events",
			eventsIn: []eventIn{
				{
					sourceID:   pipeline.SourceID(1),
					sourceName: "parse_es_test_source.log",
					offset:     0,
					bytes:      []byte(validMsg),
				},
				{
					sourceID:   pipeline.SourceID(1),
					sourceName: "parse_es_test_source.log",
					offset:     0,
					bytes:      []byte(invalidMsg),
				},
			},
			eventsInCount:  2,
			eventsOutCount: 1,
		},
	}

	for _, tCase := range cases {
		tCase := tCase
		t.Run(tCase.name, func(t *testing.T) {
			config := &Config{}
			p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeOr, nil, false))

			err := cfg.Parse(config, nil)
			if err != nil {
				logger.Panicf("wrong config")
			}

			wg := &sync.WaitGroup{}
			wg.Add(tCase.eventsInCount)
			wg.Add(tCase.eventsOutCount)

			eventsIn := 0
			input.SetInFn(func() {
				wg.Done()
				eventsIn++
			})

			outEvents := make([]*pipeline.Event, 0)
			output.SetOutFn(func(e *pipeline.Event) {
				wg.Done()
				outEvents = append(outEvents, e)
			})

			for _, event := range tCase.eventsIn {
				input.In(event.sourceID, event.sourceName, event.offset, event.bytes)
			}

			wg.Wait()
			p.Stop()

			assert.Equal(t, 2, eventsIn, "wrong eventsIn events count")
			assert.Equal(t, tCase.eventsOutCount, len(outEvents), "wrong out events count")
		})
	}
}
