package join_template

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/action/join_template/sample"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestSimpleJoin(t *testing.T) {
	cases := []struct {
		name       string
		template   string
		templates  []cfgTemplate
		content    string
		expEvents  int32
		iterations int
	}{
		{
			name:       "should_ok_for_go_panic",
			template:   "go_panic",
			content:    sample.Panics,
			iterations: 100,
			expEvents:  17 * 100,
		},
		{
			name:       "should_ok_for_cs_exception",
			template:   "cs_exception",
			content:    sample.SharpException,
			iterations: 100,
			expEvents:  3 * 100,
		},
		{
			name:       "should_ok_for_go_data_race",
			template:   "go_data_race",
			content:    sample.GoDataRace,
			iterations: 100,
			expEvents:  3 * 3 * 100,
		},
		{
			name: "should_ok_for_mixed",
			templates: []cfgTemplate{
				{Name: "go_panic", FastCheck: true},
				{Name: "cs_exception", FastCheck: true},
				{Name: "go_data_race", FastCheck: true},
			},
			content:    sample.Panics + sample.SharpException + sample.GoDataRace,
			iterations: 100,
			expEvents:  (17 + 3 + 3*3) * 100,
		},
	}

	for _, tt := range cases {
		tt := tt

		var fastCheck bool
		testFunc := func(t *testing.T) {
			format := `{"log":"%s\n"}`
			content := strings.ReplaceAll(tt.content, "# ===next===\n", "")
			lines := make([]string, 0)
			for _, line := range strings.Split(content, "\n") {
				if line == "" {
					continue
				}
				lines = append(lines, fmt.Sprintf(format, line))
			}

			config := test.NewConfig(&Config{
				Field:     "log",
				Template:  tt.template,
				Templates: tt.templates,

				FastCheck: fastCheck,
			}, nil)

			p, input, output := test.NewPipelineMock(
				test.NewActionPluginStaticInfo(
					factory,
					config,
					pipeline.MatchModeAnd,
					nil,
					false,
				),
				"short_event_timeout",
			)

			inEvents := atomic.Int32{}
			input.SetInFn(func() {
				inEvents.Inc()
			})

			outEvents := atomic.Int32{}
			lastID := atomic.Uint64{}
			output.SetOutFn(func(e *pipeline.Event) {
				outEvents.Inc()
				id := lastID.Swap(e.SeqID)
				require.False(t, id != 0 && id >= e.SeqID)
			})

			for i := 0; i < tt.iterations; i++ {
				for m, line := range lines {
					input.In(0, "test.log", test.NewOffset(int64(i*10000+m)), []byte(line))
				}
			}

			var (
				i     = 0
				iters = 100
			)
			for ; i < iters; i++ {
				x := outEvents.Load()
				if x < tt.expEvents {
					time.Sleep(time.Millisecond * 100)
					continue
				}
				break
			}

			p.Stop()

			require.True(t, iters > i, "test timed out")
			assert.Equal(t, tt.expEvents, outEvents.Load(), "wrong out events count")
		}

		fastCheck = false
		t.Run(tt.name, testFunc)
		fastCheck = true
		t.Run(tt.name+"_fast", testFunc)
	}
}

func TestJoinAfterNilNode(t *testing.T) {
	cases := []struct {
		name       string
		content    string
		expEvents  int32
		iterations int
	}{
		{
			name:       "should_ok_for_panics",
			content:    sample.PanicsWithNilNodes,
			iterations: 100,
			expEvents:  23 * 100,
		},
	}

	for _, tt := range cases {
		var fastCheck bool
		testFunc := func(t *testing.T) {
			formatNode := `{"log":"%s\n"}`
			formatNilNode := `{"notlog":"%s\n"}`
			content := strings.ReplaceAll(tt.content, "# ===next===\n", "")
			lines := make([]string, 0)
			for _, line := range strings.Split(content, "\n") {
				if line == "" {
					continue
				}
				if strings.HasPrefix(line, "NilNode:") {
					lines = append(lines, fmt.Sprintf(formatNilNode, line))
					continue
				}
				lines = append(lines, fmt.Sprintf(formatNode, line))
			}

			config := test.NewConfig(&Config{
				Field:    "log",
				Template: "go_panic",

				FastCheck: fastCheck,
			}, nil)

			p, input, output := test.NewPipelineMock(
				test.NewActionPluginStaticInfo(
					factory,
					config,
					pipeline.MatchModeAnd,
					nil,
					false,
				),
				"short_event_timeout",
			)

			inEvents := atomic.Int32{}
			input.SetInFn(func() {
				inEvents.Inc()
			})

			outEvents := atomic.Int32{}
			lastID := atomic.Uint64{}
			output.SetOutFn(func(e *pipeline.Event) {
				outEvents.Inc()
				id := lastID.Swap(e.SeqID)
				require.False(t, id != 0 && id >= e.SeqID)
			})

			for i := 0; i < tt.iterations; i++ {
				for m, line := range lines {
					input.In(0, "test.log", test.NewOffset(int64(i*10000+m)), []byte(line))
				}
			}

			var (
				i           = 0
				iters       = 100
				prevX       = int32(0)
				repeatCount = 0
			)
			for ; i < iters; i++ {
				x := outEvents.Load()
				if x < tt.expEvents {
					time.Sleep(time.Millisecond * 100)
					if x == prevX {
						repeatCount++
					}
					prevX = x
					continue
				}
				break
			}

			p.Stop()

			assert.Equal(t, tt.expEvents, outEvents.Load(), "wrong out events count")
		}

		fastCheck = false
		t.Run(tt.name, testFunc)
		fastCheck = true
		t.Run(tt.name+"_fast", testFunc)
	}
}
