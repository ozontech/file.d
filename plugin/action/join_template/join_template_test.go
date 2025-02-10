package join_template

import (
	_ "embed"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

//go:embed samples/go_panic.txt
var contentPanics string

//go:embed samples/go_panic_nil_nodes.txt
var contentPanicsWithNilNodes string

//go:embed samples/sharp_exception.txt
var contentSharpException string

func TestSimpleJoin(t *testing.T) {
	cases := []struct {
		name         string
		templateName string
		content      string
		expEvents    int32
		iterations   int
	}{
		{
			name:         "should_ok_for_panics",
			templateName: "go_panic",
			content:      contentPanics,
			iterations:   100,
			expEvents:    17 * 100,
		},
		{
			name:         "should_ok_for_sharp_exception",
			templateName: "sharp_exception",
			content:      contentSharpException,
			iterations:   100,
			expEvents:    1 * 100,
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
				Field:    "log",
				Template: tt.templateName,

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
			content:    contentPanicsWithNilNodes,
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
