package transform

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/ozontech/file.d/plugin/action/transform/runtime"
	"github.com/stretchr/testify/require"
)

func TestPluginStdlibScratchSurvivesInterruptedExecution(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name      string
		ending    string
		wantPanic bool
	}
	tests := []testCase{
		{
			name:   "function error after partial fill",
			ending: `.failed = join(["first", "second", 7], "/")`,
		},
		{
			name:   "abort",
			ending: `abort`,
		},
		{
			name:      "panic",
			wantPanic: true,
		},
	}
	type eventCase struct {
		name        string
		count       int
		interrupted bool
	}
	eventCases := []eventCase{
		{
			name:  "previous",
			count: 64,
		},
		{
			name:        "interrupted",
			count:       256,
			interrupted: true,
		},
		{
			name:  "next",
			count: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			source := `
				.before = [saved, %saved]
				saved = to_string(.values);
				%saved = saved
				joined = join(.values, "|")
				prefix = slice(joined, 0, end: 8)
				.out = {text: saved, joined: joined, prefix: prefix, metadata: %saved}
				.values = "overwritten"
			`
			if tt.ending != "" {
				source += "\nif .interrupt {\n" + tt.ending + "\n}\n"
			}
			source += "\n.finished = true\n"
			p := newStateTestPlugin(t, t.Name(), source)
			state := p.state.Load()
			if tt.wantPanic {
				// The helper copies the expression slice before wrapping it, so
				// the cached prepared program remains unchanged for other users.
				wrapStateTestExpr(p, len(p.program.expressions)-1, func(ctx core.EvalContext) {
					if ctx.GetTarget().(*runtime.RootTarget).SourceName == "interrupted" {
						panic("stdlib cleanup panic")
					}
				})
			}
			type expectedEvent struct {
				event *pipeline.Event
				want  string
			}
			var outputs []expectedEvent
			for _, input := range eventCases {
				values := make([]string, input.count)
				for i := range values {
					values[i] = fmt.Sprintf("%s-界🙂-%d", input.name, i)
				}
				encoded, err := json.Marshal(map[string]any{
					"values":    values,
					"interrupt": input.interrupted,
				})
				require.NoError(t, err)
				event := newStateTestEvent(t, string(encoded), input.name)
				if tt.wantPanic && input.interrupted {
					require.PanicsWithValue(t, "stdlib cleanup panic", func() { p.Do(event) })
				} else {
					require.Equal(t, pipeline.ActionPass, p.Do(event))
				}
				require.Same(t, state, p.state.Load(), "interrupted calls must return their execution state")
				requireStateTestReleased(t, p)

				joined := strings.Join(values, "|")
				formatted := stateToStringExpectedArray(values)
				want := map[string]any{
					"values":    "overwritten",
					"interrupt": input.interrupted,
					"before":    []any{nil, nil},
					"out": map[string]string{
						"text":     formatted,
						"joined":   joined,
						"prefix":   string([]rune(joined)[:8]),
						"metadata": formatted,
					},
				}
				if !input.interrupted {
					want["finished"] = true
				}
				wantJSON, err := json.Marshal(want)
				require.NoError(t, err)
				outputs = append(outputs, expectedEvent{
					event: event,
					want:  string(wantJSON),
				})

				// Both functions reuse one cache across success, interruption,
				// and recovery; all earlier event outputs must remain intact.
				for _, output := range outputs {
					require.JSONEq(t, output.want, output.event.Root.EncodeToString())
				}
			}
		})
	}
}
