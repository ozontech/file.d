package transform

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/stretchr/testify/require"
)

func TestPluginToStringResultsSurviveConcurrentReuse(t *testing.T) {
	t.Parallel()

	const source = `
		first = to_string(.values);
		%first = first
		second = to_string(.object);
		%second = second
		items = [first, second, to_string([first, to_string([second, 7])])]
		key = to_string(.key)
		.dynamic = {}
		.dynamic[key] = to_string([join(.tail, "/"), first, second])
		.again = join([to_string(.values), to_string(.object)], "|")
		.values = "overwritten"
		.object = "overwritten"
		.key = "overwritten"
		.tail = "overwritten"
		.out = {first: first, second: second, metadata: [%first, %second], items: items}
	`
	plugins := []*Plugin{
		newStateTestPlugin(t, t.Name(), source),
		newStateTestPlugin(t, t.Name(), source),
	}
	require.Same(t, plugins[0].program.expressions[0], plugins[1].program.expressions[0])
	require.NotSame(t, plugins[0].state.Load(), plugins[1].state.Load())

	type inputSize struct {
		name  string
		count int
	}
	sizes := []inputSize{
		{
			name:  "large",
			count: 256,
		},
		{
			name:  "small",
			count: 4,
		},
		{
			name:  "tiny",
			count: 2,
		},
	}
	for worker := range 4 {
		t.Run(fmt.Sprintf("worker_%d", worker), func(t *testing.T) {
			t.Parallel()

			p := plugins[worker%len(plugins)]
			type expectedEvent struct {
				event *pipeline.Event
				want  string
			}
			var previous []expectedEvent
			for round := range 2 {
				for _, size := range sizes {
					marker := fmt.Sprintf("worker_%d_round_%d_%s", worker, round, size.name)
					values := make([]string, size.count)
					for i := range values {
						values[i] = fmt.Sprintf("%s-value_%d-界🙂\n\"quoted\"", marker, i)
					}
					left := []string{marker, "nested界"}
					right := []string{"tail", marker}
					tail := []string{marker, "joined🙂"}
					input, err := json.Marshal(map[string]any{
						"values": values,
						"object": map[string]any{"nested": [][]string{left, right}},
						"key":    []any{marker, round},
						"tail":   tail,
					})
					require.NoError(t, err)
					event := newStateTestEvent(t, string(input), marker)

					// Derive the expected display syntax independently of Value.String
					// and keep formatted objects to one key, whose order is stable.
					first := stateToStringExpectedArray(values)
					second := fmt.Sprintf(`{"nested": [%s, %s]}`,
						stateToStringExpectedArray(left), stateToStringExpectedArray(right))
					inner := "[" + strconv.Quote(second) + ", 7]"
					key := fmt.Sprintf("[%q, %d]", marker, round)
					want, err := json.Marshal(map[string]any{
						"values": "overwritten",
						"object": "overwritten",
						"key":    "overwritten",
						"tail":   "overwritten",
						"dynamic": map[string]string{
							key: stateToStringExpectedArray([]string{strings.Join(tail, "/"), first, second}),
						},
						"again": first + "|" + second,
						"out": map[string]any{
							"first":    first,
							"second":   second,
							"metadata": []string{first, second},
							"items": []string{
								first,
								second,
								stateToStringExpectedArray([]string{first, inner}),
							},
						},
					})
					require.NoError(t, err)
					require.Equal(t, pipeline.ActionPass, p.Do(event))
					previous = append(previous, expectedEvent{
						event: event,
						want:  string(want),
					})

					// Later large and small calls share scratch with join while other
					// workers execute the same plugin and prepared program.
					for _, output := range previous {
						require.JSONEq(t, output.want, output.event.Root.EncodeToString())
					}
				}
			}
		})
	}
}

func stateToStringExpectedArray(values []string) string {
	quoted := make([]string, len(values))
	for i, value := range values {
		quoted[i] = strconv.Quote(value)
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}
