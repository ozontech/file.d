package transform

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/stretchr/testify/require"
)

func TestPluginSliceResultsSurviveConcurrentReuse(t *testing.T) {
	t.Parallel()

	const source = `
		first = slice(.value, 1, end: 17);
		%saved = first
		items = [first, {tail: slice(.value, -12)}]
		key = slice(.key, 1, end: -1)
		.dynamic = {}
		.dynamic[key] = slice(slice(.value, 2), 0, end: 8)
		.value = "overwritten"
		.key = "overwritten"
		.out = {local: first, metadata: %saved, items: items}
		.again = slice(.replacement, 1, end: 5)
		.replacement = "overwritten"
	`
	plugins := []*Plugin{
		newStateTestPlugin(t, t.Name(), source),
		newStateTestPlugin(t, t.Name(), source),
	}
	require.Same(t, plugins[0].program.expressions[0], plugins[1].program.expressions[0])
	require.NotSame(t, plugins[0].state.Load(), plugins[1].state.Load())

	for worker := range 4 {
		t.Run(fmt.Sprintf("worker_%d", worker), func(t *testing.T) {
			t.Parallel()

			p := plugins[worker%len(plugins)]
			type expectedEvent struct {
				event *pipeline.Event
				want  string
			}
			var previous []expectedEvent
			for iteration := range 8 {
				value := strings.Repeat(fmt.Sprintf("w%d-e%d-界🙂запрос/", worker, iteration), 256)
				key := fmt.Sprintf("worker_%d_event_%d", worker, iteration)
				replacement := fmt.Sprintf("次%d-%d🙂replacement", worker, iteration)
				input, err := json.Marshal(map[string]any{
					"value":       value,
					"key":         "界" + key + "🙂",
					"replacement": replacement,
				})
				require.NoError(t, err)
				event := newStateTestEvent(t, string(input), t.Name())

				// The reference keeps the old rune-based semantics and does not
				// call the optimized function to derive expected output.
				runes := []rune(value)
				first := string(runes[1:17])
				want, err := json.Marshal(map[string]any{
					"value":       "overwritten",
					"key":         "overwritten",
					"replacement": "overwritten",
					"dynamic": map[string]string{
						key: string(runes[2:10]),
					},
					"out": map[string]any{
						"local":    first,
						"metadata": first,
						"items": []any{
							first,
							map[string]string{"tail": string(runes[len(runes)-12:])},
						},
					},
					"again": string([]rune(replacement)[1:5]),
				})
				require.NoError(t, err)
				require.Equal(t, pipeline.ActionPass, p.Do(event))
				previous = append(previous, expectedEvent{
					event: event,
					want:  string(want),
				})

				// Recheck earlier events after each subsequent Do while the
				// other workers continue on the same plugin and shared program.
				for _, output := range previous {
					require.JSONEq(t, output.want, output.event.Root.EncodeToString())
				}
			}
		})
	}
}
