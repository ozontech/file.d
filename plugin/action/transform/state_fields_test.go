package transform

import (
	"fmt"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/stretchr/testify/require"
)

func TestPluginFieldOperationsConcurrentEvents(t *testing.T) {
	t.Parallel()

	p := newStateTestPlugin(t, t.Name(), `
		.present = exists(.source)
		.saved = .source
		.moved = rename(.source, .destination)
		if .fail {
			rename(.destination, .destination.child)
		}
		.destination.value = "updated"
		.finished = true
	`)
	for worker := range 8 {
		t.Run(fmt.Sprintf("worker_%d", worker), func(t *testing.T) {
			t.Parallel()

			// Retain previous events while subsequent calls reuse execution state.
			events := make([]*pipeline.Event, 0, 24)
			for i := range 24 {
				input := fmt.Sprintf(`{"source":{"value":"%d/%d"},"fail":%t}`, worker, i, i%2 == 0)
				event := newStateTestEvent(t, input, "fields.log")
				require.Equal(t, pipeline.ActionPass, p.Do(event))
				events = append(events, event)
			}
			for i, event := range events {
				value := fmt.Sprintf("%d/%d", worker, i)
				destination := value
				finished := ""
				if i%2 != 0 {
					destination = "updated"
					finished = `,"finished":true`
				}
				want := fmt.Sprintf(`{"saved":{"value":%q},"destination":{"value":%q},"present":true,"moved":true,"fail":%t%s}`,
					value, destination, i%2 == 0, finished)
				require.JSONEq(t, want, event.Root.EncodeToString())
			}
		})
	}
}
