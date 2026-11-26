package transform

import (
	"testing"
	"time"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/stretchr/testify/require"
)

func TestPluginCompositeOutputDuringConcurrentReuse(t *testing.T) {
	t.Parallel()

	p := newStateTestPlugin(t, t.Name(), `
		.copy = {label: .value, items: [.value, "<&>"]}
		.raw_copy = .raw
		.again = .copy
		del .again
	`)
	previous := newStateTestEvent(t, `{"value":"previous","raw":1.50}`, "previous.log")
	require.Equal(t, pipeline.ActionPass, p.Do(previous))
	wantPrevious := previous.Root.EncodeToString()
	require.JSONEq(t, `{"value":"previous","raw":1.50,"copy":{"label":"previous","items":["previous","<&>"]},"raw_copy":1.50}`, wantPrevious)

	type testCase struct {
		name  string
		input string
		want  string
	}
	tests := []testCase{
		{
			name:  "first",
			input: `{"value":"first","raw":1e3}`,
			want:  `{"value":"first","raw":1e3,"copy":{"label":"first","items":["first","<&>"]},"raw_copy":1e3}`,
		},
		{
			name:  "second",
			input: `{"value":"second","raw":-0}`,
			want:  `{"value":"second","raw":-0,"copy":{"label":"second","items":["second","<&>"]},"raw_copy":-0}`,
		},
	}
	events := make([]*pipeline.Event, len(tests))
	for i, tc := range tests {
		events[i] = newStateTestEvent(t, tc.input, tc.name)
	}
	entered := make(chan core.EvalContext, len(events))
	release := make(chan struct{})
	wrapStateTestExpr(p, 1, func(ctx core.EvalContext) {
		entered <- ctx
		<-release
	})
	results := make(chan pipeline.ActionResult, len(events))
	for _, event := range events {
		go func() { results <- p.Do(event) }()
	}

	// Both executions have already serialized one composite and still own
	// their workspaces. The previous output must survive further writes/reset.
	contexts := make([]core.EvalContext, 0, len(events))
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for range events {
		select {
		case ctx := <-entered:
			contexts = append(contexts, ctx)
		case <-results:
			close(release)
			for range len(events) - 1 {
				<-results
			}
			t.Fatal("execution returned before the composite overlap check")
		case <-timer.C:
			close(release)
			for range events {
				<-results
			}
			t.Fatal("executions did not reach the composite overlap check")
		}
	}
	close(release)
	var changed bool
	for range 100 {
		if previous.Root.EncodeToString() != wantPrevious {
			changed = true
		}
	}
	actions := make([]pipeline.ActionResult, len(events))
	for i := range actions {
		actions[i] = <-results
	}
	require.NotSame(t, contexts[0], contexts[1])
	require.False(t, changed, "previous output must own its serialized data")
	for i, action := range actions {
		require.Equal(t, pipeline.ActionPass, action)
		require.JSONEq(t, tests[i].want, events[i].Root.EncodeToString())
		require.Equal(t, events[i].Root.Dig("raw").AsString(), events[i].Root.Dig("raw_copy").AsString())
	}
	require.Equal(t, wantPrevious, previous.Root.EncodeToString())
	requireStateTestReleased(t, p)
}
