package transform

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/ozontech/file.d/plugin/action/transform/runtime"
	"github.com/stretchr/testify/require"
)

func TestPluginJoinResultsSurviveReuse(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name     string
		ending   string
		finished bool
	}
	tests := []testCase{
		{
			name:     "success",
			ending:   `.finished = true`,
			finished: true,
		},
		{
			name: "error",
			ending: `
				.failed = join(["prefix", 7], ",")
				.finished = true
			`,
		},
		{
			name: "abort",
			ending: `
				abort
				.finished = true
			`,
		},
	}
	largeParts := make([]string, 4096)
	for i := range largeParts {
		largeParts[i] = fmt.Sprintf("large-%d", i)
	}
	inputs := []stateJoinTestInput{
		{
			Parts:     largeParts,
			Tail:      []string{"large", "tail"},
			Separator: "/",
		},
		{
			Parts:     []string{"small", "value"},
			Tail:      []string{"small", "tail"},
			Separator: ":",
		},
		{
			Parts:     []string{"singleton"},
			Tail:      []string{"last"},
			Separator: "unused",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := newStateTestPlugin(t, t.Name(), stateJoinTestSource+tt.ending)
			state := p.state.Load()
			events := make([]*pipeline.Event, len(inputs))
			for i, input := range inputs {
				events[i] = newStateJoinTestEvent(t, fmt.Sprintf("event_%d", i), input)
				require.Equal(t, pipeline.ActionPass, p.Do(events[i]))
				require.Same(t, state, p.state.Load())
				requireStateTestReleased(t, p)
			}

			// Read every result after later calls have reused and reset the
			// workspace, including the singleton fast path in strings.Join.
			for i, event := range events {
				requireStateJoinTestOutput(t, event, inputs[i])
				require.Equal(t, tt.finished, event.Root.Dig("finished").AsBool())
				require.Nil(t, event.Root.Dig("failed"))
			}
		})
	}
}

func TestPluginJoinOutputDuringConcurrentReuse(t *testing.T) {
	t.Parallel()

	p := newStateTestPlugin(t, t.Name(), stateJoinTestSource)
	other := newStateTestPlugin(t, t.Name(), stateJoinTestSource)
	require.Same(t, p.program.expressions[0], other.program.expressions[0])
	previousInput := stateJoinTestInput{
		Parts:     []string{"previous", "result"},
		Tail:      []string{"previous", "tail"},
		Separator: ":",
	}
	previous := newStateJoinTestEvent(t, "previous", previousInput)
	require.Equal(t, pipeline.ActionPass, p.Do(previous))
	wantPrevious := previous.Root.EncodeToString()

	plugins := []*Plugin{p, p, other}
	events := make([]*pipeline.Event, len(plugins))
	inputs := make([]stateJoinTestInput, len(plugins))
	for i := range plugins {
		parts := make([]string, 64)
		for j := range parts {
			parts[j] = fmt.Sprintf("worker-%d-part-%d", i, j)
		}
		inputs[i] = stateJoinTestInput{
			Parts:     parts,
			Tail:      []string{fmt.Sprintf("worker-%d", i), "tail"},
			Separator: ":",
		}
		events[i] = newStateJoinTestEvent(t, fmt.Sprintf("worker_%d", i), inputs[i])
	}
	entered := make(chan core.EvalContext, len(plugins))
	release := make(chan struct{})
	before := func(ctx core.EvalContext) {
		entered <- ctx
		<-release
	}
	wrapStateTestExpr(p, 1, before)
	wrapStateTestExpr(other, 1, before)
	results := make(chan pipeline.ActionResult, len(plugins))
	for i, plugin := range plugins {
		go func() { results <- plugin.Do(events[i]) }()
	}

	// Each execution has a joined local before any nested join starts.
	// Two calls share the plugin; the third shares its prepared program.
	contexts := make([]core.EvalContext, 0, len(plugins))
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for range plugins {
		select {
		case ctx := <-entered:
			contexts = append(contexts, ctx)
		case <-results:
			close(release)
			for range len(plugins) - 1 {
				<-results
			}
			t.Fatal("execution returned before the join overlap check")
		case <-timer.C:
			close(release)
			for range plugins {
				<-results
			}
			t.Fatal("executions did not reach the join overlap check")
		}
	}
	close(release)
	var changed bool
	for range 1000 {
		if previous.Root.EncodeToString() != wantPrevious {
			changed = true
		}
	}
	actions := make([]pipeline.ActionResult, len(plugins))
	for i := range actions {
		actions[i] = <-results
	}
	require.NotSame(t, contexts[0], contexts[1])
	require.NotSame(t, contexts[0], contexts[2])
	require.NotSame(t, contexts[1], contexts[2])
	require.False(t, changed, "previous output must not depend on join scratch storage")
	require.Equal(t, wantPrevious, previous.Root.EncodeToString())
	for i, event := range events {
		require.Equal(t, pipeline.ActionPass, actions[i])
		requireStateJoinTestOutput(t, event, inputs[i])
	}
	requireStateTestReleased(t, p)
	requireStateTestReleased(t, other)
}

func TestPluginJoinResultsSurviveReentrantCall(t *testing.T) {
	t.Parallel()

	p := newStateTestPlugin(t, t.Name(), stateJoinTestSource)
	outerInput := stateJoinTestInput{
		Parts:     []string{"outer", "result"},
		Tail:      []string{"outer", "tail"},
		Separator: ":",
	}
	innerInput := stateJoinTestInput{
		Parts:     []string{"inner", "result"},
		Tail:      []string{"inner", "tail"},
		Separator: "/",
	}
	outer := newStateJoinTestEvent(t, "outer", outerInput)
	inner := newStateJoinTestEvent(t, "inner", innerInput)
	var outerContext, innerContext core.EvalContext
	var innerResult pipeline.ActionResult
	wrapStateTestExpr(p, 1, func(ctx core.EvalContext) {
		if ctx.GetTarget().(*runtime.RootTarget).SourceName == "outer" {
			outerContext = ctx
			innerResult = p.Do(inner)
		} else {
			innerContext = ctx
		}
	})

	require.Equal(t, pipeline.ActionPass, p.Do(outer))
	require.Equal(t, pipeline.ActionPass, innerResult)
	require.NotNil(t, outerContext)
	require.NotNil(t, innerContext)
	require.NotSame(t, outerContext, innerContext)
	requireStateJoinTestOutput(t, outer, outerInput)
	requireStateJoinTestOutput(t, inner, innerInput)
	requireStateTestReleased(t, p)
}

const stateJoinTestSource = `
	first = join(.parts, .separator);
	%saved = first
	items = [first, join([join(.tail, "/"), first], ":")]
	.out = {first: first, items: items, saved: %saved}
	.again = join(.tail, "-")
`

type stateJoinTestInput struct {
	Parts     []string `json:"parts"`
	Tail      []string `json:"tail"`
	Separator string   `json:"separator"`
}

func newStateJoinTestEvent(t *testing.T, name string, input stateJoinTestInput) *pipeline.Event {
	t.Helper()
	encoded, err := json.Marshal(input)
	require.NoError(t, err)
	return newStateTestEvent(t, string(encoded), name)
}

func requireStateJoinTestOutput(t *testing.T, event *pipeline.Event, input stateJoinTestInput) {
	t.Helper()
	first := strings.Join(input.Parts, input.Separator)
	want, err := json.Marshal(map[string]any{
		"first": first,
		"items": []string{first, strings.Join(input.Tail, "/") + ":" + first},
		"saved": first,
	})
	require.NoError(t, err)
	require.JSONEq(t, string(want), event.Root.Dig("out").EncodeToString())
	require.Equal(t, strings.Join(input.Tail, "-"), event.Root.Dig("again").AsString())
}
