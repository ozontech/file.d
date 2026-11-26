package transform

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/ozontech/file.d/plugin/action/transform/runtime"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
)

func TestPluginStateIsolationAfterReturn(t *testing.T) {
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
				.failed = before(null, "-")
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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			source := fmt.Sprintf(`
				if .seed {
					local = {"text": trim(.value, " "), "items": [.value]};
					%%local = trim(.value, " ")
					for index, item in [.value] {
						.out = local
						.meta = %%local
						%s
					}
				} else {
					.out = local
					.meta = %%local
					.index = index
					.item = item
				}
			`, tt.ending)
			p := newStateTestPlugin(t, t.Name(), source)
			state := p.state.Load()
			first := newStateTestEvent(t, `{"seed":true,"value":" first "}`, "first.log")
			second := newStateTestEvent(t, `{}`, "second.log")
			third := newStateTestEvent(t, `{"seed":true,"value":" third "}`, "third.log")

			require.Equal(t, pipeline.ActionPass, p.Do(first))
			require.Same(t, state, p.state.Load(), "sequential calls must reuse their state")
			requireStateTestReleased(t, p)
			require.Equal(t, pipeline.ActionPass, p.Do(second))
			require.Equal(t, pipeline.ActionPass, p.Do(third))

			finished := ""
			if tt.finished {
				finished = `,"finished":true`
			}
			require.JSONEq(t, `{"seed":true,"value":" first ","out":{"text":"first","items":[" first "]},"meta":"first"`+finished+`}`, first.Root.EncodeToString())
			require.JSONEq(t, `{"out":null,"meta":null,"index":null,"item":null}`, second.Root.EncodeToString())
			require.JSONEq(t, `{"seed":true,"value":" third ","out":{"text":"third","items":[" third "]},"meta":"third"`+finished+`}`, third.Root.EncodeToString())
			require.Same(t, state, p.state.Load())
			requireStateTestReleased(t, p)
		})
	}
}

func TestPluginStatesSharePreparedProgram(t *testing.T) {
	t.Parallel()

	const source = `
		local = trim(.value, " ");
		%local = local
		.out = join([upcase(local), before(%local, "-")], ":")
		.parts = parse_regex_all(local, r'(\d+)', limit: 1, group: 1)
	`
	plugins := make([]*Plugin, 4)
	for i := range plugins {
		plugins[i] = newStateTestPlugin(t, t.Name(), source)
		if i > 0 {
			require.Same(t, plugins[0].program.expressions[0], plugins[i].program.expressions[0])
			require.NotSame(t, plugins[0].state.Load(), plugins[i].state.Load())
		}
	}
	assign, ok := plugins[0].program.expressions[0].(*core.AssignExpr)
	require.True(t, ok)
	call, ok := assign.Value.(*core.CallExpr)
	require.True(t, ok)
	require.NotNil(t, call.Plan)

	for i, p := range plugins {
		t.Run(fmt.Sprintf("worker_%d", i), func(t *testing.T) {
			t.Parallel()

			for j := range 12 {
				input := fmt.Sprintf(`{"value":" w%d-%d "}`, i, j)
				event := newStateTestEvent(t, input, fmt.Sprintf("worker_%d.log", i))
				require.Equal(t, pipeline.ActionPass, p.Do(event))
				want := fmt.Sprintf(`{"value":" w%d-%d ","out":"W%d-%d:w%d","parts":["%d"]}`, i, j, i, j, i, i)
				require.JSONEq(t, want, event.Root.EncodeToString())
			}
			requireStateTestReleased(t, p)
		})
	}
}

func TestPluginStateConcurrentCalls(t *testing.T) {
	t.Parallel()

	p := newStateTestPlugin(t, t.Name(), stateTestOverlapSource)
	entered := make(chan core.EvalContext, 2)
	release := make(chan struct{})
	wrapStateTestExpr(p, 2, func(ctx core.EvalContext) {
		entered <- ctx
		<-release
	})
	first := newStateTestEvent(t, `{"value":"first"}`, "first.log")
	second := newStateTestEvent(t, `{"value":"second"}`, "second.log")
	results := make(chan pipeline.ActionResult, 2)
	go func() { results <- p.Do(first) }()
	go func() { results <- p.Do(second) }()

	contexts := make([]core.EvalContext, 0, 2)
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for range 2 {
		select {
		case ctx := <-entered:
			contexts = append(contexts, ctx)
		case <-timer.C:
			close(release)
			<-results
			<-results
			t.Fatal("both calls must enter evaluation before either call returns")
		}
	}
	close(release)
	firstResult, secondResult := <-results, <-results

	require.NotSame(t, contexts[0], contexts[1])
	require.Equal(t, pipeline.ActionPass, firstResult)
	require.Equal(t, pipeline.ActionPass, secondResult)
	require.JSONEq(t, `{"value":"first","out":"FIRST","meta":"first"}`, first.Root.EncodeToString())
	require.JSONEq(t, `{"value":"second","out":"SECOND","meta":"second"}`, second.Root.EncodeToString())
	requireStateTestReleased(t, p)
}

func TestPluginStateReentrantCall(t *testing.T) {
	t.Parallel()

	p := newStateTestPlugin(t, t.Name(), stateTestOverlapSource)
	outer := newStateTestEvent(t, `{"value":"outer"}`, "outer.log")
	inner := newStateTestEvent(t, `{"value":"inner"}`, "inner.log")
	var outerContext, innerContext core.EvalContext
	var innerResult pipeline.ActionResult
	wrapStateTestExpr(p, 2, func(ctx core.EvalContext) {
		target := ctx.GetTarget().(*runtime.RootTarget)
		if target.SourceName == "outer.log" {
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
	require.JSONEq(t, `{"value":"outer","out":"OUTER","meta":"outer"}`, outer.Root.EncodeToString())
	require.JSONEq(t, `{"value":"inner","out":"INNER","meta":"inner"}`, inner.Root.EncodeToString())
	requireStateTestReleased(t, p)
}

func TestPluginStatePreviousOutputDuringReuse(t *testing.T) {
	t.Parallel()

	p := newStateTestPlugin(t, t.Name(), stateTestOverlapSource)
	first := newStateTestEvent(t, `{"value":"first"}`, "first.log")
	next := newStateTestEvent(t, `{"value":"next"}`, "next.log")
	require.Equal(t, pipeline.ActionPass, p.Do(first))

	entered := make(chan struct{})
	release := make(chan struct{})
	wrapStateTestExpr(p, 2, func(core.EvalContext) {
		close(entered)
		<-release
	})
	result := make(chan pipeline.ActionResult, 1)
	go func() { result <- p.Do(next) }()

	// The next event already owns the reused workspace while the previous
	// output is read. Continue reading during the remaining writes and reset.
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-entered:
	case <-result:
		close(release)
		t.Fatal("the next event returned before reaching the overlap check")
	case <-timer.C:
		close(release)
		<-result
		t.Fatal("the next event did not reach the overlap check")
	}
	close(release)
	var changed bool
	for range 100 {
		if first.Root.Dig("out").AsString() != "FIRST" || first.Root.Dig("meta").AsString() != "first" {
			changed = true
			break
		}
	}
	nextResult := <-result
	require.False(t, changed, "the previous output must not depend on reusable state")
	require.Equal(t, pipeline.ActionPass, nextResult)
	require.JSONEq(t, `{"value":"first","out":"FIRST","meta":"first"}`, first.Root.EncodeToString())
	require.JSONEq(t, `{"value":"next","out":"NEXT","meta":"next"}`, next.Root.EncodeToString())
	requireStateTestReleased(t, p)
}

func TestPluginStateCleanupAfterPanic(t *testing.T) {
	t.Parallel()

	p := newStateTestPlugin(t, t.Name(), `
		if .seed {
			local = .value;
			%local = trim(.value, " ")
		}
		.out = local
		.meta = %local
	`)
	state := p.state.Load()
	panicNext := true
	wrapStateTestExpr(p, 1, func(core.EvalContext) {
		if panicNext {
			panicNext = false
			panic("evaluation panic")
		}
	})
	first := newStateTestEvent(t, `{"seed":true,"value":"first"}`, "first.log")
	second := newStateTestEvent(t, `{}`, "second.log")

	require.PanicsWithValue(t, "evaluation panic", func() { p.Do(first) })
	require.Same(t, state, p.state.Load())
	requireStateTestReleased(t, p)
	require.Equal(t, pipeline.ActionPass, p.Do(second))
	require.JSONEq(t, `{"out":null,"meta":null}`, second.Root.EncodeToString())
	requireStateTestReleased(t, p)
}

const stateTestOverlapSource = `
	local = .value;
	%local = trim(.value, " ")
	.out = upcase(local)
	.meta = %local
`

type stateTestExpr struct {
	core.Expr
	before func(core.EvalContext)
}

func (e stateTestExpr) Eval(ctx core.EvalContext) (core.Value, error) {
	e.before(ctx)
	return e.Expr.Eval(ctx)
}

func wrapStateTestExpr(p *Plugin, index int, before func(core.EvalContext)) {
	expressions := slices.Clone(p.program.expressions)
	expressions[index] = stateTestExpr{
		Expr:   expressions[index],
		before: before,
	}
	p.program = Program{expressions: expressions}
}

func newStateTestPlugin(t *testing.T, pipelineName, source string) *Plugin {
	t.Helper()
	p := &Plugin{}
	p.Start(&Config{Source: source}, &pipeline.ActionPluginParams{
		PluginDefaultParams: pipeline.PluginDefaultParams{PipelineName: pipelineName},
		Logger:              zaptest.NewLogger(t, zaptest.WrapOptions(zap.WithFatalHook(zapcore.WriteThenPanic))).Sugar(),
	})
	t.Cleanup(p.Stop)
	return p
}

func newStateTestEvent(t *testing.T, input, sourceName string) *pipeline.Event {
	t.Helper()
	root, err := insaneJSON.DecodeString(input)
	require.NoError(t, err)
	t.Cleanup(func() { insaneJSON.Release(root) })
	return &pipeline.Event{
		Root:       root,
		SourceName: sourceName,
	}
}

func requireStateTestReleased(t *testing.T, p *Plugin) {
	t.Helper()
	state := p.state.Load()
	require.NotNil(t, state)
	require.Nil(t, state.target.Root)
	require.Empty(t, state.target.SourceName)
}
