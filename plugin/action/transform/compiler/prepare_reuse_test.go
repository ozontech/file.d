package compiler

import (
	"fmt"
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/ozontech/file.d/plugin/action/transform/runtime"
	"github.com/ozontech/file.d/plugin/action/transform/stdlib"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/require"
)

func runCallsWithContext(t *testing.T, exprs []core.Expr, ctx *runtime.Context, target *runtime.RootTarget, input string) (core.Value, *insaneJSON.Root, error) {
	t.Helper()

	root, err := insaneJSON.DecodeString(input)
	require.NoError(t, err)
	// Keep earlier events alive while later runs reuse the same execution state.
	t.Cleanup(func() { insaneJSON.Release(root) })
	target.Root = root
	target.SourceName = "reuse.log"
	defer target.Reset()
	defer ctx.Reset()

	var value core.Value = core.NullValue{}
	for _, expr := range exprs {
		value, err = expr.Eval(ctx)
		if err != nil {
			break
		}
	}
	return value, root, err
}

func TestCallFramesReusePreservesResults(t *testing.T) {
	t.Parallel()

	const source = `
		first = upcase(before(.value, "-"));
		%first = first
		groups = parse_regex_all(.value, r'([a-z]+)[-!]', limit: 1, group: 1)
		all = parse_regex_all(.value, r'([a-z]+)[-!]')
		fallback = lookup("missing", {}, default: {saved: first})
		unchanged = lookup(.value, {})
		tail = slice(.value, 1, end: 3)
		full = slice(.value, 1)
		nested = join([first, upcase(between(.value, "-", "!"))], ":")
		.out = [first, %first, groups, all, fallback, unchanged, tail, full, nested];
		[fallback, groups]
	`

	type testCase struct {
		name     string
		prepared bool
	}

	tests := []testCase{
		{
			name:     "general",
			prepared: false,
		},
		{
			name:     "prepared",
			prepared: true,
		},
	}

	type eventCase struct {
		input     string
		wantEvent string
		first     string
		group     string
	}

	events := []eventCase{
		{
			input:     `{"value":"alpha-beta!"}`,
			wantEvent: `{"value":"alpha-beta!","out":["ALPHA","ALPHA",["alpha"],["alpha-","beta!"],{"saved":"ALPHA"},"alpha-beta!","lp","lpha-beta!","ALPHA:BETA"]}`,
			first:     "ALPHA",
			group:     "alpha",
		},
		{
			input:     `{"value":"gamma-delta!"}`,
			wantEvent: `{"value":"gamma-delta!","out":["GAMMA","GAMMA",["gamma"],["gamma-","delta!"],{"saved":"GAMMA"},"gamma-delta!","am","amma-delta!","GAMMA:DELTA"]}`,
			first:     "GAMMA",
			group:     "gamma",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			exprs := compileCalls(t, source, tc.prepared)
			target := runtime.NewRootTarget(nil, "", nil)
			ctx := runtime.NewContext(target, stdlib.GetRegistry())
			var results []core.Value
			var roots []*insaneJSON.Root
			for range 2 {
				for _, event := range events {
					value, root, err := runCallsWithContext(t, exprs, ctx, target, event.input)
					require.NoError(t, err)
					results = append(results, value)
					roots = append(roots, root)
				}
			}

			for i, result := range results {
				event := events[i%len(events)]
				want := core.ArrayValue{V: []core.Value{
					core.ObjectValue{V: map[string]core.Value{"saved": core.StringValue{V: event.first}}},
					core.ArrayValue{V: []core.Value{core.StringValue{V: event.group}}},
				}}
				require.Equal(t, want, result, "returned collections must survive later calls and resets")
				require.JSONEq(t, event.wantEvent, roots[i].EncodeToString())
			}
		})
	}
}

func TestCallFramesReuseAfterErrors(t *testing.T) {
	t.Parallel()

	const source = `
		.saved = upcase(before(.value, "-"))
		.out = join(parse_regex_all(.value, r'([a-z]+)', limit: .limit / .divisor, group: .group), ":")
	`
	general := compileCalls(t, source, false)
	prepared := compileCalls(t, source, true)
	generalTarget := runtime.NewRootTarget(nil, "", nil)
	preparedTarget := runtime.NewRootTarget(nil, "", nil)
	generalContext := runtime.NewContext(generalTarget, stdlib.GetRegistry())
	preparedContext := runtime.NewContext(preparedTarget, stdlib.GetRegistry())

	type testCase struct {
		name    string
		input   string
		wantErr string
	}

	tests := []testCase{
		{
			name:    "argument_evaluation",
			input:   `{"value":"alpha-beta!","limit":2,"divisor":0,"group":1}`,
			wantErr: "division by zero",
		},
		{
			name:    "argument_validation",
			input:   `{"value":"alpha-beta!","limit":2,"divisor":1,"group":false}`,
			wantErr: `argument "group": expected integer, got bool`,
		},
		{
			name:    "function_execution",
			input:   `{"value":"alpha-beta!","limit":2,"divisor":1,"group":2}`,
			wantErr: "group 2 is out of range",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			generalValue, generalRoot, generalErr := runCallsWithContext(t, general, generalContext, generalTarget, tc.input)
			preparedValue, preparedRoot, preparedErr := runCallsWithContext(t, prepared, preparedContext, preparedTarget, tc.input)
			require.ErrorContains(t, generalErr, tc.wantErr)
			require.EqualError(t, preparedErr, generalErr.Error())
			require.Equal(t, generalValue, preparedValue)
			require.JSONEq(t, generalRoot.EncodeToString(), preparedRoot.EncodeToString())
			require.Equal(t, "ALPHA", generalRoot.Dig("saved").AsString())
			require.Nil(t, generalRoot.Dig("out"))
			failedEvent := generalRoot.EncodeToString()

			const input = `{"value":"gamma-delta!","limit":2,"divisor":1,"group":1}`
			const want = `{"value":"gamma-delta!","limit":2,"divisor":1,"group":1,"saved":"GAMMA","out":"gamma:delta"}`
			generalValue, nextGeneral, generalErr := runCallsWithContext(t, general, generalContext, generalTarget, input)
			preparedValue, nextPrepared, preparedErr := runCallsWithContext(t, prepared, preparedContext, preparedTarget, input)
			require.NoError(t, generalErr)
			require.NoError(t, preparedErr)
			require.Equal(t, core.StringValue{V: "gamma:delta"}, generalValue)
			require.Equal(t, generalValue, preparedValue)
			require.JSONEq(t, want, nextGeneral.EncodeToString())
			require.JSONEq(t, want, nextPrepared.EncodeToString())
			require.JSONEq(t, failedEvent, generalRoot.EncodeToString())
			require.JSONEq(t, failedEvent, preparedRoot.EncodeToString())
		})
	}
}

func TestCallFramesSharedProgramIndependentContexts(t *testing.T) {
	t.Parallel()

	const source = `
		.first = parse_regex_all(.value, r'\d+', limit: 1, group: 0)
		.out = join([
			upcase(before(.value, "-tail")),
			join(parse_regex_all(.value, r'\d+'), ":"),
			slice(.value, 0, end: 1)
		], "/")
	`

	type testCase struct {
		name     string
		prepared bool
	}

	tests := []testCase{
		{
			name:     "general",
			prepared: false,
		},
		{
			name:     "prepared",
			prepared: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			exprs := compileCalls(t, source, tc.prepared)
			for worker := range 8 {
				t.Run(fmt.Sprintf("worker_%d", worker), func(t *testing.T) {
					t.Parallel()

					target := runtime.NewRootTarget(nil, "", nil)
					ctx := runtime.NewContext(target, stdlib.GetRegistry())
					var roots []*insaneJSON.Root
					var outputs []string
					for event := range 12 {
						input := fmt.Sprintf(`{"value":"worker%d-event%d-tail"}`, worker, event)
						_, root, err := runCallsWithContext(t, exprs, ctx, target, input)
						require.NoError(t, err)
						want := fmt.Sprintf(`{"value":"worker%[1]d-event%[2]d-tail","first":["%[1]d"],"out":"WORKER%[1]d-EVENT%[2]d/%[1]d:%[2]d/w"}`, worker, event)
						require.JSONEq(t, want, root.EncodeToString())
						roots = append(roots, root)
						outputs = append(outputs, want)
					}
					for i, root := range roots {
						require.JSONEq(t, outputs[i], root.EncodeToString())
					}
				})
			}
		})
	}
}
