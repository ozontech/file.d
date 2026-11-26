package stdlib

import (
	"errors"
	"slices"
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/stretchr/testify/require"
)

type preparedStateContext struct {
	core.EvalContext
	state *core.CallState
}

func (c *preparedStateContext) CallState() *core.CallState { return c.state }

type preparedIndexedProbe struct {
	params []Parameter
	calls  []string
	err    error
}

func (*preparedIndexedProbe) Name() string { return "state_probe" }

func (f *preparedIndexedProbe) Params() []Parameter { return f.params }

func (f *preparedIndexedProbe) Call(args map[string]core.Value) (core.Value, error) {
	f.calls = append(f.calls, "map")
	values := make([]core.Value, len(f.params))
	for i, param := range f.params {
		values[i] = args[param.Name]
	}
	return core.ArrayValue{V: values}, f.err
}

func (f *preparedIndexedProbe) CallIndexed(args []core.Value) (core.Value, error) {
	f.calls = append(f.calls, "indexed")
	return core.ArrayValue{V: slices.Clone(args)}, f.err
}

type preparedStateProbe struct {
	preparedIndexedProbe
	state *core.CallState
}

func (f *preparedStateProbe) CallIndexedWithState(state *core.CallState, args []core.Value) (core.Value, error) {
	f.calls = append(f.calls, "state")
	f.state = state
	return core.ArrayValue{V: slices.Clone(args)}, f.err
}

func preparedStateParams() []Parameter {
	return []Parameter{
		{
			Name:          "value",
			AcceptedKinds: []core.ValueKind{core.KindString},
		},
		{
			Name:          "first",
			Default:       str("first default"),
			AcceptedKinds: []core.ValueKind{core.KindString},
		},
		{
			Name:          "second",
			Default:       str("second default"),
			AcceptedKinds: []core.ValueKind{core.KindString},
		},
	}
}

func TestPreparedCallPassesCurrentExecutionState(t *testing.T) {
	t.Parallel()

	firstState := &core.CallState{}
	secondState := &core.CallState{}
	type testCase struct {
		name string
		ctx  core.EvalContext
		want *core.CallState
	}
	tests := []testCase{
		{
			name: "first execution",
			ctx:  &preparedStateContext{state: firstState},
			want: firstState,
		},
		{
			name: "second execution",
			ctx:  &preparedStateContext{state: secondState},
			want: secondState,
		},
		{
			name: "nil state",
			ctx:  &preparedStateContext{},
		},
		{
			name: "context without state provider",
			ctx:  &struct{ core.EvalContext }{},
		},
		{
			name: "nil context",
		},
	}
	probe := &preparedStateProbe{
		preparedIndexedProbe: preparedIndexedProbe{params: preparedStateParams()},
	}
	fn, err := compile(probe)
	require.NoError(t, err)
	args := []core.Argument{
		{
			Value: &core.ConstExpr{V: str("input")},
		},
		{
			Name:  "second",
			Value: &core.ConstExpr{V: str("provided")},
		},
	}
	expr := &core.CallExpr{
		Node: core.NewNode(preparedTestPosition{}),
		Name: fn.Name(),
		Args: args,
		Plan: fn.PrepareCall(args),
	}
	require.NotNil(t, expr.Plan)
	want := core.ArrayValue{V: []core.Value{str("input"), str("first default"), str("provided")}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			probe.calls = nil

			got, callErr := expr.Eval(tt.ctx)

			require.NoError(t, callErr)
			require.Equal(t, want, got)
			require.Equal(t, []string{"state"}, probe.calls)
			if tt.want == nil {
				require.Nil(t, probe.state)
			} else {
				require.Same(t, tt.want, probe.state)
			}
		})
	}
}

func TestPreparedCallKeepsIndexedFunctions(t *testing.T) {
	t.Parallel()

	probe := &preparedIndexedProbe{params: preparedStateParams()}
	fn, err := compile(probe)
	require.NoError(t, err)
	args := []core.Argument{
		{
			Value: &core.ConstExpr{V: str("input")},
		},
	}
	expr := &core.CallExpr{
		Node: core.NewNode(preparedTestPosition{}),
		Name: fn.Name(),
		Args: args,
		Plan: fn.PrepareCall(args),
	}
	require.NotNil(t, expr.Plan)

	got, err := expr.Eval(&preparedStateContext{state: &core.CallState{}})

	require.NoError(t, err)
	require.Equal(t, core.ArrayValue{V: []core.Value{str("input"), str("first default"), str("second default")}}, got)
	require.Equal(t, []string{"indexed"}, probe.calls)
}

func TestStatefulFunctionKeepsDirectEntryPoints(t *testing.T) {
	t.Parallel()

	probe := &preparedStateProbe{
		preparedIndexedProbe: preparedIndexedProbe{params: preparedStateParams()},
	}
	fn, err := compile(probe)
	require.NoError(t, err)
	values := []core.Value{str("input"), str("first default"), str("second default")}
	resolved, err := fn.Resolve(values[:1], nil)
	require.NoError(t, err)

	indexed, indexedErr := probe.CallIndexed(values)
	general, generalErr := fn.Call(resolved)

	require.NoError(t, indexedErr)
	require.NoError(t, generalErr)
	require.Equal(t, core.ArrayValue{V: values}, indexed)
	require.Equal(t, indexed, general)
	require.Equal(t, []string{"indexed", "map"}, probe.calls)
	require.Nil(t, probe.state)
}

type preparedStateArgument struct {
	core.Node
	name  string
	order *[]string
	value core.Value
	err   error
}

func (e *preparedStateArgument) Eval(core.EvalContext) (core.Value, error) {
	*e.order = append(*e.order, e.name)
	return e.value, e.err
}

func TestPreparedStatefulCallPreservesErrorsAndEvaluationOrder(t *testing.T) {
	t.Parallel()

	argumentErr := errors.New("argument failed")
	functionErr := errors.New("operation failed")
	type testCase struct {
		name       string
		value      core.Value
		first      core.Value
		second     core.Value
		secondErr  error
		callErr    error
		wantError  string
		wantCause  error
		wantOrder  []string
		wantCalled bool
	}
	tests := []testCase{
		{
			name:       "function error is wrapped without retry",
			value:      str("input"),
			first:      str("first"),
			second:     str("second"),
			callErr:    functionErr,
			wantError:  "test:1:2: state_probe: operation failed",
			wantCause:  functionErr,
			wantOrder:  []string{"value", "second", "first"},
			wantCalled: true,
		},
		{
			name:      "type checks follow parameter order after evaluation",
			value:     str("input"),
			first:     core.IntegerValue{V: 1},
			second:    core.BoolValue{V: true},
			wantError: `test:1:2: function "state_probe": argument "first": expected string, got integer`,
			wantOrder: []string{"value", "second", "first"},
		},
		{
			name:      "argument error stops evaluation before kind checks",
			value:     core.BoolValue{V: true},
			first:     str("first"),
			second:    str("second"),
			secondErr: argumentErr,
			wantError: "argument failed",
			wantCause: argumentErr,
			wantOrder: []string{"value", "second"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			probe := &preparedStateProbe{
				preparedIndexedProbe: preparedIndexedProbe{
					params: preparedStateParams(),
					err:    tt.callErr,
				},
			}
			fn, err := compile(probe)
			require.NoError(t, err)
			var order []string
			args := []core.Argument{
				{
					Value: &preparedStateArgument{
						name:  "value",
						order: &order,
						value: tt.value,
					},
				},
				{
					Name: "second",
					Value: &preparedStateArgument{
						name:  "second",
						order: &order,
						value: tt.second,
						err:   tt.secondErr,
					},
				},
				{
					Name: "first",
					Value: &preparedStateArgument{
						name:  "first",
						order: &order,
						value: tt.first,
					},
				},
			}
			expr := &core.CallExpr{
				Node: core.NewNode(preparedTestPosition{}),
				Name: fn.Name(),
				Args: args,
				Plan: fn.PrepareCall(args),
			}
			require.NotNil(t, expr.Plan)

			got, err := expr.Eval(&preparedStateContext{state: &core.CallState{}})

			require.EqualError(t, err, tt.wantError)
			if tt.wantCause != nil {
				require.ErrorIs(t, err, tt.wantCause)
			}
			require.Equal(t, core.NullValue{}, got)
			require.Equal(t, tt.wantOrder, order)
			if tt.wantCalled {
				require.Equal(t, []string{"state"}, probe.calls)
			} else {
				require.Empty(t, probe.calls)
			}
		})
	}
}
