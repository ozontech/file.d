package core

import (
	"errors"
	"slices"
	"testing"

	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/require"
)

type callArgumentExpr struct {
	Node
	eval func() (Value, error)
}

func (e *callArgumentExpr) Eval(EvalContext) (Value, error) { return e.eval() }

// callPlanContext records evaluation order and arguments for either call path.
type callPlanContext struct {
	*mockContext
	steps         []string
	args          []Value
	result        Value
	err           error
	fallbackCalls int
}

func (c *callPlanContext) call(_ Position, args []Value) (Value, error) {
	c.steps = append(c.steps, "call")
	c.args = slices.Clone(args)
	return c.result, c.err
}

func (c *callPlanContext) CallFunc(pos Position, _ string, args []Value, _ map[string]Value) (Value, error) {
	c.fallbackCalls++
	return c.call(pos, args)
}

func TestCallPlanMaterializesInSourceOrder(t *testing.T) {
	tests := []struct {
		name     string
		prepared bool
		input    string
		want     Value
	}{
		{
			name:     "general string",
			prepared: false,
			input:    `{"value":"original"}`,
			want:     StringValue{V: "original"},
		},
		{
			name:     "prepared string",
			prepared: true,
			input:    `{"value":"original"}`,
			want:     StringValue{V: "original"},
		},
		{
			name:     "general array",
			prepared: false,
			input:    `{"value":["original"]}`,
			want:     ArrayValue{V: []Value{StringValue{V: "original"}}},
		},
		{
			name:     "prepared array",
			prepared: true,
			input:    `{"value":["original"]}`,
			want:     ArrayValue{V: []Value{StringValue{V: "original"}}},
		},
		{
			name:     "general object",
			prepared: false,
			input:    `{"value":{"key":"original"}}`,
			want:     ObjectValue{V: map[string]Value{"key": StringValue{V: "original"}}},
		},
		{
			name:     "prepared object",
			prepared: true,
			input:    `{"value":{"key":"original"}}`,
			want:     ObjectValue{V: map[string]Value{"key": StringValue{V: "original"}}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			root, err := insaneJSON.DecodeString(tt.input)
			require.NoError(t, err)
			defer insaneJSON.Release(root)
			node := root.Dig("value")
			ctx := &callPlanContext{mockContext: newMockCtx(), result: tt.want}
			expr := &CallExpr{Node: n(), Name: "snapshot", Args: []Argument{
				{Value: &callArgumentExpr{Node: n(), eval: func() (Value, error) {
					ctx.steps = append(ctx.steps, "first")
					return JSONNodeValue{N: node}, nil
				}}},
				{Value: &callArgumentExpr{Node: n(), eval: func() (Value, error) {
					ctx.steps = append(ctx.steps, "second")
					node.MutateToString("changed")
					return StringValue{V: "second"}, nil
				}}},
			}}
			if tt.prepared {
				expr.Plan = NewCallPlan([]int{0, 1}, make([]Value, 2), ctx.call)
			}

			value, err := expr.Eval(ctx)

			require.NoError(t, err)
			require.Equal(t, tt.want, value)
			require.Equal(t, []Value{tt.want, StringValue{V: "second"}}, ctx.args)
			require.Equal(t, []string{"first", "second", "call"}, ctx.steps)
		})
	}
}

func TestCallPlanStopsOnError(t *testing.T) {
	evalErr := errors.New("argument failed")
	tests := []struct {
		name     string
		prepared bool
		err      error
	}{
		{
			name:     "general evaluation error",
			prepared: false,
			err:      evalErr,
		},
		{
			name:     "prepared evaluation error",
			prepared: true,
			err:      evalErr,
		},
		{
			name:     "general abort",
			prepared: false,
			err:      AbortError,
		},
		{
			name:     "prepared abort",
			prepared: true,
			err:      AbortError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := &callPlanContext{mockContext: newMockCtx()}
			expr := &CallExpr{Node: n(), Name: "unreachable", Args: []Argument{
				{Value: &callArgumentExpr{Node: n(), eval: func() (Value, error) {
					ctx.steps = append(ctx.steps, "first")
					return NullValue{}, tt.err
				}}},
				{Value: &callArgumentExpr{Node: n(), eval: func() (Value, error) {
					ctx.steps = append(ctx.steps, "second")
					return StringValue{V: "second"}, nil
				}}},
			}}
			if tt.prepared {
				expr.Plan = NewCallPlan([]int{0, 1}, make([]Value, 2), ctx.call)
			}

			value, err := expr.Eval(ctx)

			require.ErrorIs(t, err, tt.err)
			require.Equal(t, NullValue{}, value)
			require.Equal(t, []string{"first"}, ctx.steps)
			require.Zero(t, ctx.fallbackCalls)
		})
	}
}

func TestCallPlanDoesNotRetryFailedCall(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{
			name: "function error",
			err:  errors.New("call failed"),
		},
		{
			name: "abort",
			err:  AbortError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := &callPlanContext{mockContext: newMockCtx(), result: NullValue{}, err: tt.err}
			expr := &CallExpr{Node: n(), Name: "fail", Plan: NewCallPlan(nil, nil, ctx.call)}

			value, err := expr.Eval(ctx)

			require.ErrorIs(t, err, tt.err)
			require.Equal(t, NullValue{}, value)
			require.Equal(t, []string{"call"}, ctx.steps)
			require.Zero(t, ctx.fallbackCalls, "retried through the general path")
		})
	}
}
