package core

import (
	"errors"
	"testing"

	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/require"
)

type reusableCallPlanContext struct {
	*callPlanContext
	state *CallState
}

func (c *reusableCallPlanContext) CallState() *CallState { return c.state }

func TestCallPlanReleasesFrames(t *testing.T) {
	t.Parallel()

	argumentErr := errors.New("argument failed")
	callErr := errors.New("call failed")
	type testCase struct {
		name            string
		argumentErr     error
		callErr         error
		panicInArgument bool
		panicInCall     bool
		wantErr         error
	}
	tests := []testCase{
		{
			name: "success",
		},
		{
			name:        "argument error",
			argumentErr: argumentErr,
			wantErr:     argumentErr,
		},
		{
			name:        "argument abort",
			argumentErr: AbortError,
			wantErr:     AbortError,
		},
		{
			name:    "call error",
			callErr: callErr,
			wantErr: callErr,
		},
		{
			name:    "call abort",
			callErr: AbortError,
			wantErr: AbortError,
		},
		{
			name:            "argument panic",
			panicInArgument: true,
		},
		{
			name:        "call panic",
			panicInCall: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := &reusableCallPlanContext{
				callPlanContext: &callPlanContext{mockContext: newMockCtx()},
				state:           &CallState{},
			}
			result := ObjectValue{V: map[string]Value{"saved": StringValue{V: "original"}}}
			failed := false
			inner := &CallExpr{
				Node: n(),
				Args: []Argument{
					{
						Value: &ConstExpr{V: result},
					},
					{
						Value: &callArgumentExpr{
							Node: n(),
							eval: func() (Value, error) {
								if !failed && tc.panicInArgument {
									panic("argument panic")
								}
								if !failed {
									return NullValue{}, tc.argumentErr
								}
								return NullValue{}, nil
							},
						},
					},
				},
				Plan: NewStatefulCallPlan([]int{0, 1}, make([]Value, 2), func(state *CallState, _ Position, args []Value) (Value, error) {
					require.Same(t, ctx.state, state)
					if !failed && tc.panicInCall {
						panic("call panic")
					}
					if !failed {
						return args[0], tc.callErr
					}
					return args[0], nil
				}),
			}
			outer := &CallExpr{
				Node: n(),
				Args: []Argument{{Value: inner}},
				Plan: NewCallPlan([]int{0}, make([]Value, 1), ctx.call),
			}
			if tc.panicInArgument || tc.panicInCall {
				require.Panics(t, func() { _, _ = outer.Eval(ctx) })
			} else {
				_, err := outer.Eval(ctx)
				require.ErrorIs(t, err, tc.wantErr)
			}
			requireReleasedCallFrames(t, ctx.state)
			require.Zero(t, ctx.fallbackCalls)

			failed = true
			value, err := inner.Eval(ctx)
			require.NoError(t, err)
			require.Equal(t, result, value)
			requireReleasedCallFrames(t, ctx.state)
			ctx.state.Reset()
			require.Equal(t, StringValue{V: "original"}, value.(ObjectValue).V["saved"])
		})
	}
}

func TestCallPlanReusedFramesMaterializeInSourceOrder(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name  string
		input string
		want  Value
	}
	tests := []testCase{
		{
			name:  "string",
			input: `{"value":"original"}`,
			want:  StringValue{V: "original"},
		},
		{
			name:  "array",
			input: `{"value":["original"]}`,
			want:  ArrayValue{V: []Value{StringValue{V: "original"}}},
		},
		{
			name:  "object",
			input: `{"value":{"key":"original"}}`,
			want:  ObjectValue{V: map[string]Value{"key": StringValue{V: "original"}}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := &reusableCallPlanContext{
				callPlanContext: &callPlanContext{mockContext: newMockCtx()},
				state:           &CallState{},
			}
			for range 3 {
				root, err := insaneJSON.DecodeString(tc.input)
				require.NoError(t, err)
				t.Cleanup(func() { insaneJSON.Release(root) })
				node := root.Dig("value")
				ctx.steps = nil
				expr := &CallExpr{
					Node: n(),
					Args: []Argument{
						{
							Name: "second",
							Value: &callArgumentExpr{Node: n(), eval: func() (Value, error) {
								ctx.steps = append(ctx.steps, "first")
								return JSONNodeValue{N: node}, nil
							}},
						},
						{
							Name: "first",
							Value: &callArgumentExpr{Node: n(), eval: func() (Value, error) {
								ctx.steps = append(ctx.steps, "second")
								node.MutateToString("changed")
								return StringValue{V: "second"}, nil
							}},
						},
					},
					Plan: NewCallPlan([]int{1, 0}, make([]Value, 2), ctx.call),
				}

				_, err = expr.Eval(ctx)

				require.NoError(t, err)
				require.Equal(t, []Value{StringValue{V: "second"}, tc.want}, ctx.args)
				require.Equal(t, []string{"first", "second", "call"}, ctx.steps)
				requireReleasedCallFrames(t, ctx.state)
			}
		})
	}
}

func requireReleasedCallFrames(t *testing.T, state *CallState) {
	t.Helper()

	require.Zero(t, state.depth)
	for _, frame := range state.frames {
		for _, value := range frame[:cap(frame)] {
			require.Nil(t, value)
		}
	}
}

func TestCallPlanRestoresDefaultsOnReusedFrames(t *testing.T) {
	t.Parallel()

	ctx := &reusableCallPlanContext{
		callPlanContext: &callPlanContext{
			mockContext: newMockCtx(),
			clearArgs:   true,
		},
		state: &CallState{},
	}
	defaults := []Value{StringValue{V: "default"}, nil}
	expr := &CallExpr{
		Node: n(),
		Args: []Argument{{Value: &ConstExpr{V: StringValue{V: "input"}}}},
		Plan: NewCallPlan([]int{1}, defaults, ctx.call),
	}
	for range 3 {
		_, err := expr.Eval(ctx)
		require.NoError(t, err)
		require.Equal(t, []Value{StringValue{V: "default"}, StringValue{V: "input"}}, ctx.args)
		require.Equal(t, []Value{StringValue{V: "default"}, nil}, defaults)
		requireReleasedCallFrames(t, ctx.state)
	}
}

func TestCallPlanNestedResultSurvivesNextArgument(t *testing.T) {
	t.Parallel()

	ctx := &reusableCallPlanContext{
		callPlanContext: &callPlanContext{mockContext: newMockCtx()},
		state:           &CallState{},
	}
	plan := NewCallPlan([]int{0}, make([]Value, 1), func(_ Position, args []Value) (Value, error) {
		return args[0], ctx.err
	})
	want := ArrayValue{V: []Value{StringValue{V: "first"}}}
	expr := &CallExpr{
		Node: n(),
		Args: []Argument{
			{
				Value: &CallExpr{
					Node: n(),
					Args: []Argument{{Value: &ConstExpr{V: want}}},
					Plan: plan,
				},
			},
			{
				Value: &CallExpr{
					Node: n(),
					Args: []Argument{{Value: &ConstExpr{V: ObjectValue{V: map[string]Value{"next": NullValue{}}}}}},
					Plan: plan,
				},
			},
		},
		Plan: NewCallPlan([]int{0, 1}, make([]Value, 2), ctx.call),
	}
	for range 3 {
		_, err := expr.Eval(ctx)
		require.NoError(t, err)
		require.Equal(t, want, ctx.args[0])
		requireReleasedCallFrames(t, ctx.state)
	}
}
