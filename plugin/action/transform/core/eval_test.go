package core

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockPos struct{ s string }

func (p mockPos) String() string { return p.s }

var defaultPos Position = mockPos{"test:1:1"}

func n() Node { return NewNode(defaultPos) }

type mockTarget struct {
	store map[string]Value
	err   error
}

func newMockTarget() *mockTarget {
	return &mockTarget{store: make(map[string]Value)}
}

func (t *mockTarget) pathKey(path Path) string {
	parts := make([]string, len(path.Segments))
	for i, s := range path.Segments {
		if s.IsField() {
			parts[i] = s.Field
		} else {
			parts[i] = strconv.Itoa(s.Idx)
		}
	}
	return strings.Join(parts, ".")
}

func (t *mockTarget) Get(path Path) (Value, error) {
	if t.err != nil {
		return NullValue{}, t.err
	}
	if v, ok := t.store[t.pathKey(path)]; ok {
		return v, nil
	}
	return NullValue{}, nil
}

func (t *mockTarget) Set(path Path, value Value) error {
	if t.err != nil {
		return t.err
	}
	t.store[t.pathKey(path)] = value
	return nil
}

func (t *mockTarget) Delete(path Path) error {
	if t.err != nil {
		return t.err
	}
	delete(t.store, t.pathKey(path))
	return nil
}

type mockContext struct {
	vars   map[string]Value
	target *mockTarget
	funcs  map[string]func([]Value, map[string]Value) (Value, error)
}

func newMockCtx() *mockContext {
	return &mockContext{
		vars:   make(map[string]Value),
		target: newMockTarget(),
		funcs:  make(map[string]func([]Value, map[string]Value) (Value, error)),
	}
}

func (c *mockContext) GetVar(name string) (Value, bool) {
	v, ok := c.vars[name]
	return v, ok
}
func (c *mockContext) SetVar(name string, val Value) { c.vars[name] = val }
func (c *mockContext) DeleteVar(name string)         { delete(c.vars, name) }
func (c *mockContext) GetTarget() Target             { return c.target }

func (c *mockContext) CallFunc(pos Position, name string, positional []Value, named map[string]Value) (Value, error) {
	fn, ok := c.funcs[name]
	if !ok {
		return NullValue{}, fmt.Errorf("unknown function %q", name)
	}
	return fn(positional, named)
}

func TestEvalLiterals(t *testing.T) {
	t.Parallel()

	ctx := newMockCtx()

	tests := []struct {
		name string
		expr Expr
		want Value
	}{
		{"int", &IntLit{Node: n(), Value: 42}, IntegerValue{V: 42}},
		{"float", &FloatLit{Node: n(), Value: 3.14}, FloatValue{V: 3.14}},
		{"string", &StringLit{Node: n(), Value: "hello"}, StringValue{V: "hello"}},
		{"bool_true", &BoolLit{Node: n(), Value: true}, BoolValue{V: true}},
		{"bool_false", &BoolLit{Node: n(), Value: false}, BoolValue{V: false}},
		{"null", &NullLit{Node: n()}, NullValue{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := tc.expr.Eval(ctx)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestEvalIdentExpr(t *testing.T) {
	t.Parallel()

	t.Run("existing_var", func(t *testing.T) {
		t.Parallel()

		ctx := newMockCtx()
		ctx.vars["x"] = IntegerValue{V: 42}
		got, err := (&IdentExpr{Node: n(), Name: "x"}).Eval(ctx)
		require.NoError(t, err)
		assert.Equal(t, IntegerValue{V: 42}, got)
	})

	t.Run("missing_var_returns_null", func(t *testing.T) {
		t.Parallel()

		ctx := newMockCtx()
		got, err := (&IdentExpr{Node: n(), Name: "undefined"}).Eval(ctx)
		require.NoError(t, err)
		assert.Equal(t, NullValue{}, got)
	})
}

func TestEvalPathExpr(t *testing.T) {
	t.Parallel()

	t.Run("single_field", func(t *testing.T) {
		t.Parallel()

		ctx := newMockCtx()
		ctx.target.store["status"] = IntegerValue{V: 200}
		expr := &PathExpr{Node: n(), Root: EventRoot, Segments: []PathSegment{{Field: "status"}}}
		got, err := expr.Eval(ctx)
		require.NoError(t, err)
		assert.Equal(t, IntegerValue{V: 200}, got)
	})

	t.Run("nested_fields", func(t *testing.T) {
		t.Parallel()

		ctx := newMockCtx()
		ctx.target.store["user.name"] = StringValue{V: "alice"}
		expr := &PathExpr{
			Node:     n(),
			Root:     EventRoot,
			Segments: []PathSegment{{Field: "user"}, {Field: "name"}},
		}
		got, err := expr.Eval(ctx)
		require.NoError(t, err)
		assert.Equal(t, StringValue{V: "alice"}, got)
	})

	t.Run("missing_path_returns_null", func(t *testing.T) {
		t.Parallel()

		ctx := newMockCtx()
		expr := &PathExpr{Node: n(), Root: EventRoot, Segments: []PathSegment{{Field: "gone"}}}
		got, err := expr.Eval(ctx)
		require.NoError(t, err)
		assert.Equal(t, NullValue{}, got)
	})

	t.Run("integer_index_segment", func(t *testing.T) {
		t.Parallel()

		ctx := newMockCtx()
		ctx.target.store["2"] = StringValue{V: "third"}
		expr := &PathExpr{
			Node:     n(),
			Root:     EventRoot,
			Segments: []PathSegment{{Index: &IntLit{Node: n(), Value: 2}}},
		}
		got, err := expr.Eval(ctx)
		require.NoError(t, err)
		assert.Equal(t, StringValue{V: "third"}, got)
	})

	t.Run("string_index_becomes_field", func(t *testing.T) {
		t.Parallel()

		ctx := newMockCtx()
		ctx.target.store["key"] = BoolValue{V: true}
		expr := &PathExpr{
			Node:     n(),
			Root:     EventRoot,
			Segments: []PathSegment{{Index: &StringLit{Node: n(), Value: "key"}}},
		}
		got, err := expr.Eval(ctx)
		require.NoError(t, err)
		assert.Equal(t, BoolValue{V: true}, got)
	})

	t.Run("invalid_index_type_error", func(t *testing.T) {
		t.Parallel()

		ctx := newMockCtx()
		expr := &PathExpr{
			Node:     n(),
			Root:     EventRoot,
			Segments: []PathSegment{{Index: &BoolLit{Node: n(), Value: true}}},
		}
		_, err := expr.Eval(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "path index must be integer or string")
	})

	t.Run("index_eval_error_propagates", func(t *testing.T) {
		t.Parallel()

		ctx := newMockCtx()
		expr := &PathExpr{
			Node:     n(),
			Root:     EventRoot,
			Segments: []PathSegment{{Index: &AbortExpr{Node: n()}}},
		}
		_, err := expr.Eval(ctx)
		require.ErrorIs(t, err, AbortError)
	})
}

func TestEvalArrayExpr(t *testing.T) {
	t.Parallel()

	ctx := newMockCtx()

	t.Run("empty", func(t *testing.T) {
		t.Parallel()

		got, err := (&ArrayExpr{Node: n()}).Eval(ctx)
		require.NoError(t, err)
		arr, ok := got.(ArrayValue)
		require.True(t, ok)
		assert.Empty(t, arr.V)
	})

	t.Run("with_elements", func(t *testing.T) {
		t.Parallel()

		expr := &ArrayExpr{
			Node: n(),
			Elements: []Expr{
				&IntLit{Node: n(), Value: 1},
				&StringLit{Node: n(), Value: "two"},
				&BoolLit{Node: n(), Value: true},
			},
		}
		got, err := expr.Eval(ctx)
		require.NoError(t, err)
		assert.Equal(t, ArrayValue{V: []Value{
			IntegerValue{V: 1}, StringValue{V: "two"}, BoolValue{V: true},
		}}, got)
	})
}

func TestEvalObjectExpr(t *testing.T) {
	t.Parallel()

	ctx := newMockCtx()

	t.Run("empty", func(t *testing.T) {
		t.Parallel()

		got, err := (&ObjectExpr{Node: n()}).Eval(ctx)
		require.NoError(t, err)
		obj, ok := got.(ObjectValue)
		require.True(t, ok)
		assert.Empty(t, obj.V)
	})

	t.Run("multiple_pairs", func(t *testing.T) {
		t.Parallel()

		expr := &ObjectExpr{
			Node: n(),
			Pairs: []KVPair{
				{Key: "x", Value: &IntLit{Node: n(), Value: 10}},
				{Key: "y", Value: &StringLit{Node: n(), Value: "hello"}},
			},
		}
		got, err := expr.Eval(ctx)
		require.NoError(t, err)
		obj := got.(ObjectValue)
		assert.Equal(t, IntegerValue{V: 10}, obj.V["x"])
		assert.Equal(t, StringValue{V: "hello"}, obj.V["y"])
	})
}

func TestEvalUnaryExpr(t *testing.T) {
	t.Parallel()

	ctx := newMockCtx()

	tests := []struct {
		name    string
		op      string
		operand Expr
		want    Value
		wantErr bool
	}{
		{"not_true", "!", &BoolLit{Node: n(), Value: true}, BoolValue{V: false}, false},
		{"not_false", "!", &BoolLit{Node: n(), Value: false}, BoolValue{V: true}, false},
		{"not_null", "!", &NullLit{Node: n()}, BoolValue{V: true}, false},
		{"not_int_zero", "!", &IntLit{Node: n(), Value: 0}, BoolValue{V: false}, false},
		{"negate_int", "-", &IntLit{Node: n(), Value: 42}, IntegerValue{V: -42}, false},
		{"negate_negative_int", "-", &IntLit{Node: n(), Value: -7}, IntegerValue{V: 7}, false},
		{"negate_float", "-", &FloatLit{Node: n(), Value: 3.14}, FloatValue{V: -3.14}, false},
		{"negate_string_err", "-", &StringLit{Node: n(), Value: "x"}, nil, true},
		{"negate_null_err", "-", &NullLit{Node: n()}, nil, true},
		{"unknown_op_err", "~", &IntLit{Node: n(), Value: 1}, nil, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			expr := &UnaryExpr{Node: n(), Op: tc.op, Operand: tc.operand}
			got, err := expr.Eval(ctx)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestEvalBinaryExpr(t *testing.T) {
	t.Parallel()

	ctx := newMockCtx()

	iL := func(v int64) Expr { return &IntLit{Node: n(), Value: v} }
	fL := func(v float64) Expr { return &FloatLit{Node: n(), Value: v} }
	sL := func(v string) Expr { return &StringLit{Node: n(), Value: v} }
	bL := func(v bool) Expr { return &BoolLit{Node: n(), Value: v} }
	null := func() Expr { return &NullLit{Node: n()} }

	tests := []struct {
		name    string
		left    Expr
		op      string
		right   Expr
		want    Value
		wantErr bool
	}{
		{"and_false_returns_left", bL(false), "&&", bL(true), BoolValue{V: false}, false},
		{"and_true_returns_right", bL(true), "&&", bL(false), BoolValue{V: false}, false},
		{"and_null_returns_null", null(), "&&", bL(true), NullValue{}, false},
		{"or_true_returns_left", bL(true), "||", bL(false), BoolValue{V: true}, false},
		{"or_false_returns_right", bL(false), "||", bL(true), BoolValue{V: true}, false},
		{"or_int_truthy_returns_int", iL(42), "||", bL(false), IntegerValue{V: 42}, false},
		{"eq_int_same", iL(42), "==", iL(42), BoolValue{V: true}, false},
		{"eq_int_diff", iL(1), "==", iL(2), BoolValue{V: false}, false},
		{"neq_int_diff", iL(1), "!=", iL(2), BoolValue{V: true}, false},
		{"eq_str_same", sL("a"), "==", sL("a"), BoolValue{V: true}, false},
		{"eq_null_null", null(), "==", null(), BoolValue{V: true}, false},
		{"neq_cross_type", iL(1), "!=", sL("1"), BoolValue{V: true}, false},
		{"add_int_int", iL(2), "+", iL(3), IntegerValue{V: 5}, false},
		{"add_float_float", fL(1.5), "+", fL(2.5), FloatValue{V: 4.0}, false},
		{"add_int_float_mixed", iL(3), "+", fL(1.5), FloatValue{V: 4.5}, false},
		{"add_str_str", sL("hello"), "+", sL(" world"), StringValue{V: "hello world"}, false},
		{"add_str_int_err", sL("x"), "+", iL(1), nil, true},
		{"sub_int_int", iL(10), "-", iL(3), IntegerValue{V: 7}, false},
		{"sub_float_float", fL(5.0), "-", fL(2.0), FloatValue{V: 3.0}, false},
		{"sub_int_float_promotes", iL(10), "-", fL(3.0), FloatValue{V: 7.0}, false},
		{"mul_int_int", iL(4), "*", iL(5), IntegerValue{V: 20}, false},
		{"div_int_int", iL(10), "/", iL(3), IntegerValue{V: 3}, false},
		{"div_float_float", fL(10.0), "/", fL(4.0), FloatValue{V: 2.5}, false},
		{"div_int_zero_err", iL(10), "/", iL(0), nil, true},
		{"div_float_zero_err", fL(1.0), "/", fL(0.0), nil, true},
		{"mod_int_int", iL(10), "%", iL(3), IntegerValue{V: 1}, false},
		{"mod_int_zero_err", iL(10), "%", iL(0), nil, true},
		{"lt_int_true", iL(1), "<", iL(2), BoolValue{V: true}, false},
		{"lt_int_false", iL(2), "<", iL(1), BoolValue{V: false}, false},
		{"lte_int_eq", iL(2), "<=", iL(2), BoolValue{V: true}, false},
		{"gt_int", iL(3), ">", iL(2), BoolValue{V: true}, false},
		{"gte_int_eq", iL(2), ">=", iL(2), BoolValue{V: true}, false},
		{"lt_str", sL("a"), "<", sL("b"), BoolValue{V: true}, false},
		{"gt_float", fL(3.14), ">", fL(2.71), BoolValue{V: true}, false},
		{"lt_int_vs_float", iL(5), "<", fL(5.5), BoolValue{V: true}, false},
		{"err_cmp_incompatible_types", sL("a"), "<", iL(1), nil, true},
		{"err_unknown_op", iL(1), "^", iL(2), nil, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			expr := &BinaryExpr{Node: n(), Left: tc.left, Op: tc.op, Right: tc.right}
			got, err := expr.Eval(ctx)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func TestEvalBinaryShortCircuit(t *testing.T) {
	t.Parallel()

	errExpr := func() Expr {
		return &UnaryExpr{Node: n(), Op: "-", Operand: &StringLit{Node: n(), Value: "x"}}
	}

	t.Run("and_false_skips_right", func(t *testing.T) {
		t.Parallel()

		ctx := newMockCtx()
		expr := &BinaryExpr{
			Node:  n(),
			Left:  &BoolLit{Node: n(), Value: false},
			Op:    "&&",
			Right: errExpr(),
		}
		got, err := expr.Eval(ctx)
		require.NoError(t, err)
		assert.Equal(t, BoolValue{V: false}, got)
	})

	t.Run("or_true_skips_right", func(t *testing.T) {
		t.Parallel()

		ctx := newMockCtx()
		expr := &BinaryExpr{
			Node:  n(),
			Left:  &BoolLit{Node: n(), Value: true},
			Op:    "||",
			Right: errExpr(),
		}
		got, err := expr.Eval(ctx)
		require.NoError(t, err)
		assert.Equal(t, BoolValue{V: true}, got)
	})
}

func TestEvalAssignExpr(t *testing.T) {
	t.Parallel()

	t.Run("to_ident", func(t *testing.T) {
		t.Parallel()

		ctx := newMockCtx()
		expr := &AssignExpr{
			Node:   n(),
			Target: &IdentExpr{Node: n(), Name: "x"},
			Value:  &IntLit{Node: n(), Value: 42},
		}
		got, err := expr.Eval(ctx)
		require.NoError(t, err)
		assert.Equal(t, IntegerValue{V: 42}, got)
		v, ok := ctx.GetVar("x")
		require.True(t, ok)
		assert.Equal(t, IntegerValue{V: 42}, v)
	})

	t.Run("to_path", func(t *testing.T) {
		t.Parallel()

		ctx := newMockCtx()
		expr := &AssignExpr{
			Node:   n(),
			Target: &PathExpr{Node: n(), Root: EventRoot, Segments: []PathSegment{{Field: "foo"}}},
			Value:  &StringLit{Node: n(), Value: "bar"},
		}
		got, err := expr.Eval(ctx)
		require.NoError(t, err)
		assert.Equal(t, StringValue{V: "bar"}, got)
		assert.Equal(t, StringValue{V: "bar"}, ctx.target.store["foo"])
	})

	t.Run("to_array_index", func(t *testing.T) {
		t.Parallel()

		ctx := newMockCtx()
		ctx.vars["arr"] = ArrayValue{V: []Value{IntegerValue{V: 1}, IntegerValue{V: 2}, IntegerValue{V: 3}}}
		expr := &AssignExpr{
			Node: n(),
			Target: &IndexExpr{
				Node:   n(),
				Object: &IdentExpr{Node: n(), Name: "arr"},
				Index:  &IntLit{Node: n(), Value: 0},
			},
			Value: &IntLit{Node: n(), Value: 99},
		}
		got, err := expr.Eval(ctx)
		require.NoError(t, err)
		assert.Equal(t, IntegerValue{V: 99}, got)
		arr := ctx.vars["arr"].(ArrayValue)
		assert.Equal(t, IntegerValue{V: 99}, arr.V[0])
		assert.Equal(t, IntegerValue{V: 2}, arr.V[1])
	})

	t.Run("to_array_negative_index", func(t *testing.T) {
		t.Parallel()

		ctx := newMockCtx()
		ctx.vars["arr"] = ArrayValue{V: []Value{IntegerValue{V: 1}, IntegerValue{V: 2}, IntegerValue{V: 3}}}
		expr := &AssignExpr{
			Node: n(),
			Target: &IndexExpr{
				Node:   n(),
				Object: &IdentExpr{Node: n(), Name: "arr"},
				Index:  &IntLit{Node: n(), Value: -1},
			},
			Value: &IntLit{Node: n(), Value: 77},
		}
		_, err := expr.Eval(ctx)
		require.NoError(t, err)
		arr := ctx.vars["arr"].(ArrayValue)
		assert.Equal(t, IntegerValue{V: 77}, arr.V[2])
	})

	t.Run("to_array_grow_with_nulls", func(t *testing.T) {
		t.Parallel()

		ctx := newMockCtx()
		ctx.vars["arr"] = ArrayValue{V: []Value{IntegerValue{V: 1}}}
		expr := &AssignExpr{
			Node: n(),
			Target: &IndexExpr{
				Node:   n(),
				Object: &IdentExpr{Node: n(), Name: "arr"},
				Index:  &IntLit{Node: n(), Value: 3},
			},
			Value: &IntLit{Node: n(), Value: 9},
		}
		_, err := expr.Eval(ctx)
		require.NoError(t, err)
		arr := ctx.vars["arr"].(ArrayValue)
		require.Len(t, arr.V, 4)
		assert.Equal(t, IntegerValue{V: 1}, arr.V[0])
		assert.Equal(t, NullValue{}, arr.V[1])
		assert.Equal(t, NullValue{}, arr.V[2])
		assert.Equal(t, IntegerValue{V: 9}, arr.V[3])
	})

	t.Run("to_array_out_of_bounds_err", func(t *testing.T) {
		t.Parallel()

		ctx := newMockCtx()
		ctx.vars["arr"] = ArrayValue{V: []Value{IntegerValue{V: 1}}}
		expr := &AssignExpr{
			Node: n(),
			Target: &IndexExpr{
				Node:   n(),
				Object: &IdentExpr{Node: n(), Name: "arr"},
				Index:  &IntLit{Node: n(), Value: -5},
			},
			Value: &IntLit{Node: n(), Value: 99},
		}
		_, err := expr.Eval(ctx)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "out of bounds")
	})

	t.Run("to_object_key", func(t *testing.T) {
		t.Parallel()

		ctx := newMockCtx()
		ctx.vars["obj"] = ObjectValue{V: map[string]Value{"a": IntegerValue{V: 1}}}
		expr := &AssignExpr{
			Node: n(),
			Target: &IndexExpr{
				Node:   n(),
				Object: &IdentExpr{Node: n(), Name: "obj"},
				Index:  &StringLit{Node: n(), Value: "b"},
			},
			Value: &IntLit{Node: n(), Value: 2},
		}
		_, err := expr.Eval(ctx)
		require.NoError(t, err)
		obj := ctx.vars["obj"].(ObjectValue)
		assert.Equal(t, IntegerValue{V: 1}, obj.V["a"])
		assert.Equal(t, IntegerValue{V: 2}, obj.V["b"])
	})

	t.Run("to_index_on_non_ident_err", func(t *testing.T) {
		t.Parallel()

		ctx := newMockCtx()
		expr := &AssignExpr{
			Node: n(),
			Target: &IndexExpr{
				Node:   n(),
				Object: &IntLit{Node: n(), Value: 42},
				Index:  &IntLit{Node: n(), Value: 0},
			},
			Value: &IntLit{Node: n(), Value: 1},
		}
		_, err := expr.Eval(ctx)
		require.Error(t, err)
	})
}
