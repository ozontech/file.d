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

	tests := []struct {
		name      string
		store     map[string]Value // mock target contents, keyed by joined path
		segments  []PathSegment
		want      Value
		wantErr   string
		wantErrIs error
	}{
		{
			name:     "single_field",
			store:    map[string]Value{"status": IntegerValue{V: 200}},
			segments: []PathSegment{{Field: "status"}},
			want:     IntegerValue{V: 200},
		},
		{
			name:     "nested_fields",
			store:    map[string]Value{"user.name": StringValue{V: "alice"}},
			segments: []PathSegment{{Field: "user"}, {Field: "name"}},
			want:     StringValue{V: "alice"},
		},
		{
			name:     "missing_path_returns_null",
			segments: []PathSegment{{Field: "gone"}},
			want:     NullValue{},
		},
		{
			name:     "integer_index_segment",
			store:    map[string]Value{"2": StringValue{V: "third"}},
			segments: []PathSegment{{Index: &IntLit{Node: n(), Value: 2}}},
			want:     StringValue{V: "third"},
		},
		{
			name:     "string_index_becomes_field",
			store:    map[string]Value{"key": BoolValue{V: true}},
			segments: []PathSegment{{Index: &StringLit{Node: n(), Value: "key"}}},
			want:     BoolValue{V: true},
		},
		{
			name:     "invalid_index_type_error",
			segments: []PathSegment{{Index: &BoolLit{Node: n(), Value: true}}},
			wantErr:  "path index must be integer or string",
		},
		{
			name:      "index_eval_error_propagates",
			segments:  []PathSegment{{Index: &AbortExpr{Node: n()}}},
			wantErrIs: AbortError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := newMockCtx()
			for k, v := range tt.store {
				ctx.target.store[k] = v
			}

			expr := &PathExpr{Node: n(), Root: EventRoot, Segments: tt.segments}
			got, err := expr.Eval(ctx)
			switch {
			case tt.wantErrIs != nil:
				require.ErrorIs(t, err, tt.wantErrIs)
			case tt.wantErr != "":
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			default:
				require.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
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

	num := func(v int64) Value { return IntegerValue{V: v} }
	arr := func(vals ...Value) Value { return ArrayValue{V: vals} }
	index := func(obj Expr, idx Expr) Expr {
		return &IndexExpr{Node: n(), Object: obj, Index: idx}
	}
	ident := func(name string) Expr { return &IdentExpr{Node: n(), Name: name} }
	iL := func(v int64) Expr { return &IntLit{Node: n(), Value: v} }
	sL := func(v string) Expr { return &StringLit{Node: n(), Value: v} }

	tests := []struct {
		name      string
		vars      map[string]Value // initial ctx variables
		target    Expr
		value     Expr
		want      Value            // expected result of the assignment expression
		wantErr   string
		wantVars  map[string]Value // exact ctx variables afterwards; nil skips the check
		wantStore map[string]Value // exact mock target contents afterwards; nil skips the check
	}{
		{
			name:     "to_ident",
			target:   ident("x"),
			value:    iL(42),
			want:     num(42),
			wantVars: map[string]Value{"x": num(42)},
		},
		{
			name:      "to_path",
			target:    &PathExpr{Node: n(), Root: EventRoot, Segments: []PathSegment{{Field: "foo"}}},
			value:     sL("bar"),
			want:      StringValue{V: "bar"},
			wantStore: map[string]Value{"foo": StringValue{V: "bar"}},
		},
		{
			name:     "to_array_index",
			vars:     map[string]Value{"arr": arr(num(1), num(2), num(3))},
			target:   index(ident("arr"), iL(0)),
			value:    iL(99),
			want:     num(99),
			wantVars: map[string]Value{"arr": arr(num(99), num(2), num(3))},
		},
		{
			name:     "to_array_negative_index",
			vars:     map[string]Value{"arr": arr(num(1), num(2), num(3))},
			target:   index(ident("arr"), iL(-1)),
			value:    iL(77),
			want:     num(77),
			wantVars: map[string]Value{"arr": arr(num(1), num(2), num(77))},
		},
		{
			name:     "to_array_grow_with_nulls",
			vars:     map[string]Value{"arr": arr(num(1))},
			target:   index(ident("arr"), iL(3)),
			value:    iL(9),
			want:     num(9),
			wantVars: map[string]Value{"arr": arr(num(1), NullValue{}, NullValue{}, num(9))},
		},
		{
			name:    "to_array_out_of_bounds_err",
			vars:    map[string]Value{"arr": arr(num(1))},
			target:  index(ident("arr"), iL(-5)),
			value:   iL(99),
			wantErr: "out of bounds",
		},
		{
			name:   "to_object_key",
			vars:   map[string]Value{"obj": ObjectValue{V: map[string]Value{"a": num(1)}}},
			target: index(ident("obj"), sL("b")),
			value:  iL(2),
			want:   num(2),
			wantVars: map[string]Value{
				"obj": ObjectValue{V: map[string]Value{"a": num(1), "b": num(2)}},
			},
		},
		{
			name:    "to_index_on_non_ident_err",
			target:  index(iL(42), iL(0)),
			value:   iL(1),
			wantErr: "must be a local variable",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := newMockCtx()
			for k, v := range tt.vars {
				ctx.vars[k] = v
			}

			expr := &AssignExpr{Node: n(), Target: tt.target, Value: tt.value}
			got, err := expr.Eval(ctx)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			if tt.wantVars != nil {
				assert.Equal(t, tt.wantVars, ctx.vars)
			}
			if tt.wantStore != nil {
				assert.Equal(t, tt.wantStore, ctx.target.store)
			}
		})
	}
}
