package compiler

import (
	"strings"
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/ozontech/file.d/plugin/action/transform/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// prepare compiles src and runs the validation walk, which is where folding
// happens. It returns the prepared AST.
func prepare(t *testing.T, src string) []core.Expr {
	t.Helper()

	exprs := compileN(t, src)
	require.NoError(t, stdlibValidate(exprs))
	return exprs
}

func stdlibValidate(exprs []core.Expr) error {
	return ValidateCalls(exprs, stdlib.GetRegistry())
}

// dumpPrepared renders the AST after preparation, so folded nodes show up as Const(...).
func dumpPrepared(t *testing.T, src string) string {
	t.Helper()

	dumps := make([]string, 0)
	for _, e := range prepare(t, src) {
		dumps = append(dumps, core.DumpAST(e, 0))
	}
	return strings.Join(dumps, "\n")
}

// assertNotFolded checks that the value of the single assignment in src was
// left in the AST. It reaches into a call argument when argIndex is not -1.
func assertNotFolded(t *testing.T, src string, argIndex int) {
	t.Helper()

	exprs := prepare(t, src)
	require.Len(t, exprs, 1)

	assign, ok := exprs[0].(*core.AssignExpr)
	require.True(t, ok, "expected an assignment, got %T", exprs[0])

	target := assign.Value
	if argIndex >= 0 {
		call, ok := target.(*core.CallExpr)
		require.True(t, ok, "expected a call, got %T", target)
		require.Greater(t, len(call.Args), argIndex)
		target = call.Args[argIndex].Value
	}

	_, folded := target.(*core.ConstExpr)
	assert.False(t, folded, "expression must not be folded: %s", core.DumpAST(exprs[0], 0))
}

// A failing constant expression must stay in the AST so it keeps failing per
// event. Folding must never turn a runtime error into a startup error.
func TestFoldSkipsFailingExpressions(t *testing.T) {
	tests := []struct {
		name string
		src  string
		arg  int
	}{
		{name: "division by zero", src: `x = 1 / 0`, arg: -1},
		{name: "modulo by zero", src: `x = 1 % 0`, arg: -1},
		{name: "string plus integer", src: `x = "a" + 1`, arg: -1},
		{name: "negate a string", src: `x = -"a"`, arg: -1},
		{name: "failing element in an array", src: `x = [1, 1 / 0]`, arg: -1},
		{name: "failing value in an object", src: `x = {a: 1 / 0}`, arg: -1},
		{name: "failing argument", src: `x = upcase(1 / 0)`, arg: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assertNotFolded(t, tt.src, tt.arg)
		})
	}
}

func TestFoldConstantArguments(t *testing.T) {
	tests := []goldenCase{
		{
			name: "object literal argument",
			src:  `.x = lookup(.x, {"0": "produce", "1": "fetch"})`,
			want: `
Assign
  Path(.x)
  Call(lookup)
    Path(.x)
    Const({"0": "produce", "1": "fetch"})`,
		},
		{
			name: "nested composite literal",
			src:  `.x = lookup(.x, {a: [1, {b: "c"}]})`,
			want: `
Assign
  Path(.x)
  Call(lookup)
    Path(.x)
    Const({"a": [1, {"b": "c"}]})`,
		},
		{
			name: "negative numbers do not block folding",
			src:  `.x = lookup(.x, {a: -1, b: 2 + 3})`,
			want: `
Assign
  Path(.x)
  Call(lookup)
    Path(.x)
    Const({"a": -1, "b": 5})`,
		},
		{
			name: "named argument",
			src:  `.x = lookup(.x, {}, default: "unknown")`,
			want: `
Assign
  Path(.x)
  Call(lookup)
    Path(.x)
    Const({})
    named(default:)
      Const("unknown")`,
		},
		{
			name: "regex literal folds after it is compiled",
			src:  `m = capture(.log, r'(?P<a>\d+)')`,
			want: `
Assign
  Ident(m)
  Call(capture)
    Path(.log)
    Const(r'(?P<a>\d+)')`,
		},
		{
			name: "timestamp literal folds after it is parsed",
			src:  `.x = lookup(.x, {}, default: t'2024-01-15T10:30:00Z')`,
			want: `
Assign
  Path(.x)
  Call(lookup)
    Path(.x)
    Const({})
    named(default:)
      Const(2024-01-15T10:30:00Z)`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, strings.TrimSpace(tt.want), dumpPrepared(t, tt.src))
		})
	}
}

// The `table = { ... }` idiom must not rebuild the object on every event.
func TestFoldAssignmentValue(t *testing.T) {
	tests := []goldenCase{
		{
			name: "object literal",
			src:  `t = {"0": "produce"}`,
			want: `
Assign
  Ident(t)
  Const({"0": "produce"})`,
		},
		{
			name: "array literal",
			src:  `t = [1, "two", true]`,
			want: `
Assign
  Ident(t)
  Const([1, "two", true])`,
		},
		{
			name: "field write",
			src:  `.x = 1 + 2`,
			want: `
Assign
  Path(.x)
  Const(3)`,
		},
	}

	runFoldGolden(t, tests)
}

// Anything that depends on the event or on per-event state is not constant.
func TestFoldSkipsNonConstant(t *testing.T) {
	tests := []struct {
		name string
		src  string
		arg  int
	}{
		{name: "event field", src: `t = .table`, arg: -1},
		{name: "variable", src: `t = other`, arg: -1},
		{name: "object holding a field read", src: `t = {a: .x}`, arg: -1},
		{name: "array holding a variable", src: `t = [1, other]`, arg: -1},
		// A call is never folded, even with constant arguments: functions are
		// not required to be pure.
		{name: "function call", src: `t = upcase("a")`, arg: -1},
		{name: "table argument read from the event", src: `.x = lookup(.x, .table)`, arg: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assertNotFolded(t, tt.src, tt.arg)
		})
	}
}

// Folding reaches into nested blocks, since the validation walk does.
func TestFoldInsideBlocks(t *testing.T) {
	tests := []goldenCase{
		{
			name: "if branches",
			src: `if .a {
  t = {a: 1}
} else {
  t = {b: 2}
}`,
			want: `
If
  condition:
    Path(.a)
  then:
    Assign
      Ident(t)
      Const({"a": 1})
  else:
    Assign
      Ident(t)
      Const({"b": 2})`,
		},
		{
			name: "for body",
			src: `for _, item in .items {
  t = {a: 1}
}`,
			want: `
For(index="", item="item")
  iter:
    Path(.items)
  body:
    Assign
      Ident(t)
      Const({"a": 1})`,
		},
	}

	runFoldGolden(t, tests)
}

func runFoldGolden(t *testing.T, tests []goldenCase) {
	t.Helper()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, strings.TrimSpace(tt.want), dumpPrepared(t, tt.src))
		})
	}
}

// A folded value is shared by every event and every processor goroutine, so it
// must survive being written through.
func TestFoldedValueIsNotMutatedByAssignment(t *testing.T) {
	exprs := prepare(t, `
t = {a: 1}
t.a = 2
`)
	require.Len(t, exprs, 2)

	assign, ok := exprs[0].(*core.AssignExpr)
	require.True(t, ok)
	folded, ok := assign.Value.(*core.ConstExpr)
	require.True(t, ok, "the table must have been folded")

	table, ok := folded.V.(core.ObjectValue)
	require.True(t, ok)

	// Run the program twice against the same AST; the second run must see the
	// original table, not the one the first run wrote to.
	for range 2 {
		ctx := newFoldTestContext()
		for _, e := range exprs {
			_, err := e.Eval(ctx)
			require.NoError(t, err)
		}
		assert.Equal(t, core.IntegerValue{V: 1}, table.V["a"], "the folded table was mutated")
	}
}

// foldTestContext is the smallest EvalContext that supports variables; the
// mutation test needs no event target.
type foldTestContext struct {
	vars map[string]core.Value
}

func newFoldTestContext() *foldTestContext {
	return &foldTestContext{vars: make(map[string]core.Value)}
}

func (c *foldTestContext) GetVar(name string) (core.Value, bool) {
	v, ok := c.vars[name]
	return v, ok
}
func (c *foldTestContext) SetVar(name string, v core.Value) { c.vars[name] = v }
func (c *foldTestContext) DeleteVar(name string)            { delete(c.vars, name) }
func (c *foldTestContext) GetTarget() core.Target           { return nil }
func (c *foldTestContext) CallFunc(_ core.Position, _ string, _ []core.Value, _ map[string]core.Value) (core.Value, error) {
	return core.NullValue{}, nil
}
