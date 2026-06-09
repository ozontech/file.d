package compiler

import (
	"fmt"
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func compile(t *testing.T, src string) core.Expr {
	t.Helper()
	c, err := NewCompiler(src)
	require.NoError(t, err, "lexer error in %q", src)
	exprs, err := c.Compile()
	require.NoError(t, err, "compile error in %q", src)
	require.Len(t, exprs, 1, "expected exactly 1 expression")
	return exprs[0]
}

func compileN(t *testing.T, src string) []core.Expr {
	t.Helper()
	c, err := NewCompiler(src)
	require.NoError(t, err)
	exprs, err := c.Compile()
	require.NoError(t, err)
	return exprs
}

func mustFail(t *testing.T, src string) error {
	t.Helper()
	c, err := NewCompiler(src)
	if err != nil {
		return err
	}
	_, err = c.Compile()
	require.Error(t, err, "expected compile error for %q", src)
	return err
}

func TestCompileEmpty(t *testing.T) {
	for _, src := range []string{"", "   ", ";;;", "# comment\n# another"} {
		t.Run(fmt.Sprintf("%q", src), func(t *testing.T) {
			c, err := NewCompiler(src)
			require.NoError(t, err)
			exprs, err := c.Compile()
			require.NoError(t, err)
			assert.Empty(t, exprs)
		})
	}
}

type litCase[V any] struct {
	name string
	src  string
	want V
}

func testLit[L core.Expr, V any](t *testing.T, cases []litCase[V], getValue func(L) V) {
	t.Helper()
	for _, tc := range cases {
		name := tc.name
		if name == "" {
			name = tc.src
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			expr := compile(t, tc.src)
			lit, ok := expr.(L)
			require.True(t, ok, "expected %T, got %T", (*L)(nil), expr)
			assert.Equal(t, tc.want, getValue(lit))
		})
	}
}

func TestLiterals(t *testing.T) {
	t.Parallel()

	testLit(t, []litCase[int64]{
		{src: "0", want: 0},
		{src: "42", want: 42},
		{src: "1000000", want: 1000000},
	}, func(l *core.IntLit) int64 { return l.Value })

	testLit(t, []litCase[float64]{
		{src: "3.14", want: 3.14},
		{src: "0.5", want: 0.5},
		{src: "1e10", want: 1e10},
		{src: "1.5e-3", want: 1.5e-3},
	}, func(l *core.FloatLit) float64 { return l.Value })

	testLit(t, []litCase[string]{
		{name: "simple", src: `"hello"`, want: "hello"},
		{name: "escaped_quote", src: `"say \"hi\""`, want: `say "hi"`},
		{name: "escape_newline", src: `"line\nbreak"`, want: "line\nbreak"},
		{name: "escape_tab", src: `"tab\there"`, want: "tab\there"},
		{name: "empty", src: `""`, want: ""},
	}, func(l *core.StringLit) string { return l.Value })

	testLit(t, []litCase[string]{
		{name: "simple", src: `s'hello'`, want: "hello"},
		{name: "backslash_preserved", src: `s'C:\new\path'`, want: `C:\new\path`},
		{name: "no_escape", src: `s'no\nescape'`, want: `no\nescape`},
	}, func(l *core.StringLit) string { return l.Value })

	testLit(t, []litCase[bool]{
		{src: "true", want: true},
		{src: "false", want: false},
	}, func(l *core.BoolLit) bool { return l.Value })

	testLit(t, []litCase[string]{
		{name: "simple", src: `r'\d+'`, want: `\d+`},
		{name: "complex", src: `r'\w+@\w+\.\w+'`, want: `\w+@\w+\.\w+`},
	}, func(l *core.RegexLit) string { return l.Pattern })

	testLit(t, []litCase[string]{
		{src: `t'2024-01-15T10:30:00Z'`, want: "2024-01-15T10:30:00Z"},
	}, func(l *core.TimestampLit) string { return l.Value })

	_, ok := compile(t, "null").(*core.NullLit)
	assert.True(t, ok)
}

func TestCompileIdent(t *testing.T) {
	t.Parallel()

	tests := []string{"foo", "_bar", "baz123", "x"}
	for _, name := range tests {
		name := name
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			expr := compile(t, name)
			ident, ok := expr.(*core.IdentExpr)
			require.True(t, ok, "expected *core.IdentExpr, got %T", expr)
			assert.Equal(t, name, ident.Name)
		})
	}
}

func TestCompileEventPath(t *testing.T) {
	t.Parallel()

	t.Run("root_only", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, ".")
		p, ok := expr.(*core.PathExpr)
		require.True(t, ok)
		assert.Equal(t, core.EventRoot, p.Root)
		assert.Empty(t, p.Segments)
	})

	t.Run("single_field", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, ".status")
		p, ok := expr.(*core.PathExpr)
		require.True(t, ok)
		assert.Equal(t, core.EventRoot, p.Root)
		require.Len(t, p.Segments, 1)
		assert.Equal(t, "status", p.Segments[0].Field)
	})

	t.Run("nested_fields", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, ".user.name")
		p, ok := expr.(*core.PathExpr)
		require.True(t, ok)
		require.Len(t, p.Segments, 2)
		assert.Equal(t, "user", p.Segments[0].Field)
		assert.Equal(t, "name", p.Segments[1].Field)
	})

	t.Run("field_with_integer_index", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, ".items[0]")
		p, ok := expr.(*core.PathExpr)
		require.True(t, ok)
		require.Len(t, p.Segments, 2)
		assert.Equal(t, "items", p.Segments[0].Field)
		assert.NotNil(t, p.Segments[1].Index)
		idxLit, ok := p.Segments[1].Index.(*core.IntLit)
		require.True(t, ok)
		assert.Equal(t, int64(0), idxLit.Value)
	})

	t.Run("field_with_dynamic_index", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, ".items[idx]")
		p, ok := expr.(*core.PathExpr)
		require.True(t, ok)
		require.Len(t, p.Segments, 2)
		idxExpr, ok := p.Segments[1].Index.(*core.IdentExpr)
		require.True(t, ok)
		assert.Equal(t, "idx", idxExpr.Name)
	})

	t.Run("field_with_negative_index", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, ".items[-1]")
		p, ok := expr.(*core.PathExpr)
		require.True(t, ok)
		require.Len(t, p.Segments, 2)
		_, ok = p.Segments[1].Index.(*core.UnaryExpr)
		require.True(t, ok, "expected unary expression for negative index")
	})
}

func TestCompileMetadataPath(t *testing.T) {
	t.Parallel()

	t.Run("simple_field", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, "%ts")
		p, ok := expr.(*core.PathExpr)
		require.True(t, ok)
		assert.Equal(t, core.MetadataRoot, p.Root)
		require.Len(t, p.Segments, 1)
		assert.Equal(t, "ts", p.Segments[0].Field)
	})

	t.Run("nested", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, "%meta.key")
		p, ok := expr.(*core.PathExpr)
		require.True(t, ok)
		assert.Equal(t, core.MetadataRoot, p.Root)
		require.Len(t, p.Segments, 2)
		assert.Equal(t, "meta", p.Segments[0].Field)
		assert.Equal(t, "key", p.Segments[1].Field)
	})
}

func TestCompileUnaryExpr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		src string
		op  string
	}{
		{"!foo", "!"},
		{"-42", "-"},
		{"!.flag", "!"},
	}
	for _, tc := range tests {
		t.Run(tc.src, func(t *testing.T) {
			t.Parallel()

			expr := compile(t, tc.src)
			u, ok := expr.(*core.UnaryExpr)
			require.True(t, ok, "expected *core.UnaryExpr, got %T", expr)
			assert.Equal(t, tc.op, u.Op)
		})
	}
}

func TestCompileBinaryExpr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		src string
		op  string
	}{
		{"1 + 2", "+"},
		{"a - b", "-"},
		{"x * y", "*"},
		{"n / 2", "/"},
		{"n % 3", "%"},
		{"a == b", "=="},
		{"a != b", "!="},
		{".x < .y", "<"},
		{".x <= .y", "<="},
		{".x > .y", ">"},
		{".x >= .y", ">="},
		{"a && b", "&&"},
		{"a || b", "||"},
	}
	for _, tc := range tests {
		t.Run(tc.op, func(t *testing.T) {
			t.Parallel()

			expr := compile(t, tc.src)
			bin, ok := expr.(*core.BinaryExpr)
			require.True(t, ok, "expected *core.BinaryExpr for %q, got %T", tc.src, expr)
			assert.Equal(t, tc.op, bin.Op)
		})
	}
}

func TestCompileOperatorPrecedence(t *testing.T) {
	t.Parallel()

	t.Run("mul_over_add", func(t *testing.T) {
		t.Parallel()

		bin := compile(t, "1 + 2 * 3").(*core.BinaryExpr)
		assert.Equal(t, "+", bin.Op)
		rightBin, ok := bin.Right.(*core.BinaryExpr)
		require.True(t, ok)
		assert.Equal(t, "*", rightBin.Op)
	})

	t.Run("left_associative_add", func(t *testing.T) {
		t.Parallel()

		bin := compile(t, "1 * 2 + 3").(*core.BinaryExpr)
		assert.Equal(t, "+", bin.Op)
		leftBin, ok := bin.Left.(*core.BinaryExpr)
		require.True(t, ok)
		assert.Equal(t, "*", leftBin.Op)
	})

	t.Run("and_over_or", func(t *testing.T) {
		t.Parallel()

		bin := compile(t, "a || b && c").(*core.BinaryExpr)
		assert.Equal(t, "||", bin.Op)
		rightBin, ok := bin.Right.(*core.BinaryExpr)
		require.True(t, ok)
		assert.Equal(t, "&&", rightBin.Op)
	})

	t.Run("eq_over_and", func(t *testing.T) {
		t.Parallel()

		bin := compile(t, "a == b && c != d").(*core.BinaryExpr)
		assert.Equal(t, "&&", bin.Op)
		_, ok := bin.Left.(*core.BinaryExpr)
		require.True(t, ok, "left should be == expr")
		_, ok = bin.Right.(*core.BinaryExpr)
		require.True(t, ok, "right should be != expr")
	})

	t.Run("unary_over_binary", func(t *testing.T) {
		t.Parallel()

		bin := compile(t, "!a && b").(*core.BinaryExpr)
		assert.Equal(t, "&&", bin.Op)
		_, ok := bin.Left.(*core.UnaryExpr)
		require.True(t, ok, "left should be unary ! expression")
	})

	t.Run("grouping_overrides_precedence", func(t *testing.T) {
		t.Parallel()

		bin := compile(t, "(1 + 2) * 3").(*core.BinaryExpr)
		assert.Equal(t, "*", bin.Op)
		_, ok := bin.Left.(*core.BinaryExpr)
		require.True(t, ok, "left should be grouped + expression")
	})
}

func TestCompileAssignExpr(t *testing.T) {
	t.Parallel()

	t.Run("to_ident", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `x = "hello"`)
		a, ok := expr.(*core.AssignExpr)
		require.True(t, ok)
		_, ok = a.Target.(*core.IdentExpr)
		require.True(t, ok, "target should be IdentExpr")
	})

	t.Run("to_event_path", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `.foo = 42`)
		a, ok := expr.(*core.AssignExpr)
		require.True(t, ok)
		p, ok := a.Target.(*core.PathExpr)
		require.True(t, ok)
		assert.Equal(t, core.EventRoot, p.Root)
		require.Len(t, p.Segments, 1)
		assert.Equal(t, "foo", p.Segments[0].Field)
		lit, ok := a.Value.(*core.IntLit)
		require.True(t, ok)
		assert.Equal(t, int64(42), lit.Value)
	})

	t.Run("to_index_expr", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `arr[0] = 99`)
		a, ok := expr.(*core.AssignExpr)
		require.True(t, ok)
		_, ok = a.Target.(*core.IndexExpr)
		require.True(t, ok, "target should be IndexExpr")
	})

	t.Run("right_associative", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `x = y = 1`)
		outer, ok := expr.(*core.AssignExpr)
		require.True(t, ok)
		_, ok = outer.Value.(*core.AssignExpr)
		require.True(t, ok, "right side should also be AssignExpr (right-assoc)")
	})
}

func TestCompileIfExpr(t *testing.T) {
	t.Parallel()

	t.Run("no_else", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `if .x > 0 { .y = 1 }`)
		ifExpr, ok := expr.(*core.IfExpr)
		require.True(t, ok)
		assert.NotNil(t, ifExpr.Condition)
		assert.Len(t, ifExpr.Then, 1)
		assert.Empty(t, ifExpr.Else)
	})

	t.Run("with_else", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `if .ok { .r = "yes" } else { .r = "no" }`)
		ifExpr, ok := expr.(*core.IfExpr)
		require.True(t, ok)
		assert.Len(t, ifExpr.Then, 1)
		assert.Len(t, ifExpr.Else, 1)
	})

	t.Run("else_if_chain", func(t *testing.T) {
		t.Parallel()

		src := `if .s >= 500 { .sev = "crit" } else if .s >= 400 { .sev = "warn" } else { .sev = "ok" }`
		expr := compile(t, src)
		ifExpr, ok := expr.(*core.IfExpr)
		require.True(t, ok)
		require.Len(t, ifExpr.Else, 1)
		_, ok = ifExpr.Else[0].(*core.IfExpr)
		require.True(t, ok, "else branch should be another IfExpr")
	})

	t.Run("multi_statement_then_block", func(t *testing.T) {
		t.Parallel()

		src := `if true { .a = 1; .b = 2 }`
		expr := compile(t, src)
		ifExpr, ok := expr.(*core.IfExpr)
		require.True(t, ok)
		assert.Len(t, ifExpr.Then, 2)
	})
}

func TestCompileForExpr(t *testing.T) {
	t.Parallel()

	t.Run("index_only", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `for i in .arr {}`)
		f, ok := expr.(*core.ForExpr)
		require.True(t, ok)
		assert.Equal(t, "i", f.Index)
		assert.Equal(t, "", f.Item)
	})

	t.Run("index_and_item", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `for i, v in .arr {}`)
		f, ok := expr.(*core.ForExpr)
		require.True(t, ok)
		assert.Equal(t, "i", f.Index)
		assert.Equal(t, "v", f.Item)
	})

	t.Run("blank_index", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `for _, v in .arr {}`)
		f, ok := expr.(*core.ForExpr)
		require.True(t, ok)
		assert.Equal(t, "", f.Index)
		assert.Equal(t, "v", f.Item)
	})

	t.Run("blank_item", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `for i, _ in .arr {}`)
		f, ok := expr.(*core.ForExpr)
		require.True(t, ok)
		assert.Equal(t, "i", f.Index)
		assert.Equal(t, "", f.Item)
	})

	t.Run("body_statements", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `for i in .items { .items[i] = .items[i] + 1 }`)
		f, ok := expr.(*core.ForExpr)
		require.True(t, ok)
		assert.Len(t, f.Body, 1)
	})

	t.Run("iter_is_ident", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `for i in myArr {}`)
		f, ok := expr.(*core.ForExpr)
		require.True(t, ok)
		ident, ok := f.Iter.(*core.IdentExpr)
		require.True(t, ok)
		assert.Equal(t, "myArr", ident.Name)
	})
}

func TestCompileDelExpr(t *testing.T) {
	t.Parallel()

	t.Run("event_path", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `del .secret`)
		d, ok := expr.(*core.DelExpr)
		require.True(t, ok)
		assert.Equal(t, core.EventRoot, d.Target.Root)
		require.Len(t, d.Target.Segments, 1)
		assert.Equal(t, "secret", d.Target.Segments[0].Field)
	})

	t.Run("nested_event_path", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `del .user.password`)
		d, ok := expr.(*core.DelExpr)
		require.True(t, ok)
		require.Len(t, d.Target.Segments, 2)
	})

	t.Run("metadata_path", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `del %meta`)
		d, ok := expr.(*core.DelExpr)
		require.True(t, ok)
		assert.Equal(t, core.MetadataRoot, d.Target.Root)
	})
}

func TestCompileArrayExpr(t *testing.T) {
	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `[]`)
		arr, ok := expr.(*core.ArrayExpr)
		require.True(t, ok)
		assert.Empty(t, arr.Elements)
	})

	t.Run("integers", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `[1, 2, 3]`)
		arr, ok := expr.(*core.ArrayExpr)
		require.True(t, ok)
		assert.Len(t, arr.Elements, 3)
	})

	t.Run("mixed_types", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `[1, "two", true]`)
		arr, ok := expr.(*core.ArrayExpr)
		require.True(t, ok)
		assert.Len(t, arr.Elements, 3)
		_, ok = arr.Elements[0].(*core.IntLit)
		require.True(t, ok, "first element should be IntLit")
		_, ok = arr.Elements[1].(*core.StringLit)
		require.True(t, ok, "second element should be StringLit")
		_, ok = arr.Elements[2].(*core.BoolLit)
		require.True(t, ok, "third element should be BoolLit")
	})

	t.Run("trailing_comma_not_required", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `[1, 2]`)
		arr, ok := expr.(*core.ArrayExpr)
		require.True(t, ok)
		assert.Len(t, arr.Elements, 2)
	})
}

func compileObj(t *testing.T, src string) *core.ObjectExpr {
	t.Helper()
	expr := compile(t, src)
	obj, ok := expr.(*core.ObjectExpr)
	require.True(t, ok, "expected *core.ObjectExpr, got %T", expr)
	return obj
}

type kvExpect struct {
	key string
	val func(t *testing.T, expr core.Expr)
}

func litVal[L any, V any](want V, get func(*L) V) func(*testing.T, core.Expr) {
	return func(t *testing.T, expr core.Expr) {
		t.Helper()
		lit, ok := any(expr).(*L)
		require.True(t, ok, "expected %T, got %T", (*L)(nil), expr)
		assert.Equal(t, want, get(lit))
	}
}

func nullVal() func(*testing.T, core.Expr) {
	return func(t *testing.T, expr core.Expr) {
		t.Helper()
		_, ok := any(expr).(*core.NullLit)
		require.True(t, ok, "expected *core.NullLit, got %T", expr)
	}
}

func TestCompileObjectExpr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		src   string
		pairs []kvExpect
	}{
		{
			name:  "empty",
			src:   `{}`,
			pairs: nil,
		},
		{
			name: "string_key",
			src:  `{"name": "alice"}`,
			pairs: []kvExpect{
				{"name", litVal("alice", func(l *core.StringLit) string { return l.Value })},
			},
		},
		{
			name: "ident_key",
			src:  `{level: "info"}`,
			pairs: []kvExpect{
				{"level", litVal("info", func(l *core.StringLit) string { return l.Value })},
			},
		},
		{
			name: "raw_string_key",
			src:  `{s'raw\key': 1}`,
			pairs: []kvExpect{
				{`raw\key`, litVal(1, func(l *core.IntLit) int64 { return l.Value })},
			},
		},
		{
			name: "multiple_pairs_all_scalar_types",
			src:  `{"n": 42, "s": "hi", "b": true, "f": 3.14, "z": null}`,
			pairs: []kvExpect{
				{"n", litVal(42, func(l *core.IntLit) int64 { return l.Value })},
				{"s", litVal("hi", func(l *core.StringLit) string { return l.Value })},
				{"b", litVal(true, func(l *core.BoolLit) bool { return l.Value })},
				{"f", litVal(3.14, func(l *core.FloatLit) float64 { return l.Value })},
				{"z", nullVal()},
			},
		},
		{
			name: "bool_false_value",
			src:  `{"ok": false}`,
			pairs: []kvExpect{
				{"ok", litVal(false, func(l *core.BoolLit) bool { return l.Value })},
			},
		},
		{
			name: "integer_zero",
			src:  `{"count": 0}`,
			pairs: []kvExpect{
				{"count", litVal(0, func(l *core.IntLit) int64 { return l.Value })},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			obj := compileObj(t, tc.src)
			require.Len(t, obj.Pairs, len(tc.pairs), "wrong number of pairs")
			for i, kv := range tc.pairs {
				assert.Equal(t, kv.key, obj.Pairs[i].Key, "pair[%d] key", i)
				kv.val(t, obj.Pairs[i].Value)
			}
		})
	}
}

func TestCompileCallExpr(t *testing.T) {
	t.Parallel()

	t.Run("no_args", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `now()`)
		call, ok := expr.(*core.CallExpr)
		require.True(t, ok)
		assert.Equal(t, "now", call.Name)
		assert.Empty(t, call.Args)
	})

	t.Run("positional_args", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `upcase(.level)`)
		call, ok := expr.(*core.CallExpr)
		require.True(t, ok)
		assert.Equal(t, "upcase", call.Name)
		require.Len(t, call.Args, 1)
		assert.Equal(t, "", call.Args[0].Name, "should be positional (no name)")
	})

	t.Run("named_arg", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `fn(key: "value")`)
		call, ok := expr.(*core.CallExpr)
		require.True(t, ok)
		require.Len(t, call.Args, 1)
		assert.Equal(t, "key", call.Args[0].Name)
		_, ok = call.Args[0].Value.(*core.StringLit)
		require.True(t, ok)
	})

	t.Run("mixed_positional_and_named", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `fn(.x, sep: ",")`)
		call, ok := expr.(*core.CallExpr)
		require.True(t, ok)
		require.Len(t, call.Args, 2)
		assert.Equal(t, "", call.Args[0].Name, "first arg is positional")
		assert.Equal(t, "sep", call.Args[1].Name)
	})
}

func TestCompileIndexExpr(t *testing.T) {
	t.Parallel()

	t.Run("array_index", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `arr[0]`)
		idx, ok := expr.(*core.IndexExpr)
		require.True(t, ok)
		_, ok = idx.Object.(*core.IdentExpr)
		require.True(t, ok)
		lit, ok := idx.Index.(*core.IntLit)
		require.True(t, ok)
		assert.Equal(t, int64(0), lit.Value)
	})

	t.Run("object_string_index", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `obj["key"]`)
		idx, ok := expr.(*core.IndexExpr)
		require.True(t, ok)
		lit, ok := idx.Index.(*core.StringLit)
		require.True(t, ok)
		assert.Equal(t, "key", lit.Value)
	})

	t.Run("dynamic_index", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `arr[i]`)
		idx, ok := expr.(*core.IndexExpr)
		require.True(t, ok)
		ident, ok := idx.Index.(*core.IdentExpr)
		require.True(t, ok)
		assert.Equal(t, "i", ident.Name)
	})
}

func TestCompileGrouped(t *testing.T) {
	t.Parallel()

	expr := compile(t, `(42)`)
	_, ok := expr.(*core.IntLit)
	require.True(t, ok, "grouped literal should unwrap to the literal itself")

	bin := compile(t, `(a + b) * c`).(*core.BinaryExpr)
	assert.Equal(t, "*", bin.Op)
	_, ok = bin.Left.(*core.BinaryExpr)
	require.True(t, ok)
}

func TestCompileMultipleStatements(t *testing.T) {
	t.Parallel()

	exprs := compileN(t, `.a = 1; .b = 2; .c = 3`)
	assert.Len(t, exprs, 3)
	for _, e := range exprs {
		_, ok := e.(*core.AssignExpr)
		require.True(t, ok)
	}
}

func TestCompileMultipleStatementsNewline(t *testing.T) {
	t.Parallel()

	src := `
	.x = 1
	.y = 2
	`
	exprs := compileN(t, src)
	assert.Len(t, exprs, 2)
}

func TestCompileSemicolonOnlyLines(t *testing.T) {
	t.Parallel()

	exprs := compileN(t, `;; .x = 1 ;;`)
	assert.Len(t, exprs, 1)
}

func TestCompileErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		src         string
		errContains string
	}{
		{
			name:        "lexer_unexpected_char",
			src:         "@var",
			errContains: "unexpected character",
		},
		{
			name:        "assign_to_literal",
			src:         "42 = 1",
			errContains: "left side of assignment must be a variable, path, or index expression",
		},
		{
			name:        "assign_to_binary_expr",
			src:         "(a + b) = 1",
			errContains: "left side of assignment",
		},
		{
			name:        "call_on_non_ident",
			src:         "foo()()",
			errContains: "function call requires an identifier on the left",
		},
		{
			name:        "metadata_path_missing_ident",
			src:         "% 42",
			errContains: "expected metadata field name after %",
		},
		{
			name:        "unclosed_bracket",
			src:         "[1, 2",
			errContains: "",
		},
		{
			name:        "unclosed_block",
			src:         "if .x { .y = 1",
			errContains: "",
		},
		{
			name:        "del_non_path",
			src:         "del null",
			errContains: "del requires a path",
		},
		{
			name:        "del_literal",
			src:         "del 42",
			errContains: "del requires a path",
		},
		{
			name:        "for_blank_blank",
			src:         "for _, _ in .arr {}",
			errContains: "for loop must bind at least one variable",
		},
		{
			name:        "for_missing_in",
			src:         "for i .arr {}",
			errContains: "expected KW_IN",
		},
		{
			name:        "object_int_key",
			src:         "{1: 2}",
			errContains: "object key must be a string or identifier",
		},
		{
			name:        "unexpected_token",
			src:         ")",
			errContains: "unexpected token",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := mustFail(t, tc.src)
			if tc.errContains != "" {
				assert.Contains(t, err.Error(), tc.errContains)
			}
		})
	}
}

func TestNewCompilerError(t *testing.T) {
	t.Parallel()

	_, err := NewCompiler("@bad_char")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected character")
}
