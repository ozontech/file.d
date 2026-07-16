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

	for _, name := range []string{"foo", "_bar", "baz123", "x"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			expr := compile(t, name)
			ident, ok := expr.(*core.IdentExpr)
			require.True(t, ok, "expected *core.IdentExpr, got %T", expr)
			assert.Equal(t, name, ident.Name)
		})
	}
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

func TestCompilePaths(t *testing.T) {
	t.Parallel()

	runGolden(t, []goldenCase{
		{name: "root_only", src: `.`, want: `Path(.)`},
		{name: "single_field", src: `.status`, want: `Path(.status)`},
		{name: "nested_fields", src: `.user.name`, want: `Path(.user.name)`},
		{name: "integer_index", src: `.items[0]`, want: `Path(.items.[IntLit(0)])`},
		{name: "dynamic_index", src: `.items[idx]`, want: `Path(.items.[Ident(idx)])`},
		{
			name: "negative_index",
			src:  `.items[-1]`,
			want: `
Path(.items.[Unary(-)
  IntLit(1)])`,
		},
		{name: "metadata_field", src: `%ts`, want: `Path(%ts)`},
		{name: "metadata_nested", src: `%meta.key`, want: `Path(%meta.key)`},
	})
}

func TestCompilePrecedence(t *testing.T) {
	t.Parallel()

	runGolden(t, []goldenCase{
		{
			name: "mul_over_add",
			src:  `1 + 2 * 3`,
			want: `
Binary(+)
  IntLit(1)
  Binary(*)
    IntLit(2)
    IntLit(3)`,
		},
		{
			name: "left_associative_add",
			src:  `1 * 2 + 3`,
			want: `
Binary(+)
  Binary(*)
    IntLit(1)
    IntLit(2)
  IntLit(3)`,
		},
		{
			name: "and_over_or",
			src:  `a || b && c`,
			want: `
Binary(||)
  Ident(a)
  Binary(&&)
    Ident(b)
    Ident(c)`,
		},
		{
			name: "eq_over_and",
			src:  `a == b && c != d`,
			want: `
Binary(&&)
  Binary(==)
    Ident(a)
    Ident(b)
  Binary(!=)
    Ident(c)
    Ident(d)`,
		},
		{
			name: "unary_over_binary",
			src:  `!a && b`,
			want: `
Binary(&&)
  Unary(!)
    Ident(a)
  Ident(b)`,
		},
		{
			name: "grouping_overrides_precedence",
			src:  `(1 + 2) * 3`,
			want: `
Binary(*)
  Binary(+)
    IntLit(1)
    IntLit(2)
  IntLit(3)`,
		},
		{name: "grouped_literal_unwraps", src: `(42)`, want: `IntLit(42)`},
	})
}

func TestCompileAssignExpr(t *testing.T) {
	t.Parallel()

	runGolden(t, []goldenCase{
		{
			name: "to_ident",
			src:  `x = "hello"`,
			want: `
Assign
  Ident(x)
  StringLit("hello")`,
		},
		{
			name: "to_event_path",
			src:  `.foo = 42`,
			want: `
Assign
  Path(.foo)
  IntLit(42)`,
		},
		{
			name: "to_index_expr",
			src:  `arr[0] = 99`,
			want: `
Assign
  Index
    Ident(arr)
    IntLit(0)
  IntLit(99)`,
		},
		{
			name: "right_associative",
			src:  `x = y = 1`,
			want: `
Assign
  Ident(x)
  Assign
    Ident(y)
    IntLit(1)`,
		},
	})
}

func TestCompileIfExpr(t *testing.T) {
	t.Parallel()

	runGolden(t, []goldenCase{
		{
			name: "no_else",
			src:  `if .x > 0 { .y = 1 }`,
			want: `
If
  condition:
    Binary(>)
      Path(.x)
      IntLit(0)
  then:
    Assign
      Path(.y)
      IntLit(1)`,
		},
		{
			name: "with_else",
			src:  `if .ok { .r = "yes" } else { .r = "no" }`,
			want: `
If
  condition:
    Path(.ok)
  then:
    Assign
      Path(.r)
      StringLit("yes")
  else:
    Assign
      Path(.r)
      StringLit("no")`,
		},
		{
			name: "else_if_chain",
			src:  `if .s >= 500 { .sev = "crit" } else if .s >= 400 { .sev = "warn" } else { .sev = "ok" }`,
			want: `
If
  condition:
    Binary(>=)
      Path(.s)
      IntLit(500)
  then:
    Assign
      Path(.sev)
      StringLit("crit")
  else:
    If
      condition:
        Binary(>=)
          Path(.s)
          IntLit(400)
      then:
        Assign
          Path(.sev)
          StringLit("warn")
      else:
        Assign
          Path(.sev)
          StringLit("ok")`,
		},
		{
			name: "multi_statement_then_block",
			src:  `if true { .a = 1; .b = 2 }`,
			want: `
If
  condition:
    BoolLit(true)
  then:
    Assign
      Path(.a)
      IntLit(1)
    Assign
      Path(.b)
      IntLit(2)`,
		},
	})
}

func TestCompileForExpr(t *testing.T) {
	t.Parallel()

	runGolden(t, []goldenCase{
		{
			name: "index_only",
			src:  `for i in .arr {}`,
			want: `
For(index="i", item="")
  iter:
    Path(.arr)`,
		},
		{
			name: "index_and_item",
			src:  `for i, v in .arr {}`,
			want: `
For(index="i", item="v")
  iter:
    Path(.arr)`,
		},
		{
			name: "blank_index",
			src:  `for _, v in .arr {}`,
			want: `
For(index="", item="v")
  iter:
    Path(.arr)`,
		},
		{
			name: "blank_item",
			src:  `for i, _ in .arr {}`,
			want: `
For(index="i", item="")
  iter:
    Path(.arr)`,
		},
		{
			name: "body_statements",
			src:  `for i in .items { .items[i] = .items[i] + 1 }`,
			want: `
For(index="i", item="")
  iter:
    Path(.items)
  body:
    Assign
      Path(.items.[Ident(i)])
      Binary(+)
        Path(.items.[Ident(i)])
        IntLit(1)`,
		},
		{
			name: "iter_is_ident",
			src:  `for i in myArr {}`,
			want: `
For(index="i", item="")
  iter:
    Ident(myArr)`,
		},
	})
}

func TestCompileDelExpr(t *testing.T) {
	t.Parallel()

	runGolden(t, []goldenCase{
		{
			name: "event_path",
			src:  `del .secret`,
			want: `
Del
  Path(.secret)`,
		},
		{
			name: "nested_event_path",
			src:  `del .user.password`,
			want: `
Del
  Path(.user.password)`,
		},
		{
			name: "metadata_path",
			src:  `del %meta`,
			want: `
Del
  Path(%meta)`,
		},
	})
}

func TestCompileArrayExpr(t *testing.T) {
	t.Parallel()

	runGolden(t, []goldenCase{
		{name: "empty", src: `[]`, want: `Array`},
		{
			name: "integers",
			src:  `[1, 2, 3]`,
			want: `
Array
  IntLit(1)
  IntLit(2)
  IntLit(3)`,
		},
		{
			name: "mixed_types",
			src:  `[1, "two", true]`,
			want: `
Array
  IntLit(1)
  StringLit("two")
  BoolLit(true)`,
		},
	})
}

func TestCompileObjectExpr(t *testing.T) {
	t.Parallel()

	runGolden(t, []goldenCase{
		{name: "empty", src: `{}`, want: `Object`},
		{
			name: "string_key",
			src:  `{"name": "alice"}`,
			want: `
Object
  key("name"):
    StringLit("alice")`,
		},
		{
			name: "ident_key",
			src:  `{level: "info"}`,
			want: `
Object
  key("level"):
    StringLit("info")`,
		},
		{
			name: "raw_string_key",
			src:  `{s'raw\key': 1}`,
			want: `
Object
  key("raw\\key"):
    IntLit(1)`,
		},
		{
			name: "multiple_pairs_all_scalar_types",
			src:  `{"n": 42, "s": "hi", "b": true, "f": 3.14, "z": null}`,
			want: `
Object
  key("n"):
    IntLit(42)
  key("s"):
    StringLit("hi")
  key("b"):
    BoolLit(true)
  key("f"):
    FloatLit(3.14)
  key("z"):
    NullLit`,
		},
		{
			name: "bool_false_value",
			src:  `{"ok": false}`,
			want: `
Object
  key("ok"):
    BoolLit(false)`,
		},
		{
			name: "integer_zero",
			src:  `{"count": 0}`,
			want: `
Object
  key("count"):
    IntLit(0)`,
		},
	})
}

func TestCompileCallExpr(t *testing.T) {
	t.Parallel()

	runGolden(t, []goldenCase{
		{name: "no_args", src: `now()`, want: `Call(now)`},
		{
			name: "positional_args",
			src:  `upcase(.level)`,
			want: `
Call(upcase)
  Path(.level)`,
		},
		{
			name: "named_arg",
			src:  `fn(key: "value")`,
			want: `
Call(fn)
  named(key:)
    StringLit("value")`,
		},
		{
			name: "mixed_positional_and_named",
			src:  `fn(.x, sep: ",")`,
			want: `
Call(fn)
  Path(.x)
  named(sep:)
    StringLit(",")`,
		},
	})
}

func TestCompileIndexExpr(t *testing.T) {
	t.Parallel()

	runGolden(t, []goldenCase{
		{
			name: "array_index",
			src:  `arr[0]`,
			want: `
Index
  Ident(arr)
  IntLit(0)`,
		},
		{
			name: "object_string_index",
			src:  `obj["key"]`,
			want: `
Index
  Ident(obj)
  StringLit("key")`,
		},
		{
			name: "dynamic_index",
			src:  `arr[i]`,
			want: `
Index
  Ident(arr)
  Ident(i)`,
		},
	})
}

func TestCompileStatements(t *testing.T) {
	t.Parallel()

	runGolden(t, []goldenCase{
		{
			name: "semicolon_separated",
			src:  `.a = 1; .b = 2; .c = 3`,
			want: `
Assign
  Path(.a)
  IntLit(1)
Assign
  Path(.b)
  IntLit(2)
Assign
  Path(.c)
  IntLit(3)`,
		},
		{
			name: "newline_separated",
			src:  "\t.x = 1\n\t.y = 2\n",
			want: `
Assign
  Path(.x)
  IntLit(1)
Assign
  Path(.y)
  IntLit(2)`,
		},
		{
			name: "extra_semicolons_ignored",
			src:  `;; .x = 1 ;;`,
			want: `
Assign
  Path(.x)
  IntLit(1)`,
		},
		{name: "abort", src: `abort`, want: `Abort`},
	})
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
