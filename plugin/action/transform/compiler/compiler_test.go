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
		src := src
		t.Run(fmt.Sprintf("%q", src), func(t *testing.T) {
			c, err := NewCompiler(src)
			require.NoError(t, err)
			exprs, err := c.Compile()
			require.NoError(t, err)
			assert.Empty(t, exprs)
		})
	}
}

func TestCompileIntLiteral(t *testing.T) {
	tests := []struct {
		src  string
		want int64
	}{
		{"0", 0},
		{"42", 42},
		{"1000000", 1000000},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.src, func(t *testing.T) {
			expr := compile(t, tc.src)
			lit, ok := expr.(*core.IntLit)
			require.True(t, ok, "expected *core.IntLit, got %T", expr)
			assert.Equal(t, tc.want, lit.Value)
		})
	}
}

func TestCompileFloatLiteral(t *testing.T) {
	tests := []struct {
		src  string
		want float64
	}{
		{"3.14", 3.14},
		{"0.5", 0.5},
		{"1e10", 1e10},
		{"1.5e-3", 1.5e-3},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.src, func(t *testing.T) {
			expr := compile(t, tc.src)
			lit, ok := expr.(*core.FloatLit)
			require.True(t, ok, "expected *core.FloatLit, got %T", expr)
			assert.Equal(t, tc.want, lit.Value)
		})
	}
}

func TestCompileStringLiteral(t *testing.T) {
	tests := []struct {
		name string
		src  string
		want string
	}{
		{"simple", `"hello"`, "hello"},
		{"escaped_quote", `"say \"hi\""`, `say "hi"`},
		{"escape_newline", `"line\nbreak"`, "line\nbreak"},
		{"escape_tab", `"tab\there"`, "tab\there"},
		{"empty_string", `""`, ""},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			expr := compile(t, tc.src)
			lit, ok := expr.(*core.StringLit)
			require.True(t, ok, "expected *core.StringLit, got %T", expr)
			assert.Equal(t, tc.want, lit.Value)
		})
	}
}

func TestCompileRawStringLiteral(t *testing.T) {
	tests := []struct {
		name string
		src  string
		want string
	}{
		{"simple", `s'hello'`, "hello"},
		{"backslash_preserved", `s'C:\new\path'`, `C:\new\path`},
		{"no_escape_processing", `s'no\nescape'`, `no\nescape`},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			expr := compile(t, tc.src)
			lit, ok := expr.(*core.StringLit)
			require.True(t, ok, "expected *core.StringLit (raw), got %T", expr)
			assert.Equal(t, tc.want, lit.Value)
		})
	}
}

func TestCompileBoolLiterals(t *testing.T) {
	expr := compile(t, "true")
	lit, ok := expr.(*core.BoolLit)
	require.True(t, ok)
	assert.True(t, lit.Value)

	expr = compile(t, "false")
	lit, ok = expr.(*core.BoolLit)
	require.True(t, ok)
	assert.False(t, lit.Value)
}

func TestCompileNullLiteral(t *testing.T) {
	expr := compile(t, "null")
	_, ok := expr.(*core.NullLit)
	assert.True(t, ok)
}

func TestCompileRegexLiteral(t *testing.T) {
	tests := []struct {
		src     string
		pattern string
	}{
		{`r'\d+'`, `\d+`},
		{`r'\w+@\w+\.\w+'`, `\w+@\w+\.\w+`},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.pattern, func(t *testing.T) {
			expr := compile(t, tc.src)
			lit, ok := expr.(*core.RegexLit)
			require.True(t, ok, "expected *core.RegexLit, got %T", expr)
			assert.Equal(t, tc.pattern, lit.Pattern)
		})
	}
}

func TestCompileTimestampLiteral(t *testing.T) {
	expr := compile(t, `t'2024-01-15T10:30:00Z'`)
	lit, ok := expr.(*core.TimestampLit)
	require.True(t, ok, "expected *core.TimestampLit, got %T", expr)
	assert.Equal(t, "2024-01-15T10:30:00Z", lit.Value)
}

func TestCompileIdent(t *testing.T) {
	tests := []string{"foo", "_bar", "baz123", "x"}
	for _, name := range tests {
		name := name
		t.Run(name, func(t *testing.T) {
			expr := compile(t, name)
			ident, ok := expr.(*core.IdentExpr)
			require.True(t, ok, "expected *core.IdentExpr, got %T", expr)
			assert.Equal(t, name, ident.Name)
		})
	}
}

func TestCompileEventPath(t *testing.T) {
	t.Run("root_only", func(t *testing.T) {
		expr := compile(t, ".")
		p, ok := expr.(*core.PathExpr)
		require.True(t, ok)
		assert.Equal(t, core.EventRoot, p.Root)
		assert.Empty(t, p.Segments)
	})

	t.Run("single_field", func(t *testing.T) {
		expr := compile(t, ".status")
		p, ok := expr.(*core.PathExpr)
		require.True(t, ok)
		assert.Equal(t, core.EventRoot, p.Root)
		require.Len(t, p.Segments, 1)
		assert.Equal(t, "status", p.Segments[0].Field)
	})

	t.Run("nested_fields", func(t *testing.T) {
		expr := compile(t, ".user.name")
		p, ok := expr.(*core.PathExpr)
		require.True(t, ok)
		require.Len(t, p.Segments, 2)
		assert.Equal(t, "user", p.Segments[0].Field)
		assert.Equal(t, "name", p.Segments[1].Field)
	})

	t.Run("field_with_integer_index", func(t *testing.T) {
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
		expr := compile(t, ".items[idx]")
		p, ok := expr.(*core.PathExpr)
		require.True(t, ok)
		require.Len(t, p.Segments, 2)
		idxExpr, ok := p.Segments[1].Index.(*core.IdentExpr)
		require.True(t, ok)
		assert.Equal(t, "idx", idxExpr.Name)
	})

	t.Run("field_with_negative_index", func(t *testing.T) {
		expr := compile(t, ".items[-1]")
		p, ok := expr.(*core.PathExpr)
		require.True(t, ok)
		require.Len(t, p.Segments, 2)
		_, ok = p.Segments[1].Index.(*core.UnaryExpr)
		require.True(t, ok, "expected unary expression for negative index")
	})
}

func TestCompileMetadataPath(t *testing.T) {
	t.Run("simple_field", func(t *testing.T) {
		expr := compile(t, "%ts")
		p, ok := expr.(*core.PathExpr)
		require.True(t, ok)
		assert.Equal(t, core.MetadataRoot, p.Root)
		require.Len(t, p.Segments, 1)
		assert.Equal(t, "ts", p.Segments[0].Field)
	})

	t.Run("nested", func(t *testing.T) {
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
	tests := []struct {
		src string
		op  string
	}{
		{"!foo", "!"},
		{"-42", "-"},
		{"!.flag", "!"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.src, func(t *testing.T) {
			expr := compile(t, tc.src)
			u, ok := expr.(*core.UnaryExpr)
			require.True(t, ok, "expected *core.UnaryExpr, got %T", expr)
			assert.Equal(t, tc.op, u.Op)
		})
	}
}

func TestCompileBinaryExpr(t *testing.T) {
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
		tc := tc
		t.Run(tc.op, func(t *testing.T) {
			expr := compile(t, tc.src)
			bin, ok := expr.(*core.BinaryExpr)
			require.True(t, ok, "expected *core.BinaryExpr for %q, got %T", tc.src, expr)
			assert.Equal(t, tc.op, bin.Op)
		})
	}
}

func TestCompileOperatorPrecedence(t *testing.T) {
	t.Run("mul_over_add", func(t *testing.T) {
		bin := compile(t, "1 + 2 * 3").(*core.BinaryExpr)
		assert.Equal(t, "+", bin.Op)
		rightBin, ok := bin.Right.(*core.BinaryExpr)
		require.True(t, ok)
		assert.Equal(t, "*", rightBin.Op)
	})

	t.Run("left_associative_add", func(t *testing.T) {
		bin := compile(t, "1 * 2 + 3").(*core.BinaryExpr)
		assert.Equal(t, "+", bin.Op)
		leftBin, ok := bin.Left.(*core.BinaryExpr)
		require.True(t, ok)
		assert.Equal(t, "*", leftBin.Op)
	})

	t.Run("and_over_or", func(t *testing.T) {
		bin := compile(t, "a || b && c").(*core.BinaryExpr)
		assert.Equal(t, "||", bin.Op)
		rightBin, ok := bin.Right.(*core.BinaryExpr)
		require.True(t, ok)
		assert.Equal(t, "&&", rightBin.Op)
	})

	t.Run("eq_over_and", func(t *testing.T) {
		bin := compile(t, "a == b && c != d").(*core.BinaryExpr)
		assert.Equal(t, "&&", bin.Op)
		_, ok := bin.Left.(*core.BinaryExpr)
		require.True(t, ok, "left should be == expr")
		_, ok = bin.Right.(*core.BinaryExpr)
		require.True(t, ok, "right should be != expr")
	})

	t.Run("unary_over_binary", func(t *testing.T) {
		bin := compile(t, "!a && b").(*core.BinaryExpr)
		assert.Equal(t, "&&", bin.Op)
		_, ok := bin.Left.(*core.UnaryExpr)
		require.True(t, ok, "left should be unary ! expression")
	})

	t.Run("grouping_overrides_precedence", func(t *testing.T) {
		bin := compile(t, "(1 + 2) * 3").(*core.BinaryExpr)
		assert.Equal(t, "*", bin.Op)
		_, ok := bin.Left.(*core.BinaryExpr)
		require.True(t, ok, "left should be grouped + expression")
	})
}

func TestCompileAssignExpr(t *testing.T) {
	t.Run("to_ident", func(t *testing.T) {
		expr := compile(t, `x = "hello"`)
		a, ok := expr.(*core.AssignExpr)
		require.True(t, ok)
		_, ok = a.Target.(*core.IdentExpr)
		require.True(t, ok, "target should be IdentExpr")
	})

	t.Run("to_event_path", func(t *testing.T) {
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
		expr := compile(t, `arr[0] = 99`)
		a, ok := expr.(*core.AssignExpr)
		require.True(t, ok)
		_, ok = a.Target.(*core.IndexExpr)
		require.True(t, ok, "target should be IndexExpr")
	})

	t.Run("right_associative", func(t *testing.T) {
		expr := compile(t, `x = y = 1`)
		outer, ok := expr.(*core.AssignExpr)
		require.True(t, ok)
		_, ok = outer.Value.(*core.AssignExpr)
		require.True(t, ok, "right side should also be AssignExpr (right-assoc)")
	})
}

func TestCompileIfExpr(t *testing.T) {
	t.Run("no_else", func(t *testing.T) {
		expr := compile(t, `if .x > 0 { .y = 1 }`)
		ifExpr, ok := expr.(*core.IfExpr)
		require.True(t, ok)
		assert.NotNil(t, ifExpr.Condition)
		assert.Len(t, ifExpr.Then, 1)
		assert.Empty(t, ifExpr.Else)
	})

	t.Run("with_else", func(t *testing.T) {
		expr := compile(t, `if .ok { .r = "yes" } else { .r = "no" }`)
		ifExpr, ok := expr.(*core.IfExpr)
		require.True(t, ok)
		assert.Len(t, ifExpr.Then, 1)
		assert.Len(t, ifExpr.Else, 1)
	})

	t.Run("else_if_chain", func(t *testing.T) {
		src := `if .s >= 500 { .sev = "crit" } else if .s >= 400 { .sev = "warn" } else { .sev = "ok" }`
		expr := compile(t, src)
		ifExpr, ok := expr.(*core.IfExpr)
		require.True(t, ok)
		require.Len(t, ifExpr.Else, 1)
		_, ok = ifExpr.Else[0].(*core.IfExpr)
		require.True(t, ok, "else branch should be another IfExpr")
	})

	t.Run("multi_statement_then_block", func(t *testing.T) {
		src := `if true { .a = 1; .b = 2 }`
		expr := compile(t, src)
		ifExpr, ok := expr.(*core.IfExpr)
		require.True(t, ok)
		assert.Len(t, ifExpr.Then, 2)
	})
}

func TestCompileForExpr(t *testing.T) {
	t.Run("index_only", func(t *testing.T) {
		expr := compile(t, `for i in .arr {}`)
		f, ok := expr.(*core.ForExpr)
		require.True(t, ok)
		assert.Equal(t, "i", f.Index)
		assert.Equal(t, "", f.Item)
	})

	t.Run("index_and_item", func(t *testing.T) {
		expr := compile(t, `for i, v in .arr {}`)
		f, ok := expr.(*core.ForExpr)
		require.True(t, ok)
		assert.Equal(t, "i", f.Index)
		assert.Equal(t, "v", f.Item)
	})

	t.Run("blank_index", func(t *testing.T) {
		expr := compile(t, `for _, v in .arr {}`)
		f, ok := expr.(*core.ForExpr)
		require.True(t, ok)
		assert.Equal(t, "", f.Index)
		assert.Equal(t, "v", f.Item)
	})

	t.Run("blank_item", func(t *testing.T) {
		expr := compile(t, `for i, _ in .arr {}`)
		f, ok := expr.(*core.ForExpr)
		require.True(t, ok)
		assert.Equal(t, "i", f.Index)
		assert.Equal(t, "", f.Item)
	})

	t.Run("body_statements", func(t *testing.T) {
		expr := compile(t, `for i in .items { .items[i] = .items[i] + 1 }`)
		f, ok := expr.(*core.ForExpr)
		require.True(t, ok)
		assert.Len(t, f.Body, 1)
	})

	t.Run("iter_is_ident", func(t *testing.T) {
		expr := compile(t, `for i in myArr {}`)
		f, ok := expr.(*core.ForExpr)
		require.True(t, ok)
		ident, ok := f.Iter.(*core.IdentExpr)
		require.True(t, ok)
		assert.Equal(t, "myArr", ident.Name)
	})
}

func TestCompileDelExpr(t *testing.T) {
	t.Run("event_path", func(t *testing.T) {
		expr := compile(t, `del .secret`)
		d, ok := expr.(*core.DelExpr)
		require.True(t, ok)
		assert.Equal(t, core.EventRoot, d.Target.Root)
		require.Len(t, d.Target.Segments, 1)
		assert.Equal(t, "secret", d.Target.Segments[0].Field)
	})

	t.Run("nested_event_path", func(t *testing.T) {
		expr := compile(t, `del .user.password`)
		d, ok := expr.(*core.DelExpr)
		require.True(t, ok)
		require.Len(t, d.Target.Segments, 2)
	})

	t.Run("metadata_path", func(t *testing.T) {
		expr := compile(t, `del %meta`)
		d, ok := expr.(*core.DelExpr)
		require.True(t, ok)
		assert.Equal(t, core.MetadataRoot, d.Target.Root)
	})
}

func TestCompileArrayExpr(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		expr := compile(t, `[]`)
		arr, ok := expr.(*core.ArrayExpr)
		require.True(t, ok)
		assert.Empty(t, arr.Elements)
	})

	t.Run("integers", func(t *testing.T) {
		expr := compile(t, `[1, 2, 3]`)
		arr, ok := expr.(*core.ArrayExpr)
		require.True(t, ok)
		assert.Len(t, arr.Elements, 3)
	})

	t.Run("mixed_types", func(t *testing.T) {
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
		expr := compile(t, `[1, 2]`)
		arr, ok := expr.(*core.ArrayExpr)
		require.True(t, ok)
		assert.Len(t, arr.Elements, 2)
	})
}

func TestCompileObjectExpr(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		expr := compile(t, `{}`)
		obj, ok := expr.(*core.ObjectExpr)
		require.True(t, ok)
		assert.Empty(t, obj.Pairs)
	})

	t.Run("string_key", func(t *testing.T) {
		expr := compile(t, `{"name": "alice"}`)
		obj, ok := expr.(*core.ObjectExpr)
		require.True(t, ok)
		require.Len(t, obj.Pairs, 1)
		assert.Equal(t, "name", obj.Pairs[0].Key)
	})

	t.Run("ident_key", func(t *testing.T) {
		expr := compile(t, `{level: "info"}`)
		obj, ok := expr.(*core.ObjectExpr)
		require.True(t, ok)
		require.Len(t, obj.Pairs, 1)
		assert.Equal(t, "level", obj.Pairs[0].Key)
	})

	t.Run("raw_string_key", func(t *testing.T) {
		expr := compile(t, `{s'raw\key': 1}`)
		obj, ok := expr.(*core.ObjectExpr)
		require.True(t, ok)
		require.Len(t, obj.Pairs, 1)
		assert.Equal(t, `raw\key`, obj.Pairs[0].Key)
	})

	t.Run("multiple_pairs", func(t *testing.T) {
		expr := compile(t, `{"a": 1, "b": 2}`)
		obj, ok := expr.(*core.ObjectExpr)
		require.True(t, ok)
		assert.Len(t, obj.Pairs, 2)
	})
}

func TestCompileCallExpr(t *testing.T) {
	t.Run("no_args", func(t *testing.T) {
		expr := compile(t, `now()`)
		call, ok := expr.(*core.CallExpr)
		require.True(t, ok)
		assert.Equal(t, "now", call.Name)
		assert.Empty(t, call.Args)
	})

	t.Run("positional_args", func(t *testing.T) {
		expr := compile(t, `upcase(.level)`)
		call, ok := expr.(*core.CallExpr)
		require.True(t, ok)
		assert.Equal(t, "upcase", call.Name)
		require.Len(t, call.Args, 1)
		assert.Equal(t, "", call.Args[0].Name, "should be positional (no name)")
	})

	t.Run("named_arg", func(t *testing.T) {
		expr := compile(t, `fn(key: "value")`)
		call, ok := expr.(*core.CallExpr)
		require.True(t, ok)
		require.Len(t, call.Args, 1)
		assert.Equal(t, "key", call.Args[0].Name)
		_, ok = call.Args[0].Value.(*core.StringLit)
		require.True(t, ok)
	})

	t.Run("mixed_positional_and_named", func(t *testing.T) {
		expr := compile(t, `fn(.x, sep: ",")`)
		call, ok := expr.(*core.CallExpr)
		require.True(t, ok)
		require.Len(t, call.Args, 2)
		assert.Equal(t, "", call.Args[0].Name, "first arg is positional")
		assert.Equal(t, "sep", call.Args[1].Name)
	})
}

func TestCompileIndexExpr(t *testing.T) {
	t.Run("array_index", func(t *testing.T) {
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
		expr := compile(t, `obj["key"]`)
		idx, ok := expr.(*core.IndexExpr)
		require.True(t, ok)
		lit, ok := idx.Index.(*core.StringLit)
		require.True(t, ok)
		assert.Equal(t, "key", lit.Value)
	})

	t.Run("dynamic_index", func(t *testing.T) {
		expr := compile(t, `arr[i]`)
		idx, ok := expr.(*core.IndexExpr)
		require.True(t, ok)
		ident, ok := idx.Index.(*core.IdentExpr)
		require.True(t, ok)
		assert.Equal(t, "i", ident.Name)
	})
}

func TestCompileGrouped(t *testing.T) {
	expr := compile(t, `(42)`)
	_, ok := expr.(*core.IntLit)
	require.True(t, ok, "grouped literal should unwrap to the literal itself")

	bin := compile(t, `(a + b) * c`).(*core.BinaryExpr)
	assert.Equal(t, "*", bin.Op)
	_, ok = bin.Left.(*core.BinaryExpr)
	require.True(t, ok)
}

func TestCompileMultipleStatements(t *testing.T) {
	exprs := compileN(t, `.a = 1; .b = 2; .c = 3`)
	assert.Len(t, exprs, 3)
	for _, e := range exprs {
		_, ok := e.(*core.AssignExpr)
		require.True(t, ok)
	}
}

func TestCompileMultipleStatementsNewline(t *testing.T) {
	src := `
	.x = 1
	.y = 2
	`
	exprs := compileN(t, src)
	assert.Len(t, exprs, 2)
}

func TestCompileSemicolonOnlyLines(t *testing.T) {
	exprs := compileN(t, `;; .x = 1 ;;`)
	assert.Len(t, exprs, 1)
}

func TestCompileErrors(t *testing.T) {
	tests := []struct {
		name        string
		src         string
		errContains string
	}{

		{
			"lexer_unexpected_char",
			"@var",
			"unexpected character",
		},

		{
			"assign_to_literal",
			"42 = 1",
			"left side of assignment must be a variable, path, or index expression",
		},
		{
			"assign_to_binary_expr",
			"(a + b) = 1",
			"left side of assignment",
		},

		{
			"call_on_non_ident",
			"foo()()",
			"function call requires an identifier on the left",
		},

		{
			"metadata_path_missing_ident",
			"% 42",
			"expected metadata field name after %",
		},

		{
			"unclosed_bracket",
			"[1, 2",
			"",
		},

		{
			"unclosed_block",
			"if .x { .y = 1",
			"",
		},

		{
			"del_non_path",
			"del null",
			"del requires a path",
		},
		{
			"del_literal",
			"del 42",
			"del requires a path",
		},

		{
			"for_blank_blank",
			"for _, _ in .arr {}",
			"for loop must bind at least one variable",
		},

		{
			"for_missing_in",
			"for i .arr {}",
			"expected KW_IN",
		},

		{
			"object_int_key",
			"{1: 2}",
			"object key must be a string or identifier",
		},

		{
			"unexpected_token",
			")",
			"unexpected token",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := mustFail(t, tc.src)
			if tc.errContains != "" {
				assert.Contains(t, err.Error(), tc.errContains)
			}
		})
	}
}

func TestNewCompilerError(t *testing.T) {
	_, err := NewCompiler("@bad_char")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected character")
}
