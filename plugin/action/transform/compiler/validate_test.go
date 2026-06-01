package compiler

import (
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/ozontech/file.d/plugin/action/transform/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func n() core.Node { return core.NewNode(parser.Position{Line: 1, Column: 1}) }

func compileExprs(t *testing.T, src string) []core.Expr {
	t.Helper()
	return compileN(t, src)
}

func TestValidateRegexValid(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
	}{
		{"simple_digit", `\d+`},
		{"word_char", `\w+`},
		{"email_like", `\w+@\w+\.\w+`},
		{"anchored", `^\d{4}-\d{2}-\d{2}$`},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			expr := &core.RegexLit{Node: n(), Pattern: tc.pattern}
			err := ValidateCalls([]core.Expr{expr}, nil)
			require.NoError(t, err)
			assert.NotNil(t, expr.Compiled, "Compiled should be set after validation")
			assert.Equal(t, tc.pattern, expr.Compiled.String())
		})
	}
}

func TestValidateRegexInvalid(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
	}{
		{"unclosed_bracket", `[invalid`},
		{"bad_quantifier", `*invalid`},
		{"unclosed_group", `(no close`},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			expr := &core.RegexLit{Node: n(), Pattern: tc.pattern}
			err := ValidateCalls([]core.Expr{expr}, nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "invalid regex pattern")
		})
	}
}

func TestValidateTimestampValid(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{"rfc3339", "2024-01-15T10:30:00Z"},
		{"rfc3339_nano", "2024-01-15T10:30:00.123456789Z"},
		{"rfc3339_offset", "2024-01-15T10:30:00+03:00"},
		{"datetime_no_zone", "2024-01-15T10:30:00"},
		{"date_only", "2024-01-15"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			expr := &core.TimestampLit{Node: n(), Value: tc.value}
			err := ValidateCalls([]core.Expr{expr}, nil)
			require.NoError(t, err)
			assert.False(t, expr.Parsed.IsZero(), "Parsed should be set after validation")
		})
	}
}

func TestValidateTimestampInvalid(t *testing.T) {
	tests := []struct {
		name  string
		value string
	}{
		{"garbage", "not-a-timestamp"},
		{"partial_date", "2024-13"},
		{"unix_epoch", "1700000000"},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			expr := &core.TimestampLit{Node: n(), Value: tc.value}
			err := ValidateCalls([]core.Expr{expr}, nil)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "cannot parse")
		})
	}
}

func TestValidateDuplicateObjectKey(t *testing.T) {
	t.Run("duplicate_key_error", func(t *testing.T) {
		expr := &core.ObjectExpr{
			Node: n(),
			Pairs: []core.KVPair{
				{Key: "a", Value: &core.IntLit{Node: n(), Value: 1}},
				{Key: "b", Value: &core.IntLit{Node: n(), Value: 2}},
				{Key: "a", Value: &core.IntLit{Node: n(), Value: 3}},
			},
		}
		err := ValidateCalls([]core.Expr{expr}, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "duplicate object key")
		assert.Contains(t, err.Error(), `"a"`)
	})

	t.Run("no_duplicate_ok", func(t *testing.T) {
		expr := &core.ObjectExpr{
			Node: n(),
			Pairs: []core.KVPair{
				{Key: "x", Value: &core.IntLit{Node: n(), Value: 1}},
				{Key: "y", Value: &core.IntLit{Node: n(), Value: 2}},
			},
		}
		err := ValidateCalls([]core.Expr{expr}, nil)
		require.NoError(t, err)
	})

	t.Run("empty_object_ok", func(t *testing.T) {
		expr := &core.ObjectExpr{Node: n()}
		err := ValidateCalls([]core.Expr{expr}, nil)
		require.NoError(t, err)
	})
}

func TestValidateNestedRegexInBinary(t *testing.T) {
	expr := &core.BinaryExpr{
		Node:  n(),
		Op:    "==",
		Left:  &core.StringLit{Node: n(), Value: "test"},
		Right: &core.RegexLit{Node: n(), Pattern: `\d+`},
	}
	err := ValidateCalls([]core.Expr{expr}, nil)
	require.NoError(t, err)
}

func TestValidateNestedInvalidRegexInsideArray(t *testing.T) {
	expr := &core.ArrayExpr{
		Node: n(),
		Elements: []core.Expr{
			&core.StringLit{Node: n(), Value: "ok"},
			&core.RegexLit{Node: n(), Pattern: `[bad`},
			&core.IntLit{Node: n(), Value: 1},
		},
	}
	err := ValidateCalls([]core.Expr{expr}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid regex pattern")
}

func TestValidateDuplicateKeyInNestedObject(t *testing.T) {
	innerObj := &core.ObjectExpr{
		Node: n(),
		Pairs: []core.KVPair{
			{Key: "z", Value: &core.NullLit{Node: n()}},
			{Key: "z", Value: &core.NullLit{Node: n()}},
		},
	}
	ifExpr := &core.IfExpr{
		Node:      n(),
		Condition: &core.BoolLit{Node: n(), Value: true},
		Then:      []core.Expr{&core.AssignExpr{Node: n(), Target: &core.IdentExpr{Node: n(), Name: "x"}, Value: innerObj}},
	}
	err := ValidateCalls([]core.Expr{ifExpr}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate object key")
}

func TestValidateFromCompiledRegex(t *testing.T) {
	exprs := compileExprs(t, `r'\d+'`)
	err := ValidateCalls(exprs, nil)
	require.NoError(t, err)
	lit, ok := exprs[0].(*core.RegexLit)
	require.True(t, ok)
	assert.NotNil(t, lit.Compiled)
}

func TestValidateFromCompiledTimestamp(t *testing.T) {
	exprs := compileExprs(t, `t'2024-06-01T00:00:00Z'`)
	err := ValidateCalls(exprs, nil)
	require.NoError(t, err)
	lit, ok := exprs[0].(*core.TimestampLit)
	require.True(t, ok)
	assert.False(t, lit.Parsed.IsZero())
}

func TestValidateCompiledDuplicateKey(t *testing.T) {
	exprs := compileExprs(t, `{"a": 1, "a": 2}`)
	err := ValidateCalls(exprs, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "duplicate object key")
	assert.Contains(t, err.Error(), `"a"`)
}

func TestValidateEmptyExprList(t *testing.T) {
	err := ValidateCalls(nil, nil)
	require.NoError(t, err)

	err = ValidateCalls([]core.Expr{}, nil)
	require.NoError(t, err)
}
