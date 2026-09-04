package compiler

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemberAccess(t *testing.T) {
	t.Parallel()

	runGolden(t, []goldenCase{
		{
			name: "ident_dot_field",
			src:  `m.level`,
			want: `
Index
  Ident(m)
  StringLit("level")`,
		},
		{
			name: "chained",
			src:  `m.a.b`,
			want: `
Index
  Index
    Ident(m)
    StringLit("a")
  StringLit("b")`,
		},
		{
			name: "on_call_result",
			src:  `upcase("x").len`,
			want: `
Index
  Call(upcase)
    StringLit("x")
  StringLit("len")`,
		},
		{
			name: "string_key",
			src:  `m."key with spaces"`,
			want: `
Index
  Ident(m)
  StringLit("key with spaces")`,
		},
		{
			name: "assignable",
			src:  `m.level = "INFO"`,
			want: `
Assign
  Index
    Ident(m)
    StringLit("level")
  StringLit("INFO")`,
		},
		{
			name: "dot_on_new_line_starts_event_path",
			src:  "x = m\n.level = 1",
			want: `
Assign
  Ident(x)
  Ident(m)
Assign
  Path(.level)
  IntLit(1)`,
		},
		{
			name: "semicolon_separates_statements",
			src:  `x = 1; .y = 2`,
			want: `
Assign
  Ident(x)
  IntLit(1)
Assign
  Path(.y)
  IntLit(2)`,
		},
		{
			name: "event_path_unaffected",
			src:  `.a.b.c`,
			want: `
Path(.a.b.c)`,
		},
	})
}

func TestMemberAccessErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		src         string
		errContains string
	}{
		{"space_before_dot", `x = m .y = 1`, "newline or ';'"},
		{"space_after_dot", `x = m. y`, "immediately follow '.'"},
		{"one_line_statements", `.a = 1 .b = 2`, "newline or ';'"},
		{"on_literal", `x = 1.b`, "member access is only allowed"},
		{"spaced_path_dot", `.a .b = 1`, "newline or ';'"},
		{"space_inside_path", `.a. b = 1`, "immediately follow '.'"},
		{"space_after_leading_path_dot", `. a = 1`, "immediately follow '.'"},
		{"missing_field_after_dot", `m.`, "expected field name after '.'"},
		{"unseparated_expressions", `x = 1 y = 2`, "newline or ';'"},
		{"unseparated_expressions_in_block", `if c { x = 1 y = 2 }`, "newline or ';'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := mustFail(t, tt.src)
			assert.Contains(t, err.Error(), tt.errContains)
		})
	}
}
