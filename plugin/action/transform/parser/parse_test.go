package parser

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type tokenCase struct {
	typ    TokenType
	lexeme string
}

func TestParse(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		tokens  []tokenCase
		wantErr bool
	}{
		{
			name:  "all_keywords",
			input: "if else true false null abort del for in",
			tokens: []tokenCase{
				{KW_IF, "if"},
				{KW_ELSE, "else"},
				{KW_TRUE, "true"},
				{KW_FALSE, "false"},
				{KW_NULL, "null"},
				{KW_ABORT, "abort"},
				{KW_DEL, "del"},
				{KW_FOR, "for"},
				{KW_IN, "in"},
			},
		},
		{
			name:  "identifiers_simple",
			input: "foo _bar baz123 _",
			tokens: []tokenCase{
				{IDENT, "foo"},
				{IDENT, "_bar"},
				{IDENT, "baz123"},
				{IDENT, "_"},
			},
		},
		{
			name:  "ident_prefixed_with_keyword",
			input: "iffy trueness false_alarm null_check",
			tokens: []tokenCase{
				{IDENT, "iffy"},
				{IDENT, "trueness"},
				{IDENT, "false_alarm"},
				{IDENT, "null_check"},
			},
		},
		{
			name:  "integer_literals",
			input: "0 42 100",
			tokens: []tokenCase{
				{LIT_INTEGER, "0"},
				{LIT_INTEGER, "42"},
				{LIT_INTEGER, "100"},
			},
		},
		{
			name:  "float_decimal",
			input: "3.14 0.5",
			tokens: []tokenCase{
				{LIT_FLOAT, "3.14"},
				{LIT_FLOAT, "0.5"},
			},
		},
		{
			name:  "float_scientific",
			input: "1.5e10 1.5e+10 1.5e-10 1e10 1e+10 1e-10",
			tokens: []tokenCase{
				{LIT_FLOAT, "1.5e10"},
				{LIT_FLOAT, "1.5e+10"},
				{LIT_FLOAT, "1.5e-10"},
				{LIT_FLOAT, "1e10"},
				{LIT_FLOAT, "1e+10"},
				{LIT_FLOAT, "1e-10"},
			},
		},
		{
			name:  "string_simple",
			input: `"hello"`,
			tokens: []tokenCase{
				{LIT_STRING, `"hello"`},
			},
		},
		{
			name:  "string_with_escaped_quote",
			input: `"say \"hi\""`,
			tokens: []tokenCase{
				{LIT_STRING, `"say \"hi\""`},
			},
		},
		{
			name:  "string_with_backslash_escape",
			input: `"line1\nline2"`,
			tokens: []tokenCase{
				{LIT_STRING, `"line1\nline2"`},
			},
		},
		{
			name:  "raw_string_backslashes_preserved",
			input: `s'C:\new\folder'`,
			tokens: []tokenCase{
				{LIT_STRING_RAW, `s'C:\new\folder'`},
			},
		},
		{
			name:  "raw_string_with_escaped_quote",
			input: `s'it\'s fine'`,
			tokens: []tokenCase{
				{LIT_STRING_RAW, `s'it\'s fine'`},
			},
		},
		{
			name:  "regex_simple",
			input: `r'\d+'`,
			tokens: []tokenCase{
				{LIT_REGEX, `r'\d+'`},
			},
		},
		{
			name:  "regex_complex",
			input: `r'\w+@\w+\.\w+'`,
			tokens: []tokenCase{
				{LIT_REGEX, `r'\w+@\w+\.\w+'`},
			},
		},
		{
			name:  "timestamp",
			input: `t'2024-01-01T00:00:00Z'`,
			tokens: []tokenCase{
				{LIT_TIMESTAMP, `t'2024-01-01T00:00:00Z'`},
			},
		},
		{
			name:  "comparison_operators",
			input: "== != <= >= < >",
			tokens: []tokenCase{
				{OP_EQ, "=="},
				{OP_NEQ, "!="},
				{OP_LTE, "<="},
				{OP_GTE, ">="},
				{OP_LT, "<"},
				{OP_GT, ">"},
			},
		},
		{
			name:  "logical_operators",
			input: "&& ||",
			tokens: []tokenCase{
				{OP_AND, "&&"},
				{OP_OR, "||"},
			},
		},
		{
			name:  "assign_vs_eq",
			input: "= ==",
			tokens: []tokenCase{
				{OP_ASSIGN, "="},
				{OP_EQ, "=="},
			},
		},
		{
			name:  "arithmetic_and_bang",
			input: "+ - * / % !",
			tokens: []tokenCase{
				{PLUS, "+"},
				{MINUS, "-"},
				{STAR, "*"},
				{SLASH, "/"},
				{PERCENT, "%"},
				{BANG, "!"},
			},
		},
		{
			name:  "all_delimiters",
			input: "( ) { } [ ] , : ; .",
			tokens: []tokenCase{
				{LPAREN, "("},
				{RPAREN, ")"},
				{LBRACE, "{"},
				{RBRACE, "}"},
				{LBRACKET, "["},
				{RBRACKET, "]"},
				{COMMA, ","},
				{COLON, ":"},
				{SEMICOLON, ";"},
				{DOT, "."},
			},
		},
		{
			name:   "empty_input",
			input:  "",
			tokens: nil,
		},
		{
			name:   "only_whitespace",
			input:  "   \t\r\n  ",
			tokens: nil,
		},
		{
			name:   "only_comment",
			input:  "# this is a comment",
			tokens: nil,
		},
		{
			name:  "skip_whitespace_between_tokens",
			input: "   foo   \t\r\n  bar  ",
			tokens: []tokenCase{
				{IDENT, "foo"},
				{IDENT, "bar"},
			},
		},
		{
			name:  "skip_inline_and_standalone_comments",
			input: "# header\nfoo # inline\nbar",
			tokens: []tokenCase{
				{IDENT, "foo"},
				{IDENT, "bar"},
			},
		},
		{
			name:  "field_assign_string",
			input: `.msg = "hello"`,
			tokens: []tokenCase{
				{DOT, "."},
				{IDENT, "msg"},
				{OP_ASSIGN, "="},
				{LIT_STRING, `"hello"`},
			},
		},
		{
			name:  "binary_condition",
			input: `.status >= 500 && .name != "ok"`,
			tokens: []tokenCase{
				{DOT, "."},
				{IDENT, "status"},
				{OP_GTE, ">="},
				{LIT_INTEGER, "500"},
				{OP_AND, "&&"},
				{DOT, "."},
				{IDENT, "name"},
				{OP_NEQ, "!="},
				{LIT_STRING, `"ok"`},
			},
		},
		{
			name:  "if_block",
			input: "if .x > 0 { abort }",
			tokens: []tokenCase{
				{KW_IF, "if"},
				{DOT, "."},
				{IDENT, "x"},
				{OP_GT, ">"},
				{LIT_INTEGER, "0"},
				{LBRACE, "{"},
				{KW_ABORT, "abort"},
				{RBRACE, "}"},
			},
		},
		{
			name:  "array_literal",
			input: "[1, 2, 3]",
			tokens: []tokenCase{
				{LBRACKET, "["},
				{LIT_INTEGER, "1"},
				{COMMA, ","},
				{LIT_INTEGER, "2"},
				{COMMA, ","},
				{LIT_INTEGER, "3"},
				{RBRACKET, "]"},
			},
		},
		{
			name:  "function_call",
			input: "upcase(.name)",
			tokens: []tokenCase{
				{IDENT, "upcase"},
				{LPAREN, "("},
				{DOT, "."},
				{IDENT, "name"},
				{RPAREN, ")"},
			},
		},
		{
			name:  "for_with_two_vars",
			input: "for i, item in .items { }",
			tokens: []tokenCase{
				{KW_FOR, "for"},
				{IDENT, "i"},
				{COMMA, ","},
				{IDENT, "item"},
				{KW_IN, "in"},
				{DOT, "."},
				{IDENT, "items"},
				{LBRACE, "{"},
				{RBRACE, "}"},
			},
		},
		{
			name:    "unexpected_at_sign",
			input:   "@var",
			wantErr: true,
		},
		{
			name:    "unexpected_tilde",
			input:   "~",
			wantErr: true,
		},
		{
			name:    "unexpected_dollar",
			input:   "$foo",
			wantErr: true,
		},
		{
			name:    "unexpected_char_after_valid_token",
			input:   "foo @bar",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got, err := Parse(tc.input)

			if tc.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Len(t, got, len(tc.tokens), "token count mismatch")
			for i, want := range tc.tokens {
				assert.Equal(t, want.typ, got[i].Type, "token[%d] type", i)
				assert.Equal(t, want.lexeme, got[i].Lexeme, "token[%d] lexeme", i)
			}
		})
	}
}
