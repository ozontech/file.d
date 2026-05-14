package parser

import (
	"github.com/timtadh/lexmachine"
	"github.com/timtadh/lexmachine/machines"
)

var (
	globalLexer = NewLexer()
)

func NewLexer() *lexmachine.Lexer {
	l := lexmachine.NewLexer()

	token := func(typ TokenType) lexmachine.Action {
		return func(_ *lexmachine.Scanner, m *machines.Match) (any, error) {
			return NewToken(typ, m), nil
		}
	}

	skip := func(_ *lexmachine.Scanner, _ *machines.Match) (any, error) {
		return nil, nil
	}

	// whitespaces
	l.Add([]byte(`[ \t\r\n]+`), skip)
	// comments
	l.Add([]byte(`#[^\n]*`), skip)

	// literals
	// r'\d+' - regex
	l.Add([]byte(`r'([^'\\]|\\.)*'`), token(LIT_REGEX))
	// t'2024-01-01T00:00:00Z' - timestamp
	l.Add([]byte(`t'[^']*'`), token(LIT_TIMESTAMP))
	// s'C:\new\folder' - raw string
	l.Add([]byte(`s'([^'\\]|\\.)*'`), token(LIT_STRING_RAW))

	// keywords
	l.Add([]byte(`if`), token(KW_IF))
	l.Add([]byte(`else`), token(KW_ELSE))
	l.Add([]byte(`true`), token(KW_TRUE))
	l.Add([]byte(`false`), token(KW_FALSE))
	l.Add([]byte(`null`), token(KW_NULL))
	l.Add([]byte(`abort`), token(KW_ABORT))
	l.Add([]byte(`del`), token(KW_DEL))
	l.Add([]byte(`for`), token(KW_FOR))
	l.Add([]byte(`in`), token(KW_IN))

	// identificators
	l.Add([]byte(`[a-zA-Z_][a-zA-Z0-9_]*`), token(IDENT))

	// numeric literals
	// format: 3.14 | 1.5e10 | 1.5e+10 | 1.5e-10 | 1e10 | 1e+10 | 1e-10
	l.Add([]byte(`[0-9]+(\.[0-9]+([eE][+-]?[0-9]+)?|[eE][+-]?[0-9]+)`), token(LIT_FLOAT))
	// integers
	l.Add([]byte(`[0-9]+`), token(LIT_INTEGER))
	// string literals
	l.Add([]byte(`"([^"\\]|\\.)*"`), token(LIT_STRING))

	// operators
	l.Add([]byte(`&&`), token(OP_AND))
	l.Add([]byte(`\|\|`), token(OP_OR))
	l.Add([]byte(`==`), token(OP_EQ))
	l.Add([]byte(`!=`), token(OP_NEQ))
	l.Add([]byte(`<=`), token(OP_LTE))
	l.Add([]byte(`>=`), token(OP_GTE))
	l.Add([]byte(`=`), token(OP_ASSIGN))
	l.Add([]byte(`<`), token(OP_LT))
	l.Add([]byte(`>`), token(OP_GT))

	l.Add([]byte(`\+`), token(PLUS))
	l.Add([]byte(`-`), token(MINUS))
	l.Add([]byte(`\*`), token(STAR))
	l.Add([]byte(`/`), token(SLASH))
	l.Add([]byte(`%`), token(PERCENT))
	l.Add([]byte(`!`), token(BANG))

	l.Add([]byte(`\(`), token(LPAREN))
	l.Add([]byte(`\)`), token(RPAREN))
	l.Add([]byte(`\{`), token(LBRACE))
	l.Add([]byte(`\}`), token(RBRACE))
	l.Add([]byte(`\[`), token(LBRACKET))
	l.Add([]byte(`\]`), token(RBRACKET))
	l.Add([]byte(`,`), token(COMMA))
	l.Add([]byte(`:`), token(COLON))
	l.Add([]byte(`;`), token(SEMICOLON))
	l.Add([]byte(`\.`), token(DOT))

	_ = l.Compile()

	return l
}
