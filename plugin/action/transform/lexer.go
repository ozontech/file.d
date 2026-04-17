package transform

import (
	"fmt"

	"github.com/timtadh/lexmachine"
	"github.com/timtadh/lexmachine/machines"
)

type Lexer struct {
	lexer *lexmachine.Lexer
}

func NewLexer() (*Lexer, error) {
	l := lexmachine.NewLexer()

	token := func(typ TokenType) lexmachine.Action {
		return func(s *lexmachine.Scanner, m *machines.Match) (interface{}, error) {
			return NewToken(typ, m), nil
		}
	}

	skip := func(_ *lexmachine.Scanner, _ *machines.Match) (interface{}, error) {
		return nil, nil
	}

	// whitespaces
	l.Add([]byte(`[ \t\r\n]+`), skip)
	// comments
	l.Add([]byte(`#[^\n]*`), skip)

	// literals
	// r'\d+' - regex
	l.Add([]byte(`r'([^'\\]|\\.)*'`), token(REGEX_LIT))
	// t'2024-01-01T00:00:00Z' - timestamp
	l.Add([]byte(`t'[^']*'`), token(TIMESTAMP_LIT))
	// s'C:\new\folder' - raw string
	l.Add([]byte(`s'([^'\\]|\\.)*'`), token(STRING_RAW))

	// keywords
	l.Add([]byte(`if`), token(KW_IF))
	l.Add([]byte(`else`), token(KW_ELSE))
	l.Add([]byte(`true`), token(KW_TRUE))
	l.Add([]byte(`false`), token(KW_FALSE))
	l.Add([]byte(`null`), token(KW_NULL))
	l.Add([]byte(`abort`), token(KW_ABORT))
	l.Add([]byte(`del`), token(KW_DEL))

	// identificators
	l.Add([]byte(`[a-zA-Z_][a-zA-Z0-9_]*`), token(ID))

	// numeric literals
	// format: 3.14 | 1.5e10 | 1.5e+10 | 1.5e-10 | 1e10 | 1e+10 | 1e-10
	l.Add([]byte(`[0-9]+(\.[0-9]+([eE][+-]?[0-9]+)?|[eE][+-]?[0-9]+)`), token(FLOAT))
	// integers
	l.Add([]byte(`[0-9]+`), token(INTEGER))

	// string literals
	l.Add([]byte(`"([^"\\]|\\.)*"`), token(STRING))

	// operators
	l.Add([]byte(`&&`), token(AND))
	l.Add([]byte(`\|\|`), token(OR))
	l.Add([]byte(`==`), token(EQ))
	l.Add([]byte(`!=`), token(NEQ))
	l.Add([]byte(`<=`), token(LTE))
	l.Add([]byte(`>=`), token(GTE))

	l.Add([]byte(`=`), token(ASSIGN))
	l.Add([]byte(`\+`), token(PLUS))
	l.Add([]byte(`-`), token(MINUS))
	l.Add([]byte(`\*`), token(STAR))
	l.Add([]byte(`/`), token(SLASH))
	l.Add([]byte(`%`), token(PERCENT))
	l.Add([]byte(`<`), token(LT))
	l.Add([]byte(`>`), token(GT))
	l.Add([]byte(`!`), token(BANG))

	// separators and punctuation
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

	if err := l.Compile(); err != nil {
		return nil, fmt.Errorf("can't compile lexer: %w", err)
	}
	return &Lexer{lexer: l}, nil
}

func (v *Lexer) Tokenize(input string) ([]Token, error) {
	scanner, err := v.lexer.Scanner([]byte(input))
	if err != nil {
		return nil, fmt.Errorf("can't create scanner: %w", err)
	}

	var tokens []Token
	for {
		raw, err, eos := scanner.Next()

		if eos {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("unknown symbol: %w", err)
		}
		if raw == nil {
			continue
		}

		tokens = append(tokens, raw.(Token))
	}
	return tokens, nil
}
