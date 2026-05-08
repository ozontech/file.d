package parser

import (
	"fmt"

	"github.com/timtadh/lexmachine/machines"
)

type TokenType int

const (
	EOF TokenType = -1 + iota
	WHITESPACE
	COMMENT

	KW_IF
	KW_ELSE
	KW_TRUE
	KW_FALSE
	KW_NULL
	KW_ABORT
	KW_DEL
	KW_FOR
	KW_IN

	IDENT

	LIT_FLOAT
	LIT_INTEGER
	LIT_STRING     // "double quoted"
	LIT_STRING_RAW // s'\n\n'
	LIT_REGEX      // r'\d+'
	LIT_TIMESTAMP  // t'2024-01-01T00:00:00Z'

	OP_AND    // &&
	OP_OR     // ||
	OP_EQ     // ==
	OP_NEQ    // !=
	OP_LTE    // <=
	OP_GTE    // >=
	OP_ASSIGN // =
	OP_LT     // <
	OP_GT     // >

	LPAREN   // (
	RPAREN   // )
	LBRACE   // {
	RBRACE   // }
	LBRACKET // [
	RBRACKET // ]

	PLUS      // +
	MINUS     // -
	STAR      // *
	SLASH     // /
	PERCENT   // %
	BANG      // !
	COMMA     // ,
	COLON     // :
	SEMICOLON // ;
	DOT       // .
)

var TokenNames = map[TokenType]string{
	EOF:            "EOF",
	WHITESPACE:     "WHITESPACE",
	COMMENT:        "COMMENT",
	KW_IF:          "KW_IF",
	KW_ELSE:        "KW_ELSE",
	KW_TRUE:        "KW_TRUE",
	KW_FALSE:       "KW_FALSE",
	KW_NULL:        "KW_NULL",
	KW_ABORT:       "KW_ABORT",
	KW_DEL:         "KW_DEL",
	KW_FOR:         "KW_FOR",
	KW_IN:          "KW_IN",
	IDENT:          "IDENT",
	LIT_FLOAT:      "LIT_FLOAT",
	LIT_INTEGER:    "LIT_INTEGER",
	LIT_STRING:     "LIT_STRING",
	LIT_STRING_RAW: "LIT_STRING_RAW",
	LIT_REGEX:      "LIT_REGEX",
	LIT_TIMESTAMP:  "LIT_TIMESTAMP",
	OP_AND:         "OP_AND",
	OP_OR:          "OP_OR",
	OP_EQ:          "OP_EQ",
	OP_NEQ:         "OP_NEQ",
	OP_LTE:         "OP_LTE",
	OP_GTE:         "OP_GTE",
	OP_ASSIGN:      "OP_ASSIGN",
	PLUS:           "PLUS",
	MINUS:          "MINUS",
	STAR:           "STAR",
	SLASH:          "SLASH",
	PERCENT:        "PERCENT",
	OP_LT:          "OP_LT",
	OP_GT:          "OP_GT",
	BANG:           "BANG",
	LPAREN:         "LPAREN",
	RPAREN:         "RPAREN",
	LBRACE:         "LBRACE",
	RBRACE:         "RBRACE",
	LBRACKET:       "LBRACKET",
	RBRACKET:       "RBRACKET",
	COMMA:          "COMMA",
	COLON:          "COLON",
	SEMICOLON:      "SEMICOLON",
	DOT:            "DOT",
}

// Binding power is the "gravitational pull" of an infix operator.
// The higher the value, the more tightly the operator binds its operands.
const (
	BpLowest  = iota // 0 — expression terminator
	BpAssign         // 1 — =
	BpOr             // 2 — ||
	BpAnd            // 3 — &&
	BpEqual          // 4 — == !=
	BpCompare        // 5 — < <= > >=
	BpAdd            // 6 — + -
	BpMul            // 7 — * / %
	BpUnary          // 8 — prefix ! and - (not in BindingPower, used directly)
	BpCall           // 9 — fn()  expr[]
)

func (t TokenType) BindingPower() int {
	switch t {
	case OP_ASSIGN:
		return BpAssign
	case OP_OR:
		return BpOr
	case OP_AND:
		return BpAnd
	case OP_EQ, OP_NEQ:
		return BpEqual
	case OP_LT, OP_LTE, OP_GT, OP_GTE:
		return BpCompare
	case PLUS, MINUS:
		return BpAdd
	case STAR, SLASH, PERCENT:
		return BpMul
	case LPAREN, LBRACKET: // fn(args) or expr[index]
		return BpCall
	}
	return BpLowest
}

type Position struct {
	Line   int
	Column int
}

func (p Position) String() string {
	return fmt.Sprintf("%d:%d", p.Line, p.Column)
}

type Token struct {
	Type        TokenType
	Lexeme      string
	StartLine   int
	StartColumn int
	EndLine     int
	EndColumn   int
}

func (t Token) Name() string {
	return TokenNames[t.Type]
}

func (t Token) StartPos() Position {
	return Position{t.StartLine, t.StartColumn}
}

func NewToken(typ TokenType, m *machines.Match) Token {
	return Token{
		Type:        typ,
		Lexeme:      string(m.Bytes),
		StartLine:   m.StartLine,
		StartColumn: m.StartColumn,
		EndLine:     m.EndLine,
		EndColumn:   m.EndColumn,
	}
}
