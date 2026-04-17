package transform

import (
	"github.com/timtadh/lexmachine/machines"
)

type TokenType int

const (
	EOF        TokenType = -1
	WHITESPACE TokenType = iota
	COMMENT

	KW_IF
	KW_ELSE
	KW_TRUE
	KW_FALSE
	KW_NULL
	KW_ABORT
	KW_DEL

	ID

	FLOAT
	INTEGER

	STRING     // "double quoted"
	STRING_RAW // s'\n\n'

	REGEX_LIT     // r'\d+'
	TIMESTAMP_LIT // t'2024-01-01T00:00:00Z'

	AND // &&
	OR  // ||
	EQ  // ==
	NEQ // !=
	LTE // <=
	GTE // >=

	ASSIGN  // =
	PLUS    // +
	MINUS   // -
	STAR    // *
	SLASH   // /
	PERCENT // %
	LT      // <
	GT      // >
	BANG    // !

	LPAREN    // (
	RPAREN    // )
	LBRACE    // {
	RBRACE    // }
	LBRACKET  // [
	RBRACKET  // ]
	COMMA     // ,
	COLON     // :
	SEMICOLON // ;
	DOT       // .
)

var TokenNames = map[TokenType]string{
	EOF:           "EOF",
	WHITESPACE:    "WHITESPACE",
	COMMENT:       "COMMENT",
	KW_IF:         "KW_IF",
	KW_ELSE:       "KW_ELSE",
	KW_TRUE:       "KW_TRUE",
	KW_FALSE:      "KW_FALSE",
	KW_NULL:       "KW_NULL",
	KW_ABORT:      "KW_ABORT",
	KW_DEL:        "KW_DEL",
	ID:            "ID",
	FLOAT:         "FLOAT",
	INTEGER:       "INTEGER",
	STRING:        "STRING",
	STRING_RAW:    "STRING_RAW",
	REGEX_LIT:     "REGEX_LIT",
	TIMESTAMP_LIT: "TIMESTAMP_LIT",
	AND:           "AND",
	OR:            "OR",
	EQ:            "EQ",
	NEQ:           "NEQ",
	LTE:           "LTE",
	GTE:           "GTE",
	ASSIGN:        "ASSIGN",
	PLUS:          "PLUS",
	MINUS:         "MINUS",
	STAR:          "STAR",
	SLASH:         "SLASH",
	PERCENT:       "PERCENT",
	LT:            "LT",
	GT:            "GT",
	BANG:          "BANG",
	LPAREN:        "LPAREN",
	RPAREN:        "RPAREN",
	LBRACE:        "LBRACE",
	RBRACE:        "RBRACE",
	LBRACKET:      "LBRACKET",
	RBRACKET:      "RBRACKET",
	COMMA:         "COMMA",
	COLON:         "COLON",
	SEMICOLON:     "SEMICOLON",
	DOT:           "DOT",
}

// Binding power is the "gravitational pull" of an infix operator.
// The higher the value, the more tightly the operator binds its operands.
const (
	bpLowest  = iota // 0 — expression terminator
	bpAssign         // 1 — =
	bpOr             // 2 — ||
	bpAnd            // 3 — &&
	bpEqual          // 4 — == !=
	bpCompare        // 5 — < <= > >=
	bpAdd            // 6 — + -
	bpMul            // 7 — * / %
	bpUnary          // 8 — prefix ! and - (not in BindingPower, used directly)
	bpCall           // 9 — fn()  expr[]
)

func (t TokenType) BindingPower() int {
	switch t {
	case ASSIGN:
		return bpAssign
	case OR:
		return bpOr
	case AND:
		return bpAnd
	case EQ, NEQ:
		return bpEqual
	case LT, LTE, GT, GTE:
		return bpCompare
	case PLUS, MINUS:
		return bpAdd
	case STAR, SLASH, PERCENT:
		return bpMul
	case LPAREN: // fn(args)
		return bpCall
	case LBRACKET: // expr[index]
		return bpCall
	}
	return bpLowest
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

func (t Token) Pos() Position {
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
