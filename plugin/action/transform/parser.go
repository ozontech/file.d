package transform

import (
	"fmt"
	"strconv"
)

type ParseError struct {
	Pos     Position
	Message string
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("parse error at %s: %s", e.Pos, e.Message)
}

// Parser builds an AST from a slice of tokens.
type Parser struct {
	tokens []Token
	pos    int
}

func NewParser(tokens []Token) *Parser {
	filtered := make([]Token, 0, len(tokens))
	for _, t := range tokens {
		if t.Type != WHITESPACE && t.Type != COMMENT {
			filtered = append(filtered, t)
		}
	}
	return &Parser{tokens: filtered}
}

func (p *Parser) Parse() ([]Expr, error) {
	var exprs []Expr

	for !p.atEnd() {
		for p.match(SEMICOLON) {
		}
		if p.atEnd() {
			break
		}

		expr, err := p.parseExpr(bpLowest)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
	}

	return exprs, nil
}

// Returns the current token without advancing.
// Returns the EOF token when the stream is finished.
func (p *Parser) peek() Token {
	if p.pos >= len(p.tokens) {
		return Token{Type: EOF}
	}
	return p.tokens[p.pos]
}

// Returns the token at pos+offset without advancing.
// Returns the EOF token when out of bounds.
func (p *Parser) peekAt(offset int) Token {
	idx := p.pos + offset
	if idx >= len(p.tokens) {
		return Token{Type: EOF}
	}
	return p.tokens[idx]
}

// Returns the current token and moves the position forward.
func (p *Parser) advance() Token {
	tok := p.peek()
	if !p.atEnd() {
		p.pos++
	}
	return tok
}

// Consumes the current token if it matches typ, or returns an error.
func (p *Parser) expect(typ TokenType) (Token, error) {
	tok := p.peek()
	if tok.Type != typ {
		return tok, &ParseError{
			Pos: tok.Pos(),
			Message: fmt.Sprintf("expected %s, got %s (%q)",
				TokenNames[typ], tok.Name(), tok.Lexeme),
		}
	}
	return p.advance(), nil
}

// Consumes the current token if it matches typ; returns true on success.
func (p *Parser) match(typ TokenType) bool {
	if p.peek().Type == typ {
		p.pos++
		return true
	}
	return false
}

func (p *Parser) check(typ TokenType) bool {
	return p.peek().Type == typ
}

func (p *Parser) atEnd() bool {
	return p.pos >= len(p.tokens)
}

func (p *Parser) errorf(tok Token, format string, args ...any) *ParseError {
	return &ParseError{
		Pos:     tok.Pos(),
		Message: fmt.Sprintf(format, args...),
	}
}

// parseExpr main function of the Pratt parser.
//
// minBP is the minimum binding power that an infix operator must exceed
// in order to be consumed. Calling with bpLowest parses a full expression.
//
//   - Left-associative:  infix calls parseExpr(bp(op))   - same BP blocks re-entry
//   - Right-associative: infix calls parseExpr(bp(op)-1) - same BP is allowed on the right
func (p *Parser) parseExpr(minBP int) (Expr, error) {
	// parse the left operand via a prefix handler
	left, err := p.parsePrefix()
	if err != nil {
		return nil, err
	}

	// consume infix operators while they are stronger than the threshold
	for {
		next := p.peek()
		if next.Type.BindingPower() <= minBP {
			break
		}
		op := p.advance()
		left, err = p.parseInfix(left, op)
		if err != nil {
			return nil, err
		}
	}

	return left, nil
}

// Called when a token appears at the start of an expression.
func (p *Parser) parsePrefix() (Expr, error) {
	tok := p.peek()

	switch tok.Type {

	// Literals
	case INTEGER:
		return p.parseIntLit()
	case FLOAT:
		return p.parseFloatLit()
	case STRING, STRING_RAW:
		return p.parseStringLit()
	case KW_TRUE:
		return &BoolLit{node: nodeAt(p.advance()), Value: true}, nil
	case KW_FALSE:
		return &BoolLit{node: nodeAt(p.advance()), Value: false}, nil
	case KW_NULL:
		return &NullLit{node: nodeAt(p.advance())}, nil
	case KW_DEL:
		return p.parseDel()
	case REGEX_LIT:
		t := p.advance()
		return &RegexLit{node: nodeAt(t), Pattern: unwrap(t.Lexeme, 2)}, nil
	case TIMESTAMP_LIT:
		t := p.advance()
		return &TimestampLit{node: nodeAt(t), Value: unwrap(t.Lexeme, 2)}, nil

	// Identifier - variable or function call
	case ID:
		t := p.advance()
		return &IdentExpr{node: nodeAt(t), Name: t.Lexeme}, nil

	// Paths
	case DOT:
		return p.parseEventPath()
	case PERCENT:
		return p.parseMetadataPath()

	// Unary operators
	case BANG, MINUS:
		return p.parseUnary()

	// Grouped expression
	case LPAREN:
		return p.parseGrouped()

	// Collection literals
	case LBRACKET:
		return p.parseArray()
	case LBRACE:
		return p.parseObject()

	// Control flow
	case KW_IF:
		return p.parseIf()
	case KW_ABORT:
		return &AbortExpr{node: nodeAt(p.advance())}, nil
	case KW_FOR:
		return p.parseFor()
	}

	return nil, p.errorf(tok, "unexpected token %s (%q)", tok.Name(), tok.Lexeme)
}

// Called when a token appears between two expressions.
func (p *Parser) parseInfix(left Expr, op Token) (Expr, error) {
	switch op.Type {

	case ASSIGN:
		if !isLValue(left) {
			return nil, p.errorf(op, "left side of assignment must be a variable, path, or index expression")
		}
		// right-associative: bp-1 allows chaining a = b = c -> a = (b = c)
		right, err := p.parseExpr(bpAssign - 1)
		if err != nil {
			return nil, err
		}
		return &AssignExpr{
			node:   node{pos: left.Pos()},
			Target: left,
			Value:  right,
		}, nil
	case OR, AND,
		EQ, NEQ,
		LT, LTE, GT, GTE,
		PLUS, MINUS,
		STAR, SLASH, PERCENT:
		right, err := p.parseExpr(op.Type.BindingPower())
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{
			node:  node{pos: left.Pos()},
			Left:  left,
			Op:    op.Lexeme,
			Right: right,
		}, nil

	// function call
	case LPAREN:
		ident, ok := left.(*IdentExpr)
		if !ok {
			return nil, p.errorf(op, "function call requires an identifier on the left, got %T", left)
		}
		args, err := p.parseArgList()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(RPAREN); err != nil {
			return nil, err
		}
		return &CallExpr{node: ident.node, Name: ident.Name, Args: args}, nil

	// index access
	// path indexing (.field[0]) is handled inside parseEventPath.
	case LBRACKET:
		index, err := p.parseExpr(bpLowest)
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(RBRACKET); err != nil {
			return nil, err
		}
		return &IndexExpr{
			node:   node{pos: left.Pos()},
			Object: left,
			Index:  index,
		}, nil
	}

	return nil, p.errorf(op, "unknown infix operator %q", op.Lexeme)
}

func (p *Parser) parseIntLit() (Expr, error) {
	tok := p.advance()
	v, err := strconv.ParseInt(tok.Lexeme, 10, 64)
	if err != nil {
		return nil, p.errorf(tok, "invalid integer literal %q", tok.Lexeme)
	}
	return &IntLit{node: nodeAt(tok), Value: v}, nil
}

func (p *Parser) parseFloatLit() (Expr, error) {
	tok := p.advance()
	v, err := strconv.ParseFloat(tok.Lexeme, 64)
	if err != nil {
		return nil, p.errorf(tok, "invalid float literal %q", tok.Lexeme)
	}
	return &FloatLit{node: nodeAt(tok), Value: v}, nil
}

func (p *Parser) parseStringLit() (Expr, error) {
	tok := p.advance()
	switch tok.Type {
	case STRING:
		// process escape sequences.
		v, err := strconv.Unquote(tok.Lexeme)
		if err != nil {
			return nil, p.errorf(tok, "invalid string literal: %v", err)
		}
		return &StringLit{node: nodeAt(tok), Value: v}, nil

	case STRING_RAW:
		return &StringLit{node: nodeAt(tok), Value: unwrap(tok.Lexeme, 2)}, nil
	}
	return nil, p.errorf(tok, "expected string, got %s", tok.Name())
}

func (p *Parser) parseUnary() (Expr, error) {
	op := p.advance()
	operand, err := p.parseExpr(bpUnary)
	if err != nil {
		return nil, err
	}
	return &UnaryExpr{node: nodeAt(op), Op: op.Lexeme, Operand: operand}, nil
}

func (p *Parser) parseGrouped() (Expr, error) {
	// consume (
	p.advance()

	expr, err := p.parseExpr(bpLowest)
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(RPAREN); err != nil {
		return nil, err
	}
	return expr, nil
}

func (p *Parser) parseArray() (Expr, error) {
	// consume [
	start := p.advance()

	var elements []Expr
	for !p.check(RBRACKET) && !p.atEnd() {
		el, err := p.parseExpr(bpLowest)
		if err != nil {
			return nil, err
		}
		elements = append(elements, el)
		if !p.match(COMMA) {
			break
		}
	}

	if _, err := p.expect(RBRACKET); err != nil {
		return nil, err
	}
	return &ArrayExpr{node: nodeAt(start), Elements: elements}, nil
}

func (p *Parser) parseObject() (Expr, error) {
	// consume {
	start := p.advance()

	var pairs []KVPair
	for !p.check(RBRACE) && !p.atEnd() {
		kv, err := p.parseKVPair()
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, kv)
		if !p.match(COMMA) {
			break
		}
	}

	if _, err := p.expect(RBRACE); err != nil {
		return nil, err
	}
	return &ObjectExpr{node: nodeAt(start), Pairs: pairs}, nil
}

func (p *Parser) parseKVPair() (KVPair, error) {
	tok := p.peek()

	var key string
	switch tok.Type {
	case STRING:
		t := p.advance()
		v, err := strconv.Unquote(t.Lexeme)
		if err != nil {
			return KVPair{}, p.errorf(t, "invalid object key: %v", err)
		}
		key = v
	case STRING_RAW:
		t := p.advance()
		key = unwrap(t.Lexeme, 2)
	case ID:
		key = p.advance().Lexeme
	default:
		return KVPair{}, p.errorf(tok, "object key must be a string or identifier, got %s", tok.Name())
	}

	if _, err := p.expect(COLON); err != nil {
		return KVPair{}, err
	}

	val, err := p.parseExpr(bpLowest)
	if err != nil {
		return KVPair{}, err
	}

	return KVPair{Key: key, Value: val}, nil
}

func (p *Parser) parseEventPath() (Expr, error) {
	// consume .
	start := p.advance()

	var segments []PathSegment
	if seg, ok, err := p.tryFieldSegment(); err != nil {
		return nil, err
	} else if ok {
		segments = append(segments, seg)
		var err error
		segments, err = p.continueSegments(segments)
		if err != nil {
			return nil, err
		}
	}

	return &PathExpr{node: nodeAt(start), Root: EventRoot, Segments: segments}, nil
}

func (p *Parser) parseMetadataPath() (Expr, error) {
	// consume %
	start := p.advance()

	tok := p.peek()
	if tok.Type != ID {
		return nil, p.errorf(tok, "expected metadata field name after %%, got %s", tok.Name())
	}

	segments := []PathSegment{{Field: p.advance().Lexeme}}

	var err error
	segments, err = p.continueSegments(segments)
	if err != nil {
		return nil, err
	}

	return &PathExpr{node: nodeAt(start), Root: MetadataRoot, Segments: segments}, nil
}

// Attempts to read a named path segment.
func (p *Parser) tryFieldSegment() (PathSegment, bool, error) {
	switch p.peek().Type {
	case ID:
		return PathSegment{Field: p.advance().Lexeme}, true, nil
	case STRING:
		t := p.advance()
		v, err := strconv.Unquote(t.Lexeme)
		if err != nil {
			return PathSegment{}, false, p.errorf(t, "invalid field name: %v", err)
		}
		return PathSegment{Field: v}, true, nil
	case STRING_RAW:
		t := p.advance()
		return PathSegment{Field: unwrap(t.Lexeme, 2)}, true, nil
	}
	return PathSegment{}, false, nil
}

// Greedily consumes path continuations: .field and [index].
func (p *Parser) continueSegments(segments []PathSegment) ([]PathSegment, error) {
	for {
		switch p.peek().Type {
		case DOT:
			if p.pos-1 >= 0 && p.pos-1 < len(p.tokens) {
				dot := p.peek()
				prev := p.tokens[p.pos-1]
				if dot.StartLine > prev.EndLine {
					return segments, nil
				}
			}

			p.advance()
			seg, ok, err := p.tryFieldSegment()
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, p.errorf(p.peek(), "expected field name after '.', got %s", p.peek().Name())
			}
			segments = append(segments, seg)

		case LBRACKET:
			p.advance()
			index, err := p.parseExpr(bpLowest)
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(RBRACKET); err != nil {
				return nil, err
			}
			segments = append(segments, PathSegment{Index: index})

		default:
			return segments, nil
		}
	}
}

// Parses If expressions (e.g. if condition { ... } else { ... })
func (p *Parser) parseIf() (Expr, error) {
	// consume if
	start := p.advance()

	condition, err := p.parseExpr(bpLowest)
	if err != nil {
		return nil, err
	}

	then, err := p.parseBlock()
	if err != nil {
		return nil, err
	}

	var elseBranch []Expr
	if p.match(KW_ELSE) {
		if p.check(KW_IF) {
			elseIf, err := p.parseIf()
			if err != nil {
				return nil, err
			}
			elseBranch = []Expr{elseIf}
		} else {
			elseBranch, err = p.parseBlock()
			if err != nil {
				return nil, err
			}
		}
	}

	return &IfExpr{
		node:      nodeAt(start),
		Condition: condition,
		Then:      then,
		Else:      elseBranch,
	}, nil
}

// Parses If block (e.g. { expr; expr; ... })
// Semicolons between expressions are optional.
func (p *Parser) parseBlock() ([]Expr, error) {
	if _, err := p.expect(LBRACE); err != nil {
		return nil, err
	}

	var exprs []Expr
	for !p.check(RBRACE) && !p.atEnd() {
		e, err := p.parseExpr(bpLowest)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, e)
		for p.match(SEMICOLON) {
		}
	}

	if _, err := p.expect(RBRACE); err != nil {
		return nil, err
	}
	return exprs, nil
}

func (p *Parser) parseArgList() ([]Argument, error) {
	var args []Argument
	for !p.check(RPAREN) && !p.atEnd() {
		arg, err := p.parseArgument()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
		if !p.match(COMMA) {
			break
		}
	}
	return args, nil
}

// Parses function arguments: named (key: expr) or positional (expr).
func (p *Parser) parseArgument() (Argument, error) {
	if p.peek().Type == ID && p.peekAt(1).Type == COLON {
		name := p.advance().Lexeme
		p.advance()
		val, err := p.parseExpr(bpLowest)
		if err != nil {
			return Argument{}, err
		}
		return Argument{Name: name, Value: val}, nil
	}

	val, err := p.parseExpr(bpLowest)
	if err != nil {
		return Argument{}, err
	}
	return Argument{Value: val}, nil
}

// Parses delete expressions (e.g. del .field | del .field.nested[0] | del %meta.key)
//
// Only PathExpr is a valid target - anything else is a compile-time error.
func (p *Parser) parseDel() (Expr, error) {
	start := p.advance()

	tok := p.peek()

	var pathExpr *PathExpr

	switch tok.Type {
	case DOT:
		raw, err := p.parseEventPath()
		if err != nil {
			return nil, err
		}
		pathExpr = raw.(*PathExpr)

	case PERCENT:
		raw, err := p.parseMetadataPath()
		if err != nil {
			return nil, err
		}
		pathExpr = raw.(*PathExpr)

	default:
		return nil, p.errorf(tok, "del requires a path (.field or %%field), got %s", tok.Name())
	}

	return &DelExpr{node: nodeAt(start), Target: pathExpr}, nil
}

// Parses for expressions (e.g. for i in expr { ... } | for i, item in expr { ... })
func (p *Parser) parseFor() (Expr, error) {
	start := p.advance()

	first, err := p.expect(ID)
	if err != nil {
		return nil, err
	}

	var indexName, itemName string

	if p.match(COMMA) {
		second, err := p.expect(ID)
		if err != nil {
			return nil, err
		}

		if first.Lexeme != "_" {
			indexName = first.Lexeme
		}
		if second.Lexeme != "_" {
			itemName = second.Lexeme
		}
	} else {
		indexName = first.Lexeme
	}

	if indexName == "" && itemName == "" {
		return nil, p.errorf(first, "for loop must bind at least one variable: : use 'for i in ...' or 'for i, item in ...'")
	}

	if _, err := p.expect(KW_IN); err != nil {
		return nil, err
	}

	iter, err := p.parseExpr(bpLowest)
	if err != nil {
		return nil, err
	}

	body, err := p.parseBlock()
	if err != nil {
		return nil, err
	}

	return &ForExpr{
		node:  nodeAt(start),
		Index: indexName,
		Item:  itemName,
		Iter:  iter,
		Body:  body,
	}, nil
}

// isLValue reports whether expr is a valid assignment target.
func isLValue(expr Expr) bool {
	switch expr.(type) {
	case *IdentExpr:
		return true
	case *PathExpr:
		return true
	case *IndexExpr:
		return true
	}
	return false
}

func nodeAt(tok Token) node {
	return node{pos: tok.Pos()}
}

// Strips prefixLen bytes from the front and 1 byte from the end.
func unwrap(s string, prefixLen int) string {
	if len(s) <= prefixLen+1 {
		return ""
	}
	return s[prefixLen : len(s)-1]
}
