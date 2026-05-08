package compiler

import (
	"fmt"
	"strconv"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/ozontech/file.d/plugin/action/transform/parser"
)

type ParseError struct {
	Pos     parser.Position
	Message string
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("parse error at (%s): %s", e.Pos, e.Message)
}

// Compiler builds an AST from a slice of tokens.
type Compiler struct {
	tokens []parser.Token
	pos    int
}

func NewCompiler(tokens []parser.Token) *Compiler {
	filtered := make([]parser.Token, 0, len(tokens))
	for _, t := range tokens {
		if t.Type != parser.WHITESPACE && t.Type != parser.COMMENT {
			filtered = append(filtered, t)
		}
	}
	return &Compiler{tokens: filtered}
}

func (p *Compiler) Compile() ([]core.Expr, error) {
	var exprs []core.Expr

	for !p.atEnd() {
		for p.match(parser.SEMICOLON) {
		}
		if p.atEnd() {
			break
		}

		expr, err := p.parseExpr(parser.BpLowest)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
	}

	return exprs, nil
}

// Returns the current token without advancing.
// Returns the EOF token when the stream is finished.
func (p *Compiler) peek() parser.Token {
	if p.pos >= len(p.tokens) {
		return parser.Token{Type: parser.EOF}
	}
	return p.tokens[p.pos]
}

// Returns the token at pos+offset without advancing.
// Returns the EOF token when out of bounds.
func (p *Compiler) peekAt(offset int) parser.Token {
	idx := p.pos + offset
	if idx >= len(p.tokens) {
		return parser.Token{Type: parser.EOF}
	}
	return p.tokens[idx]
}

// Returns the current token and moves the position forward.
func (p *Compiler) advance() parser.Token {
	tok := p.peek()
	if !p.atEnd() {
		p.pos++
	}
	return tok
}

// Consumes the current token if it matches typ, or returns an error.
func (p *Compiler) expect(typ parser.TokenType) (parser.Token, error) {
	tok := p.peek()
	if tok.Type != typ {
		return tok, &ParseError{
			Pos: tok.StartPos(),
			Message: fmt.Sprintf("expected %s, got %s (%q)",
				parser.TokenNames[typ], tok.Name(), tok.Lexeme),
		}
	}
	return p.advance(), nil
}

// Consumes the current token if it matches typ; returns true on success.
func (p *Compiler) match(typ parser.TokenType) bool {
	if p.peek().Type == typ {
		p.pos++
		return true
	}
	return false
}

func (p *Compiler) check(typ parser.TokenType) bool {
	return p.peek().Type == typ
}

func (p *Compiler) atEnd() bool {
	return p.pos >= len(p.tokens)
}

func (p *Compiler) errorf(tok parser.Token, format string, args ...any) *ParseError {
	return &ParseError{
		Pos:     tok.StartPos(),
		Message: fmt.Sprintf(format, args...),
	}
}

// parseExpr main function of the Pratt parser.
//
// minBP is the minimum binding power that an infix operator must exceed
// in order to be consumed. Calling with parser.BpLowest parses a full expression.
//
//   - Left-associative:  infix calls parseExpr(bp(op))   - same BP blocks re-entry
//   - Right-associative: infix calls parseExpr(bp(op)-1) - same BP is allowed on the right
func (p *Compiler) parseExpr(minBP int) (core.Expr, error) {
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
func (p *Compiler) parsePrefix() (core.Expr, error) {
	tok := p.peek()

	switch tok.Type {

	// Literals
	case parser.LIT_INTEGER:
		return p.parseIntLit()
	case parser.LIT_FLOAT:
		return p.parseFloatLit()
	case parser.LIT_STRING, parser.LIT_STRING_RAW:
		return p.parseStringLit()
	case parser.KW_TRUE:
		return &core.BoolLit{Node: nodeAt(p.advance()), Value: true}, nil
	case parser.KW_FALSE:
		return &core.BoolLit{Node: nodeAt(p.advance()), Value: false}, nil
	case parser.KW_NULL:
		return &core.NullLit{Node: nodeAt(p.advance())}, nil
	case parser.KW_DEL:
		return p.parseDel()
	case parser.LIT_REGEX:
		t := p.advance()
		return &core.RegexLit{Node: nodeAt(t), Pattern: unwrap(t.Lexeme, 2)}, nil
	case parser.LIT_TIMESTAMP:
		t := p.advance()
		return &core.TimestampLit{Node: nodeAt(t), Value: unwrap(t.Lexeme, 2)}, nil

	// Identifier - variable or function call
	case parser.IDENT:
		t := p.advance()
		return &core.IdentExpr{Node: nodeAt(t), Name: t.Lexeme}, nil

	// Paths
	case parser.DOT:
		return p.parseEventPath()
	case parser.PERCENT:
		return p.parseMetadataPath()

	// Unary operators
	case parser.BANG, parser.MINUS:
		return p.parseUnary()

	// Grouped expression
	case parser.LPAREN:
		return p.parseGrouped()

	// Collection literals
	case parser.LBRACKET:
		return p.parseArray()
	case parser.LBRACE:
		return p.parseObject()

	// Control flow
	case parser.KW_IF:
		return p.parseIf()
	case parser.KW_ABORT:
		return &core.AbortExpr{Node: nodeAt(p.advance())}, nil
	case parser.KW_FOR:
		return p.parseFor()
	}

	return nil, p.errorf(tok, "unexpected token %s (%q)", tok.Name(), tok.Lexeme)
}

// Called when a token appears between two expressions.
func (p *Compiler) parseInfix(left core.Expr, op parser.Token) (core.Expr, error) {
	switch op.Type {

	case parser.OP_ASSIGN:
		if !isLValue(left) {
			return nil, p.errorf(op, "left side of assignment must be a variable, path, or index expression")
		}
		// right-associative: bp-1 allows chaining a = b = c -> a = (b = c)
		right, err := p.parseExpr(parser.BpAssign - 1)
		if err != nil {
			return nil, err
		}
		return &core.AssignExpr{
			Node:   core.NewNode(left.Pos()),
			Target: left,
			Value:  right,
		}, nil
	case parser.OP_OR, parser.OP_AND,
		parser.OP_EQ, parser.OP_NEQ,
		parser.OP_LT, parser.OP_LTE, parser.OP_GT, parser.OP_GTE,
		parser.PLUS, parser.MINUS,
		parser.STAR, parser.SLASH, parser.PERCENT:
		right, err := p.parseExpr(op.Type.BindingPower())
		if err != nil {
			return nil, err
		}
		return &core.BinaryExpr{
			Node:  core.NewNode(left.Pos()),
			Left:  left,
			Op:    op.Lexeme,
			Right: right,
		}, nil

	// function call
	case parser.LPAREN:
		ident, ok := left.(*core.IdentExpr)
		if !ok {
			return nil, p.errorf(op, "function call requires an identifier on the left, got %T", left)
		}
		args, err := p.parseArgList()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(parser.RPAREN); err != nil {
			return nil, err
		}
		return &core.CallExpr{Node: ident.Node, Name: ident.Name, Args: args}, nil

	// index access
	// path indexing (.field[0]) is handled inside parseEventPath.
	case parser.LBRACKET:
		index, err := p.parseExpr(parser.BpLowest)
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(parser.RBRACKET); err != nil {
			return nil, err
		}
		return &core.IndexExpr{
			Node:   core.NewNode(left.Pos()),
			Object: left,
			Index:  index,
		}, nil
	}

	return nil, p.errorf(op, "unknown infix operator %q", op.Lexeme)
}

func (p *Compiler) parseIntLit() (core.Expr, error) {
	tok := p.advance()
	v, err := strconv.ParseInt(tok.Lexeme, 10, 64)
	if err != nil {
		return nil, p.errorf(tok, "invalid integer literal %q", tok.Lexeme)
	}
	return &core.IntLit{Node: nodeAt(tok), Value: v}, nil
}

func (p *Compiler) parseFloatLit() (core.Expr, error) {
	tok := p.advance()
	v, err := strconv.ParseFloat(tok.Lexeme, 64)
	if err != nil {
		return nil, p.errorf(tok, "invalid float literal %q", tok.Lexeme)
	}
	return &core.FloatLit{Node: nodeAt(tok), Value: v}, nil
}

func (p *Compiler) parseStringLit() (core.Expr, error) {
	tok := p.advance()
	switch tok.Type {
	case parser.LIT_STRING:
		// process escape sequences.
		v, err := strconv.Unquote(tok.Lexeme)
		if err != nil {
			return nil, p.errorf(tok, "invalid string literal: %v", err)
		}
		return &core.StringLit{Node: nodeAt(tok), Value: v}, nil

	case parser.LIT_STRING_RAW:
		return &core.StringLit{Node: nodeAt(tok), Value: unwrap(tok.Lexeme, 2)}, nil
	}
	return nil, p.errorf(tok, "expected string, got %s", tok.Name())
}

func (p *Compiler) parseUnary() (core.Expr, error) {
	op := p.advance()
	operand, err := p.parseExpr(parser.BpUnary)
	if err != nil {
		return nil, err
	}
	return &core.UnaryExpr{Node: nodeAt(op), Op: op.Lexeme, Operand: operand}, nil
}

func (p *Compiler) parseGrouped() (core.Expr, error) {
	// consume (
	p.advance()

	expr, err := p.parseExpr(parser.BpLowest)
	if err != nil {
		return nil, err
	}
	if _, err := p.expect(parser.RPAREN); err != nil {
		return nil, err
	}
	return expr, nil
}

func (p *Compiler) parseArray() (core.Expr, error) {
	// consume [
	start := p.advance()

	var elements []core.Expr
	for !p.check(parser.RBRACKET) && !p.atEnd() {
		el, err := p.parseExpr(parser.BpLowest)
		if err != nil {
			return nil, err
		}
		elements = append(elements, el)
		if !p.match(parser.COMMA) {
			break
		}
	}

	if _, err := p.expect(parser.RBRACKET); err != nil {
		return nil, err
	}
	return &core.ArrayExpr{Node: nodeAt(start), Elements: elements}, nil
}

func (p *Compiler) parseObject() (core.Expr, error) {
	// consume {
	start := p.advance()

	var pairs []core.KVPair
	for !p.check(parser.RBRACE) && !p.atEnd() {
		kv, err := p.parseKVPair()
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, kv)
		if !p.match(parser.COMMA) {
			break
		}
	}

	if _, err := p.expect(parser.RBRACE); err != nil {
		return nil, err
	}
	return &core.ObjectExpr{Node: nodeAt(start), Pairs: pairs}, nil
}

func (p *Compiler) parseKVPair() (core.KVPair, error) {
	tok := p.peek()

	var key string
	switch tok.Type {
	case parser.LIT_STRING:
		t := p.advance()
		v, err := strconv.Unquote(t.Lexeme)
		if err != nil {
			return core.KVPair{}, p.errorf(t, "invalid object key: %v", err)
		}
		key = v
	case parser.LIT_STRING_RAW:
		t := p.advance()
		key = unwrap(t.Lexeme, 2)
	case parser.IDENT:
		key = p.advance().Lexeme
	default:
		return core.KVPair{}, p.errorf(tok, "object key must be a string or identifier, got %s", tok.Name())
	}

	if _, err := p.expect(parser.COLON); err != nil {
		return core.KVPair{}, err
	}

	val, err := p.parseExpr(parser.BpLowest)
	if err != nil {
		return core.KVPair{}, err
	}

	return core.KVPair{Key: key, Value: val}, nil
}

func (p *Compiler) parseEventPath() (core.Expr, error) {
	// consume .
	start := p.advance()

	var segments []core.PathSegment
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

	return &core.PathExpr{Node: nodeAt(start), Root: core.EventRoot, Segments: segments}, nil
}

func (p *Compiler) parseMetadataPath() (core.Expr, error) {
	// consume %
	start := p.advance()

	tok := p.peek()
	if tok.Type != parser.IDENT {
		return nil, p.errorf(tok, "expected metadata field name after %%, got %s", tok.Name())
	}

	segments := []core.PathSegment{{Field: p.advance().Lexeme}}

	var err error
	segments, err = p.continueSegments(segments)
	if err != nil {
		return nil, err
	}

	return &core.PathExpr{Node: nodeAt(start), Root: core.MetadataRoot, Segments: segments}, nil
}

// Attempts to read a named path segment.
func (p *Compiler) tryFieldSegment() (core.PathSegment, bool, error) {
	switch p.peek().Type {
	case parser.IDENT:
		return core.PathSegment{Field: p.advance().Lexeme}, true, nil
	case parser.LIT_STRING:
		t := p.advance()
		v, err := strconv.Unquote(t.Lexeme)
		if err != nil {
			return core.PathSegment{}, false, p.errorf(t, "invalid field name: %v", err)
		}
		return core.PathSegment{Field: v}, true, nil
	case parser.LIT_STRING_RAW:
		t := p.advance()
		return core.PathSegment{Field: unwrap(t.Lexeme, 2)}, true, nil
	}
	return core.PathSegment{}, false, nil
}

// Greedily consumes path continuations: .field and [index].
func (p *Compiler) continueSegments(segments []core.PathSegment) ([]core.PathSegment, error) {
	for {
		switch p.peek().Type {
		case parser.DOT:
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

		case parser.LBRACKET:
			p.advance()
			index, err := p.parseExpr(parser.BpLowest)
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(parser.RBRACKET); err != nil {
				return nil, err
			}
			segments = append(segments, core.PathSegment{Index: index})

		default:
			return segments, nil
		}
	}
}

// Parses If expressions (e.g. if condition { ... } else { ... })
func (p *Compiler) parseIf() (core.Expr, error) {
	// consume if
	start := p.advance()

	condition, err := p.parseExpr(parser.BpLowest)
	if err != nil {
		return nil, err
	}

	then, err := p.parseBlock()
	if err != nil {
		return nil, err
	}

	var elseBranch []core.Expr
	if p.match(parser.KW_ELSE) {
		if p.check(parser.KW_IF) {
			elseIf, err := p.parseIf()
			if err != nil {
				return nil, err
			}
			elseBranch = []core.Expr{elseIf}
		} else {
			elseBranch, err = p.parseBlock()
			if err != nil {
				return nil, err
			}
		}
	}

	return &core.IfExpr{
		Node:      nodeAt(start),
		Condition: condition,
		Then:      then,
		Else:      elseBranch,
	}, nil
}

// Parses If block (e.g. { expr; expr; ... })
// Semicolons between expressions are optional.
func (p *Compiler) parseBlock() ([]core.Expr, error) {
	if _, err := p.expect(parser.LBRACE); err != nil {
		return nil, err
	}

	var exprs []core.Expr
	for !p.check(parser.RBRACE) && !p.atEnd() {
		e, err := p.parseExpr(parser.BpLowest)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, e)
		for p.match(parser.SEMICOLON) {
		}
	}

	if _, err := p.expect(parser.RBRACE); err != nil {
		return nil, err
	}
	return exprs, nil
}

func (p *Compiler) parseArgList() ([]core.Argument, error) {
	var args []core.Argument
	for !p.check(parser.RPAREN) && !p.atEnd() {
		arg, err := p.parseArgument()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
		if !p.match(parser.COMMA) {
			break
		}
	}
	return args, nil
}

// Parses function arguments: named (key: expr) or positional (expr).
func (p *Compiler) parseArgument() (core.Argument, error) {
	if p.peek().Type == parser.IDENT && p.peekAt(1).Type == parser.COLON {
		name := p.advance().Lexeme
		p.advance()
		val, err := p.parseExpr(parser.BpLowest)
		if err != nil {
			return core.Argument{}, err
		}
		return core.Argument{Name: name, Value: val}, nil
	}

	val, err := p.parseExpr(parser.BpLowest)
	if err != nil {
		return core.Argument{}, err
	}
	return core.Argument{Value: val}, nil
}

// Parses delete expressions (e.g. del .field | del .field.nested[0] | del %meta.key)
//
// Only core.PathExpr is a valid target - anything else is a compile-time error.
func (p *Compiler) parseDel() (core.Expr, error) {
	start := p.advance()

	tok := p.peek()

	var pathExpr *core.PathExpr

	switch tok.Type {
	case parser.DOT:
		raw, err := p.parseEventPath()
		if err != nil {
			return nil, err
		}
		pathExpr = raw.(*core.PathExpr)

	case parser.PERCENT:
		raw, err := p.parseMetadataPath()
		if err != nil {
			return nil, err
		}
		pathExpr = raw.(*core.PathExpr)

	default:
		return nil, p.errorf(tok, "del requires a path (.field or %%field), got %s", tok.Name())
	}

	return &core.DelExpr{Node: nodeAt(start), Target: pathExpr}, nil
}

// Parses for expressions (e.g. for i in expr { ... } | for i, item in expr { ... })
func (p *Compiler) parseFor() (core.Expr, error) {
	start := p.advance()

	first, err := p.expect(parser.IDENT)
	if err != nil {
		return nil, err
	}

	var indexName, itemName string

	if p.match(parser.COMMA) {
		second, err := p.expect(parser.IDENT)
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

	if _, err := p.expect(parser.KW_IN); err != nil {
		return nil, err
	}

	iter, err := p.parseExpr(parser.BpLowest)
	if err != nil {
		return nil, err
	}

	body, err := p.parseBlock()
	if err != nil {
		return nil, err
	}

	return &core.ForExpr{
		Node:  nodeAt(start),
		Index: indexName,
		Item:  itemName,
		Iter:  iter,
		Body:  body,
	}, nil
}

// isLValue reports whether expr is a valid assignment target.
func isLValue(expr core.Expr) bool {
	switch expr.(type) {
	case *core.IdentExpr:
		return true
	case *core.PathExpr:
		return true
	case *core.IndexExpr:
		return true
	}
	return false
}

func nodeAt(tok parser.Token) core.Node {
	return core.NewNode(tok.StartPos())
}

// Strips prefixLen bytes from the front and 1 byte from the end.
func unwrap(s string, prefixLen int) string {
	if len(s) <= prefixLen+1 {
		return ""
	}
	return s[prefixLen : len(s)-1]
}
