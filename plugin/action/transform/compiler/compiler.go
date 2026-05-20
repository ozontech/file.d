package compiler

import (
	"fmt"
	"strconv"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/ozontech/file.d/plugin/action/transform/parser"
)

type compileError struct {
	Pos     parser.Position
	Message string
}

func (e *compileError) Error() string {
	return fmt.Sprintf("parse error at (%s): %s", e.Pos, e.Message)
}

// Compiler builds an AST from a slice of tokens.
type Compiler struct {
	tokens []parser.Token
	pos    int
}

func NewCompiler(source string) (*Compiler, error) {
	tokens, err := parser.Parse(source)
	if err != nil {
		return nil, err
	}
	return &Compiler{tokens: tokens}, nil
}

func (c *Compiler) Compile() ([]core.Expr, error) {
	var exprs []core.Expr

	for !c.atEnd() {
		for c.match(parser.SEMICOLON) {
		}
		if c.atEnd() {
			break
		}

		expr, err := c.parseExpr(parser.BpLowest)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, expr)
	}

	return exprs, nil
}

// Returns the current token without advancing.
// Returns the EOF token when the stream is finished.
func (c *Compiler) peek() parser.Token {
	if c.pos >= len(c.tokens) {
		return parser.Token{Type: parser.EOF}
	}
	return c.tokens[c.pos]
}

// Returns the token at pos+offset without advancing.
// Returns the EOF token when out of bounds.
func (c *Compiler) peekAt(offset int) parser.Token {
	idx := c.pos + offset
	if idx >= len(c.tokens) {
		return parser.Token{Type: parser.EOF}
	}
	return c.tokens[idx]
}

// Returns the current token and moves the position forward.
func (c *Compiler) advance() parser.Token {
	tok := c.peek()
	if !c.atEnd() {
		c.pos++
	}
	return tok
}

// Consumes the current token if it matches typ, or returns an error.
func (c *Compiler) expect(typ parser.TokenType) (parser.Token, error) {
	tok := c.peek()
	if tok.Type != typ {
		return tok, c.errorf(tok, "expected %s, got %s (%q)", typ, tok.Type, tok.Lexeme)
	}
	return c.advance(), nil
}

// Consumes the current token if it matches typ; returns true on success.
func (c *Compiler) match(typ parser.TokenType) bool {
	if c.peek().Type == typ {
		c.pos++
		return true
	}
	return false
}

func (c *Compiler) check(typ parser.TokenType) bool {
	return c.peek().Type == typ
}

func (c *Compiler) atEnd() bool {
	return c.pos >= len(c.tokens)
}

func (c *Compiler) errorf(tok parser.Token, format string, args ...any) *compileError {
	return &compileError{
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
func (c *Compiler) parseExpr(minBP int) (core.Expr, error) {
	// parse the left operand via a prefix handler
	left, err := c.parsePrefix()
	if err != nil {
		return nil, err
	}

	// consume infix operators while they are stronger than the threshold
	for {
		next := c.peek()
		if next.Type.BindingPower() <= minBP {
			break
		}
		op := c.advance()
		left, err = c.parseInfix(left, op)
		if err != nil {
			return nil, err
		}
	}

	return left, nil
}

// Called when a token appears at the start of an expression.
func (c *Compiler) parsePrefix() (core.Expr, error) {
	tok := c.peek()

	switch tok.Type {

	// Literals
	case parser.LIT_INTEGER:
		return c.parseIntLit()
	case parser.LIT_FLOAT:
		return c.parseFloatLit()
	case parser.LIT_STRING, parser.LIT_STRING_RAW:
		return c.parseStringLit()
	case parser.KW_TRUE:
		return &core.BoolLit{Node: nodeAt(c.advance()), Value: true}, nil
	case parser.KW_FALSE:
		return &core.BoolLit{Node: nodeAt(c.advance()), Value: false}, nil
	case parser.KW_NULL:
		return &core.NullLit{Node: nodeAt(c.advance())}, nil
	case parser.KW_DEL:
		return c.parseDel()
	case parser.LIT_REGEX:
		t := c.advance()
		return &core.RegexLit{Node: nodeAt(t), Pattern: unwrap(t.Lexeme, 2)}, nil
	case parser.LIT_TIMESTAMP:
		t := c.advance()
		return &core.TimestampLit{Node: nodeAt(t), Value: unwrap(t.Lexeme, 2)}, nil

	// Identifier - variable or function call
	case parser.IDENT:
		t := c.advance()
		return &core.IdentExpr{Node: nodeAt(t), Name: t.Lexeme}, nil

	// Paths
	case parser.DOT:
		return c.parseEventPath()
	case parser.PERCENT:
		return c.parseMetadataPath()

	// Unary operators
	case parser.BANG, parser.MINUS:
		return c.parseUnary()

	// Grouped expression
	case parser.LPAREN:
		return c.parseGrouped()

	// Collection literals
	case parser.LBRACKET:
		return c.parseArray()
	case parser.LBRACE:
		return c.parseObject()

	// Control flow
	case parser.KW_IF:
		return c.parseIf()
	case parser.KW_ABORT:
		return &core.AbortExpr{Node: nodeAt(c.advance())}, nil
	case parser.KW_FOR:
		return c.parseFor()
	}

	return nil, c.errorf(tok, "unexpected token %s (%q)", tok.Type, tok.Lexeme)
}

// Called when a token appears between two expressions.
func (c *Compiler) parseInfix(left core.Expr, op parser.Token) (core.Expr, error) {
	switch op.Type {

	case parser.OP_ASSIGN:
		if !isLValue(left) {
			return nil, c.errorf(op, "left side of assignment must be a variable, path, or index expression")
		}
		// right-associative: bp-1 allows chaining a = b = c -> a = (b = c)
		right, err := c.parseExpr(parser.BpAssign - 1)
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
		right, err := c.parseExpr(op.Type.BindingPower())
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
			return nil, c.errorf(op, "function call requires an identifier on the left, got %T", left)
		}
		args, err := c.parseArgList()
		if err != nil {
			return nil, err
		}
		if _, err := c.expect(parser.RPAREN); err != nil {
			return nil, err
		}
		return &core.CallExpr{Node: ident.Node, Name: ident.Name, Args: args}, nil

	// index access
	// path indexing (.field[0]) is handled inside parseEventPath.
	case parser.LBRACKET:
		index, err := c.parseExpr(parser.BpLowest)
		if err != nil {
			return nil, err
		}
		if _, err := c.expect(parser.RBRACKET); err != nil {
			return nil, err
		}
		return &core.IndexExpr{
			Node:   core.NewNode(left.Pos()),
			Object: left,
			Index:  index,
		}, nil
	}

	return nil, c.errorf(op, "unknown infix operator %q", op.Lexeme)
}

func (c *Compiler) parseIntLit() (core.Expr, error) {
	tok := c.advance()
	v, err := strconv.ParseInt(tok.Lexeme, 10, 64)
	if err != nil {
		return nil, c.errorf(tok, "invalid integer literal %q", tok.Lexeme)
	}
	return &core.IntLit{Node: nodeAt(tok), Value: v}, nil
}

func (c *Compiler) parseFloatLit() (core.Expr, error) {
	tok := c.advance()
	v, err := strconv.ParseFloat(tok.Lexeme, 64)
	if err != nil {
		return nil, c.errorf(tok, "invalid float literal %q", tok.Lexeme)
	}
	return &core.FloatLit{Node: nodeAt(tok), Value: v}, nil
}

func (c *Compiler) parseStringLit() (core.Expr, error) {
	tok := c.advance()
	switch tok.Type {
	case parser.LIT_STRING:
		// process escape sequences.
		v, err := strconv.Unquote(tok.Lexeme)
		if err != nil {
			return nil, c.errorf(tok, "invalid string literal: %v", err)
		}
		return &core.StringLit{Node: nodeAt(tok), Value: v}, nil

	case parser.LIT_STRING_RAW:
		return &core.StringLit{Node: nodeAt(tok), Value: unwrap(tok.Lexeme, 2)}, nil
	}
	return nil, c.errorf(tok, "expected string, got %s", tok.Type)
}

func (c *Compiler) parseUnary() (core.Expr, error) {
	op := c.advance()
	operand, err := c.parseExpr(parser.BpUnary)
	if err != nil {
		return nil, err
	}
	return &core.UnaryExpr{Node: nodeAt(op), Op: op.Lexeme, Operand: operand}, nil
}

func (c *Compiler) parseGrouped() (core.Expr, error) {
	// consume (
	c.advance()

	expr, err := c.parseExpr(parser.BpLowest)
	if err != nil {
		return nil, err
	}
	if _, err := c.expect(parser.RPAREN); err != nil {
		return nil, err
	}
	return expr, nil
}

func (c *Compiler) parseArray() (core.Expr, error) {
	// consume [
	start := c.advance()

	var elements []core.Expr
	for !c.check(parser.RBRACKET) && !c.atEnd() {
		el, err := c.parseExpr(parser.BpLowest)
		if err != nil {
			return nil, err
		}
		elements = append(elements, el)
		if !c.match(parser.COMMA) {
			break
		}
	}

	if _, err := c.expect(parser.RBRACKET); err != nil {
		return nil, err
	}
	return &core.ArrayExpr{Node: nodeAt(start), Elements: elements}, nil
}

func (c *Compiler) parseObject() (core.Expr, error) {
	// consume {
	start := c.advance()

	var pairs []core.KVPair
	for !c.check(parser.RBRACE) && !c.atEnd() {
		kv, err := c.parseKVPair()
		if err != nil {
			return nil, err
		}
		pairs = append(pairs, kv)
		if !c.match(parser.COMMA) {
			break
		}
	}

	if _, err := c.expect(parser.RBRACE); err != nil {
		return nil, err
	}
	return &core.ObjectExpr{Node: nodeAt(start), Pairs: pairs}, nil
}

func (c *Compiler) parseKVPair() (core.KVPair, error) {
	tok := c.peek()

	var key string
	switch tok.Type {
	case parser.LIT_STRING:
		t := c.advance()
		v, err := strconv.Unquote(t.Lexeme)
		if err != nil {
			return core.KVPair{}, c.errorf(t, "invalid object key: %v", err)
		}
		key = v
	case parser.LIT_STRING_RAW:
		t := c.advance()
		key = unwrap(t.Lexeme, 2)
	case parser.IDENT:
		key = c.advance().Lexeme
	default:
		return core.KVPair{}, c.errorf(tok, "object key must be a string or identifier, got %s", tok.Type)
	}

	if _, err := c.expect(parser.COLON); err != nil {
		return core.KVPair{}, err
	}

	val, err := c.parseExpr(parser.BpLowest)
	if err != nil {
		return core.KVPair{}, err
	}

	return core.KVPair{Key: key, Value: val}, nil
}

func (c *Compiler) parseEventPath() (core.Expr, error) {
	// consume .
	start := c.advance()

	var segments []core.PathSegment
	if seg, ok, err := c.tryFieldSegment(); err != nil {
		return nil, err
	} else if ok {
		segments = append(segments, seg)
		var err error
		segments, err = c.continueSegments(segments)
		if err != nil {
			return nil, err
		}
	}

	return &core.PathExpr{Node: nodeAt(start), Root: core.EventRoot, Segments: segments}, nil
}

func (c *Compiler) parseMetadataPath() (core.Expr, error) {
	// consume %
	start := c.advance()

	tok := c.peek()
	if tok.Type != parser.IDENT {
		return nil, c.errorf(tok, "expected metadata field name after %%, got %s", tok.Type)
	}

	segments := []core.PathSegment{{Field: c.advance().Lexeme}}

	var err error
	segments, err = c.continueSegments(segments)
	if err != nil {
		return nil, err
	}

	return &core.PathExpr{Node: nodeAt(start), Root: core.MetadataRoot, Segments: segments}, nil
}

// Attempts to read a named path segment.
func (c *Compiler) tryFieldSegment() (core.PathSegment, bool, error) {
	switch c.peek().Type {
	case parser.IDENT:
		return core.PathSegment{Field: c.advance().Lexeme}, true, nil
	case parser.LIT_STRING:
		t := c.advance()
		v, err := strconv.Unquote(t.Lexeme)
		if err != nil {
			return core.PathSegment{}, false, c.errorf(t, "invalid field name: %v", err)
		}
		return core.PathSegment{Field: v}, true, nil
	case parser.LIT_STRING_RAW:
		t := c.advance()
		return core.PathSegment{Field: unwrap(t.Lexeme, 2)}, true, nil
	}
	return core.PathSegment{}, false, nil
}

// Greedily consumes path continuations: .field and [index].
func (c *Compiler) continueSegments(segments []core.PathSegment) ([]core.PathSegment, error) {
	for {
		switch c.peek().Type {
		case parser.DOT:
			if c.pos-1 >= 0 && c.pos-1 < len(c.tokens) {
				dot := c.peek()
				prev := c.tokens[c.pos-1]
				if dot.StartLine > prev.EndLine {
					return segments, nil
				}
			}

			c.advance()
			seg, ok, err := c.tryFieldSegment()
			if err != nil {
				return nil, err
			}
			if !ok {
				return nil, c.errorf(c.peek(), "expected field name after '.', got %s", c.peek().Type)
			}
			segments = append(segments, seg)

		case parser.LBRACKET:
			c.advance()
			index, err := c.parseExpr(parser.BpLowest)
			if err != nil {
				return nil, err
			}
			if _, err := c.expect(parser.RBRACKET); err != nil {
				return nil, err
			}
			segments = append(segments, core.PathSegment{Index: index})

		default:
			return segments, nil
		}
	}
}

// Parses If expressions (e.g. if condition { ... } else { ... })
func (c *Compiler) parseIf() (core.Expr, error) {
	// consume if
	start := c.advance()

	condition, err := c.parseExpr(parser.BpLowest)
	if err != nil {
		return nil, err
	}

	then, err := c.parseBlock()
	if err != nil {
		return nil, err
	}

	var elseBranch []core.Expr
	if c.match(parser.KW_ELSE) {
		if c.check(parser.KW_IF) {
			elseIf, err := c.parseIf()
			if err != nil {
				return nil, err
			}
			elseBranch = []core.Expr{elseIf}
		} else {
			elseBranch, err = c.parseBlock()
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
func (c *Compiler) parseBlock() ([]core.Expr, error) {
	if _, err := c.expect(parser.LBRACE); err != nil {
		return nil, err
	}

	var exprs []core.Expr
	for !c.check(parser.RBRACE) && !c.atEnd() {
		e, err := c.parseExpr(parser.BpLowest)
		if err != nil {
			return nil, err
		}
		exprs = append(exprs, e)
		for c.match(parser.SEMICOLON) {
		}
	}

	if _, err := c.expect(parser.RBRACE); err != nil {
		return nil, err
	}
	return exprs, nil
}

func (c *Compiler) parseArgList() ([]core.Argument, error) {
	var args []core.Argument
	for !c.check(parser.RPAREN) && !c.atEnd() {
		arg, err := c.parseArgument()
		if err != nil {
			return nil, err
		}
		args = append(args, arg)
		if !c.match(parser.COMMA) {
			break
		}
	}
	return args, nil
}

// Parses function arguments: named (key: expr) or positional (expr).
func (c *Compiler) parseArgument() (core.Argument, error) {
	if c.peek().Type == parser.IDENT && c.peekAt(1).Type == parser.COLON {
		name := c.advance().Lexeme
		c.advance()
		val, err := c.parseExpr(parser.BpLowest)
		if err != nil {
			return core.Argument{}, err
		}
		return core.Argument{Name: name, Value: val}, nil
	}

	val, err := c.parseExpr(parser.BpLowest)
	if err != nil {
		return core.Argument{}, err
	}
	return core.Argument{Value: val}, nil
}

// Parses delete expressions (e.g. del .field | del .field.nested[0] | del %meta.key)
//
// Only core.PathExpr is a valid target - anything else is a compile-time error.
func (c *Compiler) parseDel() (core.Expr, error) {
	start := c.advance()

	tok := c.peek()

	var pathExpr *core.PathExpr

	switch tok.Type {
	case parser.DOT:
		raw, err := c.parseEventPath()
		if err != nil {
			return nil, err
		}
		pathExpr = raw.(*core.PathExpr)

	case parser.PERCENT:
		raw, err := c.parseMetadataPath()
		if err != nil {
			return nil, err
		}
		pathExpr = raw.(*core.PathExpr)

	default:
		return nil, c.errorf(tok, "del requires a path (.field or %%field), got %s", tok.Type)
	}

	return &core.DelExpr{Node: nodeAt(start), Target: pathExpr}, nil
}

// Parses for expressions (e.g. for i in expr { ... } | for i, item in expr { ... })
func (c *Compiler) parseFor() (core.Expr, error) {
	start := c.advance()

	first, err := c.expect(parser.IDENT)
	if err != nil {
		return nil, err
	}

	var indexName, itemName string

	if c.match(parser.COMMA) {
		second, err := c.expect(parser.IDENT)
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
		return nil, c.errorf(first, "for loop must bind at least one variable: use 'for i in ...' or 'for i, item in ...'")
	}

	if _, err := c.expect(parser.KW_IN); err != nil {
		return nil, err
	}

	iter, err := c.parseExpr(parser.BpLowest)
	if err != nil {
		return nil, err
	}

	body, err := c.parseBlock()
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
	case *core.IdentExpr, *core.PathExpr, *core.IndexExpr:
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
