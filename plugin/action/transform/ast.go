package transform

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

type Position struct {
	Line   int
	Column int
}

func (p Position) String() string {
	return fmt.Sprintf("%d:%d", p.Line, p.Column)
}

type Expr interface {
	Pos() Position
	Eval(ctx *Context) (Value, error)
}

type node struct {
	pos Position
}

func (n node) Pos() Position {
	return n.pos
}

type IntLit struct {
	node
	Value int64
}

type FloatLit struct {
	node
	Value float64
}

type StringLit struct {
	node
	Value string
}

type BoolLit struct {
	node
	Value bool
}

type NullLit struct {
	node
}

type RegexLit struct {
	node
	Pattern  string
	compiled *regexp.Regexp
}

type TimestampLit struct {
	node
	Value  string
	parsed time.Time
}

type IdentExpr struct {
	node
	Name string
}

type PathRoot int

const (
	EventRoot PathRoot = iota
	MetadataRoot
)

type PathSegment struct {
	Field string
	Index Expr
}

func (s PathSegment) IsField() bool { return s.Field != "" }
func (s PathSegment) IsIndex() bool { return s.Index != nil }

type PathExpr struct {
	node
	Root     PathRoot
	Segments []PathSegment
}

type ArrayExpr struct {
	node
	Elements []Expr
}

type KVPair struct {
	Key   string
	Value Expr
}

type ObjectExpr struct {
	node
	Pairs []KVPair
}

type UnaryExpr struct {
	node
	Op      string
	Operand Expr
}

type BinaryExpr struct {
	node
	Left  Expr
	Op    string
	Right Expr
}

type AssignExpr struct {
	node
	Target Expr
	Value  Expr
}

type IndexExpr struct {
	node
	Object Expr
	Index  Expr
}

type Argument struct {
	Name  string
	Value Expr
}

type CallExpr struct {
	node
	Name string
	Args []Argument
}

type IfExpr struct {
	node
	Condition Expr
	Then      []Expr
	Else      []Expr
}

type AbortExpr struct {
	node
}

type DelExpr struct {
	node
	Target *PathExpr
}

// DumpAST returns a human-readable representation of the AST.
// Use only for debug
func DumpAST(expr Expr, depth int) string {
	pad := strings.Repeat("  ", depth)
	p := depth + 1

	switch e := expr.(type) {
	case *IntLit:
		return fmt.Sprintf("%sIntLit(%d)", pad, e.Value)
	case *FloatLit:
		return fmt.Sprintf("%sFloatLit(%g)", pad, e.Value)
	case *StringLit:
		return fmt.Sprintf("%sStringLit(%q)", pad, e.Value)
	case *BoolLit:
		return fmt.Sprintf("%sBoolLit(%v)", pad, e.Value)
	case *NullLit:
		return fmt.Sprintf("%sNullLit", pad)
	case *RegexLit:
		return fmt.Sprintf("%sRegexLit(%q)", pad, e.Pattern)
	case *TimestampLit:
		return fmt.Sprintf("%sTimestampLit(%q)", pad, e.Value)
	case *IdentExpr:
		return fmt.Sprintf("%sIdent(%s)", pad, e.Name)

	case *PathExpr:
		root := "."
		if e.Root == MetadataRoot {
			root = "%"
		}
		parts := make([]string, 0, len(e.Segments))
		for _, s := range e.Segments {
			if s.IsField() {
				parts = append(parts, s.Field)
			} else {
				parts = append(parts, fmt.Sprintf("[%s]", DumpAST(s.Index, 0)))
			}
		}
		return fmt.Sprintf("%sPath(%s%s)", pad, root, strings.Join(parts, "."))

	case *ArrayExpr:
		lines := []string{fmt.Sprintf("%sArray", pad)}
		for _, el := range e.Elements {
			lines = append(lines, DumpAST(el, p))
		}
		return strings.Join(lines, "\n")

	case *ObjectExpr:
		lines := []string{fmt.Sprintf("%sObject", pad)}
		for _, kv := range e.Pairs {
			lines = append(lines, fmt.Sprintf("%s  key(%q):", pad, kv.Key))
			lines = append(lines, DumpAST(kv.Value, p+1))
		}
		return strings.Join(lines, "\n")

	case *UnaryExpr:
		return fmt.Sprintf("%sUnary(%s)\n%s", pad, e.Op, DumpAST(e.Operand, p))

	case *BinaryExpr:
		return fmt.Sprintf("%sBinary(%s)\n%s\n%s",
			pad, e.Op,
			DumpAST(e.Left, p),
			DumpAST(e.Right, p),
		)

	case *AssignExpr:
		return fmt.Sprintf("%sAssign\n%s\n%s",
			pad,
			DumpAST(e.Target, p),
			DumpAST(e.Value, p),
		)

	case *IndexExpr:
		return fmt.Sprintf("%sIndex\n%s\n%s",
			pad,
			DumpAST(e.Object, p),
			DumpAST(e.Index, p),
		)

	case *CallExpr:
		lines := []string{fmt.Sprintf("%sCall(%s)", pad, e.Name)}
		for _, arg := range e.Args {
			if arg.Name != "" {
				lines = append(lines, fmt.Sprintf("%s  named(%s:)", pad, arg.Name))
				lines = append(lines, DumpAST(arg.Value, p+1))
			} else {
				lines = append(lines, DumpAST(arg.Value, p))
			}
		}
		return strings.Join(lines, "\n")

	case *IfExpr:
		lines := []string{
			fmt.Sprintf("%sIf", pad),
			fmt.Sprintf("%s  condition:", pad),
			DumpAST(e.Condition, p+1),
			fmt.Sprintf("%s  then:", pad),
		}
		for _, t := range e.Then {
			lines = append(lines, DumpAST(t, p+1))
		}
		if len(e.Else) > 0 {
			lines = append(lines, fmt.Sprintf("%s  else:", pad))
			for _, el := range e.Else {
				lines = append(lines, DumpAST(el, p+1))
			}
		}
		return strings.Join(lines, "\n")

	case *AbortExpr:
		return fmt.Sprintf("%sAbort", pad)
	case *DelExpr:
		return fmt.Sprintf("%sDel\n%s", pad, DumpAST(e.Target, p))
	}

	return fmt.Sprintf("%s<unknown node %T>", pad, expr)
}
