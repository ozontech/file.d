package compiler

import (
	"github.com/ozontech/file.d/plugin/action/transform/core"
)

// Constant folding computes the value of constant sub-expressions once, at
// startup, and stores it in a core.ConstExpr that replaces them in the AST.
//
// It exists so that data prepared for a function - a lookup table written as an
// object literal, most of all - is built a single time instead of on every
// event: core.ObjectExpr.Eval allocates a fresh map on every evaluation.
//
// Folding runs inside the validation walk (see validateExpr) rather than in a
// pass of its own, because that walk already visits every node and already
// prepares nodes in place: it compiles regex literals and parses timestamp
// literals. Order matters - a node is folded only after the walk has visited
// it, so RegexLit.Compiled and TimestampLit.Parsed are populated by then.
//
// An expression whose evaluation fails is never folded. It stays in the AST and
// fails per event exactly as it does today, so folding cannot turn a runtime
// error into a startup error.

// tryFold returns a core.ConstExpr holding the value of expr when that value
// can be computed at startup, and expr itself otherwise.
func tryFold(expr core.Expr) core.Expr {
	if _, ok := expr.(*core.ConstExpr); ok {
		return expr
	}
	v, ok := foldConst(expr)
	if !ok {
		return expr
	}
	return &core.ConstExpr{Node: core.NewNode(expr.Pos()), V: v}
}

// foldConst computes the value of expr, reporting whether it is constant.
// A composite is constant only when every part of it is.
func foldConst(expr core.Expr) (core.Value, bool) {
	switch e := expr.(type) {
	case *core.ConstExpr:
		return e.V, true

	case *core.IntLit:
		return core.IntegerValue{V: e.Value}, true
	case *core.FloatLit:
		return core.FloatValue{V: e.Value}, true
	case *core.StringLit:
		return core.StringValue{V: e.Value}, true
	case *core.BoolLit:
		return core.BoolValue{V: e.Value}, true
	case *core.NullLit:
		return core.NullValue{}, true

	case *core.RegexLit:
		// Compiled by the validation walk; nil means this node has not been
		// visited yet, so leave it to be evaluated at runtime.
		if e.Compiled == nil {
			return nil, false
		}
		return core.RegexValue{V: e.Compiled}, true

	case *core.TimestampLit:
		// Parsed by the validation walk. A zero time means the node has not
		// been visited; t'0001-01-01T00:00:00Z' simply misses the optimization.
		if e.Parsed.IsZero() {
			return nil, false
		}
		return core.TimestampValue{V: e.Parsed}, true

	case *core.ArrayExpr:
		elements := make([]core.Value, len(e.Elements))
		for i, el := range e.Elements {
			v, ok := foldConst(el)
			if !ok {
				return nil, false
			}
			elements[i] = v
		}
		return core.ArrayValue{V: elements}, true

	case *core.ObjectExpr:
		// Duplicate keys are rejected by validateExpr before we get here.
		pairs := make(map[string]core.Value, len(e.Pairs))
		for _, kv := range e.Pairs {
			v, ok := foldConst(kv.Value)
			if !ok {
				return nil, false
			}
			pairs[kv.Key] = v
		}
		return core.ObjectValue{V: pairs}, true

	case *core.UnaryExpr:
		operand, ok := foldConst(e.Operand)
		if !ok {
			return nil, false
		}
		return evalConst(&core.UnaryExpr{
			Node:    core.NewNode(e.Pos()),
			Op:      e.Op,
			Operand: constNode(e.Operand, operand),
		})

	case *core.BinaryExpr:
		left, ok := foldConst(e.Left)
		if !ok {
			return nil, false
		}
		right, ok := foldConst(e.Right)
		if !ok {
			return nil, false
		}
		return evalConst(&core.BinaryExpr{
			Node:  core.NewNode(e.Pos()),
			Op:    e.Op,
			Left:  constNode(e.Left, left),
			Right: constNode(e.Right, right),
		})
	}

	return nil, false
}

func constNode(from core.Expr, v core.Value) core.Expr {
	return &core.ConstExpr{Node: core.NewNode(from.Pos()), V: v}
}

// evalConst evaluates an operator node whose operands are already core.ConstExpr.
// Those ignore the evaluation context, and UnaryExpr/BinaryExpr touch the context
// only to evaluate their operands, so a nil context is never dereferenced.
//
// A failing expression - `1 / 0`, `"a" + 1` - is reported as not constant so it
// keeps failing at runtime instead of breaking pipeline startup.
func evalConst(expr core.Expr) (core.Value, bool) {
	v, err := expr.Eval(nil)
	if err != nil {
		return nil, false
	}
	return v, true
}
