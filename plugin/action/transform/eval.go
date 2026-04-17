package transform

import (
	"fmt"
	"math"
	"time"
)

// evalBlock evaluates a sequence of expressions and returns the value of the last one.
// An empty block evaluates to null.
func evalBlock(ctx *Context, exprs []Expr) (Value, error) {
	var last Value = NullValue{}
	for _, expr := range exprs {
		val, err := expr.Eval(ctx)
		if err != nil {
			return NullValue{}, err
		}
		last = val
	}
	return last, nil
}

func (e *IntLit) Eval(_ *Context) (Value, error) {
	return IntegerValue{V: e.Value}, nil
}

func (e *FloatLit) Eval(_ *Context) (Value, error) {
	return FloatValue{V: e.Value}, nil
}

func (e *StringLit) Eval(_ *Context) (Value, error) {
	return StringValue{V: e.Value}, nil
}

func (e *BoolLit) Eval(_ *Context) (Value, error) {
	return BoolValue{V: e.Value}, nil
}

func (e *NullLit) Eval(_ *Context) (Value, error) {
	return NullValue{}, nil
}

func (e *RegexLit) Eval(_ *Context) (Value, error) {
	return RegexValue{V: e.compiled}, nil
}

func (e *TimestampLit) Eval(_ *Context) (Value, error) {
	return TimestampValue{V: e.parsed}, nil
}

func (e *IdentExpr) Eval(ctx *Context) (Value, error) {
	if val, ok := ctx.GetVar(e.Name); ok {
		return val, nil
	}
	// nil or error ???
	return NullValue{}, nil
}

func (e *PathExpr) Eval(ctx *Context) (Value, error) {
	path, err := e.toRuntimePath(ctx)
	if err != nil {
		return NullValue{}, err
	}
	val, err := ctx.Target.Get(path)
	return val, err
}

// toRuntimePath converts a PathExpr (AST) into a Path (runtime) by evaluating
// any dynamic index expressions contained in the segment list.
//
//   - .field           -> FieldSeg("field")
//   - [0]              -> IndexSeg(0)
//   - ["key"]          -> FieldSeg("key")   - string key becomes a named field
//   - [.dynamic_idx]   -> IndexSeg(n)       - expression evaluated at runtime
func (e *PathExpr) toRuntimePath(ctx *Context) (Path, error) {
	segs := make([]Segment, 0, len(e.Segments))

	for _, s := range e.Segments {
		if s.IsField() {
			segs = append(segs, FieldSeg(s.Field))
			continue
		}

		idxVal, err := s.Index.Eval(ctx)
		if err != nil {
			return Path{}, err
		}

		switch v := idxVal.(type) {
		case IntegerValue:
			segs = append(segs, IndexSeg(int(v.V)))
		case StringValue:
			segs = append(segs, FieldSeg(v.V))
		default:
			return Path{}, fmt.Errorf(
				"%s: path index must be integer or string, got %s", e.Pos(), idxVal.Kind())
		}
	}

	return Path{Root: e.Root, Segments: segs}, nil
}

func (e *ArrayExpr) Eval(ctx *Context) (Value, error) {
	elements := make([]Value, len(e.Elements))
	for i, el := range e.Elements {
		val, err := el.Eval(ctx)
		if err != nil {
			return NullValue{}, err
		}
		elements[i] = val
	}
	return ArrayValue{V: elements}, nil
}

func (e *ObjectExpr) Eval(ctx *Context) (Value, error) {
	result := make(map[string]Value, len(e.Pairs))
	for _, kv := range e.Pairs {
		val, err := kv.Value.Eval(ctx)
		if err != nil {
			return NullValue{}, err
		}
		result[kv.Key] = val
	}
	return ObjectValue{V: result}, nil
}

func (e *UnaryExpr) Eval(ctx *Context) (Value, error) {
	operand, err := e.Operand.Eval(ctx)
	if err != nil {
		return NullValue{}, err
	}

	switch e.Op {
	case "!":
		return BoolValue{V: !operand.AsBool()}, nil

	case "-":
		return evalNegate(e.Pos(), resolve(operand))
	}

	return NullValue{}, fmt.Errorf("%s: unknown unary operator %q", e.Pos(), e.Op)
}

func evalNegate(pos Position, operand Value) (Value, error) {
	switch v := operand.(type) {
	case IntegerValue:
		return IntegerValue{V: -v.V}, nil
	case FloatValue:
		return FloatValue{V: -v.V}, nil
	}
	return NullValue{}, fmt.Errorf("%s: unary minus requires integer or float, got %s",
		pos, operand.Kind())
}

func (e *BinaryExpr) Eval(ctx *Context) (Value, error) {
	switch e.Op {
	case "&&":
		left, err := e.Left.Eval(ctx)
		if err != nil {
			return NullValue{}, err
		}
		if !left.AsBool() {
			return left, nil
		}
		return e.Right.Eval(ctx)

	case "||":
		left, err := e.Left.Eval(ctx)
		if err != nil {
			return NullValue{}, err
		}
		if left.AsBool() {
			return left, nil
		}
		return e.Right.Eval(ctx)
	}

	left, err := e.Left.Eval(ctx)
	if err != nil {
		return NullValue{}, err
	}
	right, err := e.Right.Eval(ctx)
	if err != nil {
		return NullValue{}, err
	}

	switch e.Op {
	case "==":
		return BoolValue{V: left.Equal(right)}, nil
	case "!=":
		return BoolValue{V: !left.Equal(right)}, nil
	case "+":
		return evalAdd(e.Pos(), resolve(left), resolve(right))
	case "-", "*", "/", "%":
		return evalArithmetic(e.Pos(), e.Op, resolve(left), resolve(right))
	case "<", "<=", ">", ">=":
		return evalComparison(e.Pos(), e.Op, resolve(left), resolve(right))
	}

	return NullValue{}, fmt.Errorf("%s: unknown binary operator %q", e.Pos(), e.Op)
}

func (e *AssignExpr) Eval(ctx *Context) (Value, error) {
	value, err := e.Value.Eval(ctx)
	if err != nil {
		return NullValue{}, err
	}

	switch target := e.Target.(type) {

	case *IdentExpr:
		ctx.SetVar(target.Name, value)
		return value, nil
	case *PathExpr:
		path, err := target.toRuntimePath(ctx)
		if err != nil {
			return NullValue{}, err
		}
		if err := ctx.Target.Set(path, value); err != nil {
			return NullValue{}, fmt.Errorf("%s: %w", e.Pos(), err)
		}
		return value, nil
	case *IndexExpr:
		if err := evalIndexAssign(ctx, target, value); err != nil {
			return NullValue{}, fmt.Errorf("%s: %w", e.Pos(), err)
		}
		return value, nil
	}

	return NullValue{}, fmt.Errorf("%s: invalid assignment target %T", e.Pos(), e.Target)
}

// evalIndexAssign handles arr[n] = value and obj["key"] = value
// where the object is a local variable (IdentExpr).
func evalIndexAssign(ctx *Context, target *IndexExpr, value Value) error {
	ident, ok := target.Object.(*IdentExpr)
	if !ok {
		return fmt.Errorf("index assignment target must be a local variable, got %T",
			target.Object)
	}

	current, _ := ctx.GetVar(ident.Name)

	idxVal, err := target.Index.Eval(ctx)
	if err != nil {
		return err
	}

	switch idx := idxVal.(type) {

	case IntegerValue:
		// arr[n] = value
		arr, ok := current.(ArrayValue)
		if !ok {
			return fmt.Errorf("cannot use integer index on %s", current.Kind())
		}
		resolved := resolveIndex(int(idx.V), len(arr.V))
		if resolved < 0 {
			return fmt.Errorf("index %d is out of bounds (len %d)", idx.V, len(arr.V))
		}
		newSlice := make([]Value, len(arr.V))
		copy(newSlice, arr.V)
		// Grow with nulls if the index exceeds the current length.
		for len(newSlice) <= resolved {
			newSlice = append(newSlice, NullValue{})
		}
		newSlice[resolved] = value
		ctx.SetVar(ident.Name, ArrayValue{V: newSlice})
		return nil

	case StringValue:
		// obj["key"] = value
		obj, ok := current.(ObjectValue)
		if !ok {
			return fmt.Errorf("cannot use string index on %s", current.Kind())
		}
		newMap := make(map[string]Value, len(obj.V)+1)
		for k, v := range obj.V {
			newMap[k] = v
		}
		newMap[idx.V] = value
		ctx.SetVar(ident.Name, ObjectValue{V: newMap})
		return nil
	}

	return fmt.Errorf("index must be integer or string, got %s", idxVal.Kind())
}

// Eval resolves arr[n] and obj["key"] on local variables and call results.
func (e *IndexExpr) Eval(ctx *Context) (Value, error) {
	obj, err := e.Object.Eval(ctx)
	if err != nil {
		return NullValue{}, err
	}

	idx, err := e.Index.Eval(ctx)
	if err != nil {
		return NullValue{}, err
	}

	return evalIndex(e.Pos(), resolve(obj), resolve(idx))
}

func evalIndex(pos Position, obj, idx Value) (Value, error) {
	switch o := obj.(type) {
	case ArrayValue:
		i, ok := idx.(IntegerValue)
		if !ok {
			return NullValue{}, fmt.Errorf(
				"%s: array index must be integer, got %s", pos, idx.Kind())
		}
		resolved := resolveIndex(int(i.V), len(o.V))
		if resolved < 0 || resolved >= len(o.V) {
			// nil or error ???
			return NullValue{}, nil
		}
		return o.V[resolved], nil
	case ObjectValue:
		s, ok := idx.(StringValue)
		if !ok {
			return NullValue{}, fmt.Errorf(
				"%s: object index must be string, got %s", pos, idx.Kind())
		}
		val, exists := o.V[s.V]
		if !exists {
			// nil or error ???
			return NullValue{}, nil
		}
		return val, nil
	}

	return NullValue{}, fmt.Errorf("%s: cannot index into %s", pos, obj.Kind())
}

func (e *CallExpr) Eval(ctx *Context) (Value, error) {
	fn, ok := ctx.Registry.Get(e.Name)
	if !ok {
		return NullValue{}, fmt.Errorf("%s: unknown function %q", e.Pos(), e.Name)
	}

	var positional []Value
	named := make(map[string]Value)

	for _, arg := range e.Args {
		val, err := arg.Value.Eval(ctx)
		if err != nil {
			return NullValue{}, err
		}

		val = resolve(val)

		if arg.Name == "" {
			positional = append(positional, val)
		} else {
			named[arg.Name] = val
		}
	}

	resolved, err := ctx.Registry.ResolveArgs(fn, positional, named)
	if err != nil {
		// null or error ???
		return NullValue{}, nil
	}

	result, err := fn.Call(resolved)
	if err != nil {
		// null or error ???
		return NullValue{}, nil
	}
	return result, nil
}
func (e *IfExpr) Eval(ctx *Context) (Value, error) {
	condition, err := e.Condition.Eval(ctx)
	if err != nil {
		return NullValue{}, err
	}

	if condition.AsBool() {
		return evalBlock(ctx, e.Then)
	}
	if len(e.Else) > 0 {
		return evalBlock(ctx, e.Else)
	}

	return NullValue{}, nil
}

func (e *AbortExpr) Eval(_ *Context) (Value, error) {
	return NullValue{}, AbortError{}
}

func (e *DelExpr) Eval(ctx *Context) (Value, error) {
	path, err := e.Target.toRuntimePath(ctx)
	if err != nil {
		return NullValue{}, err
	}
	if err := ctx.Target.Delete(path); err != nil {
		return NullValue{}, fmt.Errorf("%s: del: %w", e.Pos(), err)
	}
	return NullValue{}, nil
}

// evalAdd handles the "+" operator:
//   - string + string  -> concatenation
//   - int    + int     -> integer result
//   - any numeric mix  -> float result
func evalAdd(pos Position, left, right Value) (Value, error) {
	if l, ok := left.(StringValue); ok {
		r, ok := right.(StringValue)
		if !ok {
			return NullValue{}, fmt.Errorf(
				"%s: operator +: cannot concatenate string with %s", pos, right.Kind())
		}
		return StringValue{V: l.V + r.V}, nil
	}

	if l, ok := left.(IntegerValue); ok {
		if r, ok := right.(IntegerValue); ok {
			return IntegerValue{V: l.V + r.V}, nil
		}
	}

	l, err := ToFloat(left)
	if err != nil {
		return NullValue{}, fmt.Errorf("%s: operator +: %w", pos, err)
	}
	r, err := ToFloat(right)
	if err != nil {
		return NullValue{}, fmt.Errorf("%s: operator +: %w", pos, err)
	}
	return FloatValue{V: l + r}, nil
}

func evalArithmetic(pos Position, op string, left, right Value) (Value, error) {
	if l, ok := left.(IntegerValue); ok {
		if r, ok := right.(IntegerValue); ok {
			switch op {
			case "-":
				return IntegerValue{V: l.V - r.V}, nil
			case "*":
				return IntegerValue{V: l.V * r.V}, nil
			case "/":
				if r.V == 0 {
					return NullValue{}, fmt.Errorf("%s: operator /: division by zero", pos)
				}
				return IntegerValue{V: l.V / r.V}, nil
			case "%":
				if r.V == 0 {
					return NullValue{}, fmt.Errorf("%s: operator %%: modulo by zero", pos)
				}
				return IntegerValue{V: l.V % r.V}, nil
			}
		}
	}

	l, err := ToFloat(left)
	if err != nil {
		return NullValue{}, fmt.Errorf("%s: operator %s: %w", pos, op, err)
	}
	r, err := ToFloat(right)
	if err != nil {
		return NullValue{}, fmt.Errorf("%s: operator %s: %w", pos, op, err)
	}

	switch op {
	case "-":
		return FloatValue{V: l - r}, nil
	case "*":
		return FloatValue{V: l * r}, nil
	case "/":
		if r == 0 {
			return NullValue{}, fmt.Errorf("%s: operator /: division by zero", pos)
		}
		return FloatValue{V: l / r}, nil
	case "%":
		if r == 0 {
			return NullValue{}, fmt.Errorf("%s: operator %%: modulo by zero", pos)
		}
		return FloatValue{V: math.Mod(l, r)}, nil
	}

	return NullValue{}, fmt.Errorf("%s: unknown arithmetic operator %q", pos, op)
}

func evalComparison(pos Position, op string, left, right Value) (Value, error) {
	if l, ok := left.(IntegerValue); ok {
		if r, ok := right.(IntegerValue); ok {
			return BoolValue{V: cmpInts(op, l.V, r.V)}, nil
		}
	}

	lNum, lErr := ToFloat(left)
	rNum, rErr := ToFloat(right)
	if lErr == nil && rErr == nil {
		return BoolValue{V: cmpFloats(op, lNum, rNum)}, nil
	}

	if l, ok := left.(StringValue); ok {
		if r, ok := right.(StringValue); ok {
			return BoolValue{V: cmpStrings(op, l.V, r.V)}, nil
		}
	}

	if l, ok := left.(TimestampValue); ok {
		if r, ok := right.(TimestampValue); ok {
			return BoolValue{V: cmpTimestamps(op, l.V, r.V)}, nil
		}
	}

	return NullValue{}, fmt.Errorf(
		"%s: operator %s: cannot compare %s and %s", pos, op, left.Kind(), right.Kind())
}

func cmpInts(op string, l, r int64) bool {
	switch op {
	case "<":
		return l < r
	case "<=":
		return l <= r
	case ">":
		return l > r
	case ">=":
		return l >= r
	}
	return false
}

func cmpFloats(op string, l, r float64) bool {
	switch op {
	case "<":
		return l < r
	case "<=":
		return l <= r
	case ">":
		return l > r
	case ">=":
		return l >= r
	}
	return false
}

func cmpStrings(op, l, r string) bool {
	switch op {
	case "<":
		return l < r
	case "<=":
		return l <= r
	case ">":
		return l > r
	case ">=":
		return l >= r
	}
	return false
}

func cmpTimestamps(op string, l, r time.Time) bool {
	switch op {
	case "<":
		return l.Before(r)
	case "<=":
		return !l.After(r)
	case ">":
		return l.After(r)
	case ">=":
		return !l.Before(r)
	}
	return false
}
