package core

import "slices"

// CallPlan is an immutable binding of source arguments to parameter slots.
// Only the layout and callable are shared; each evaluation owns its values.
type CallPlan struct {
	slots    []int
	defaults []Value
	call     func(Position, []Value) (Value, error)
}

// NewCallPlan binds an already validated call. slots maps each source argument
// to a parameter index; defaults has one entry per parameter. The callable must
// neither retain the argument slice nor mutate shared default values.
func NewCallPlan(slots []int, defaults []Value, call func(Position, []Value) (Value, error)) *CallPlan {
	return &CallPlan{
		slots:    slices.Clone(slots),
		defaults: slices.Clone(defaults),
		call:     call,
	}
}

func (p *CallPlan) eval(ctx EvalContext, expr *CallExpr) (Value, error) {
	values := make([]Value, len(p.defaults))
	copy(values, p.defaults)
	for i, arg := range expr.Args {
		value, err := arg.Value.Eval(ctx)
		if err != nil {
			return NullValue{}, err
		}
		// Materialize before evaluating the next argument, which may mutate
		// the event referenced by a JSONNodeValue.
		values[p.slots[i]] = resolve(value)
	}
	return p.call(expr.Pos(), values)
}
