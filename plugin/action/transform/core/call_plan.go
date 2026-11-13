package core

import "slices"

// CallExecutor is an immutable prepared call. It may bind expressions and
// constants, but must obtain the target and mutable state from each context.
type CallExecutor interface {
	EvalCall(ctx EvalContext, expr *CallExpr) (Value, error)
}

// CallPlanner resolves unprepared specialized calls against the current context.
// A nil executor preserves CallFunc. The shared expression must not be modified.
type CallPlanner interface {
	PlanCall(expr *CallExpr) (CallExecutor, error)
}

// CallPlan is an immutable binding of source arguments to parameter slots.
// Only the layout and callable are shared; each evaluation owns its values.
type CallPlan struct {
	slots    []int
	defaults []Value
	call     func(*CallState, Position, []Value) (Value, error)
}

// NewCallPlan binds an already validated call. slots maps each source argument
// to a parameter index; defaults has one entry per parameter. The callable must
// neither retain the argument slice nor mutate shared default values.
func NewCallPlan(slots []int, defaults []Value, call func(Position, []Value) (Value, error)) *CallPlan {
	return NewStatefulCallPlan(slots, defaults, func(_ *CallState, pos Position, values []Value) (Value, error) {
		return call(pos, values)
	})
}

// NewStatefulCallPlan also passes the current execution's state to the callable.
// State is nil for contexts without CallStateProvider. Neither state nor the
// argument slice may be retained by the callable. Returned values may outlive
// the call and must not depend on mutable storage reused by subsequent calls.
func NewStatefulCallPlan(slots []int, defaults []Value, call func(*CallState, Position, []Value) (Value, error)) *CallPlan {
	return &CallPlan{
		slots:    slices.Clone(slots),
		defaults: slices.Clone(defaults),
		call:     call,
	}
}

func (p *CallPlan) EvalCall(ctx EvalContext, expr *CallExpr) (Value, error) {
	var state *CallState
	if provider, ok := ctx.(CallStateProvider); ok {
		state = provider.CallState()
	}
	var values []Value
	if state != nil {
		values = state.acquire(len(p.defaults))
		defer state.release(values)
	} else {
		values = make([]Value, len(p.defaults))
	}
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
	return p.call(state, expr.Pos(), values)
}
