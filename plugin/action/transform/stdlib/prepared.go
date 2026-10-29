package stdlib

import (
	"fmt"

	"github.com/ozontech/file.d/plugin/action/transform/core"
)

// indexedFunction receives validated values in Params order, without maps.
// Implementations must not retain args or mutate shared constants/defaults.
type indexedFunction interface {
	CallIndexed(args []core.Value) (core.Value, error)
}

// statefulIndexedFunction optionally uses execution-owned scratch storage.
// State can be nil; implementations must then work without reuse. Neither state
// nor args may escape the call, and returned values must survive later reuse.
// Call and CallIndexed remain the supported entry points without runtime state.
type statefulIndexedFunction interface {
	indexedFunction
	CallIndexedWithState(state *core.CallState, args []core.Value) (core.Value, error)
}

// PrepareCall binds an already structurally validated call to this function.
// Functions without indexed support keep the general call path; every function
// in the standard registry supports indexed arguments.
func (c *CompiledFunc) PrepareCall(args []core.Argument) *core.CallPlan {
	fn, ok := c.fn.(indexedFunction)
	if !ok {
		return nil
	}

	defaults := make([]core.Value, len(c.params))
	for i, p := range c.params {
		defaults[i] = p.Default
	}
	slots := make([]int, len(args))
	positional := 0
	for i, arg := range args {
		if arg.Name == "" {
			slots[i] = positional
			positional++
			continue
		}
		for j := c.numPositional; j < len(c.params); j++ {
			if c.params[j].Name == arg.Name {
				slots[i] = j
				break
			}
		}
	}

	call := func(_ *core.CallState, values []core.Value) (core.Value, error) {
		return fn.CallIndexed(values)
	}
	if stateful, ok := fn.(statefulIndexedFunction); ok {
		call = stateful.CallIndexedWithState
	}

	return core.NewStatefulCallPlan(slots, defaults, func(state *core.CallState, pos core.Position, values []core.Value) (core.Value, error) {
		if c.checkKinds {
			for i, p := range c.params {
				if err := c.checkKind(p, values[i]); err != nil {
					return core.NullValue{}, fmt.Errorf("%s: %w", pos, err)
				}
			}
		}
		result, err := call(state, values)
		if err != nil {
			return core.NullValue{}, fmt.Errorf("%s: %s: %w", pos, c.Name(), err)
		}
		return result, nil
	})
}
