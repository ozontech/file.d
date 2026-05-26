package runtime

import (
	"fmt"
	"slices"
	"strings"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/ozontech/file.d/plugin/action/transform/stdlib"
)

// Context carries all runtime state available during expression evaluation.
//
// A single Context is created per Program.Run call and passed down through every Eval call.
type Context struct {
	target   core.Target
	registry *stdlib.Registry
	scope    map[string]core.Value
}

func NewContext(target core.Target, registry *stdlib.Registry) *Context {
	return &Context{
		target:   target,
		registry: registry,
		scope:    make(map[string]core.Value),
	}
}

func (c *Context) GetVar(name string) (core.Value, bool) {
	v, ok := c.scope[name]
	return v, ok
}

func (c *Context) SetVar(name string, value core.Value) {
	c.scope[name] = value
}

func (c *Context) DeleteVar(name string) {
	delete(c.scope, name)
}

func (c *Context) GetTarget() core.Target {
	return c.target
}

func (c *Context) CallFunc(pos core.Position, name string, positional []core.Value, named map[string]core.Value) (core.Value, error) {
	fn, ok := c.registry.Get(name)
	if !ok {
		return core.NullValue{}, fmt.Errorf("%s: unknown function %q", pos, name)
	}
	resolved, err := ResolveFunctionArgs(fn, positional, named)
	if err != nil {
		return core.NullValue{}, nil
	}
	result, err := fn.Call(resolved)
	if err != nil {
		return core.NullValue{}, nil
	}
	return result, nil
}

// Maps evaluated argument values to the function's parameter map.
func ResolveFunctionArgs(
	fn stdlib.Function,
	positional []core.Value,
	named map[string]core.Value,
) (map[string]core.Value, error) {
	params := fn.Params()

	if len(positional) > len(params) {
		return nil, fmt.Errorf(
			"function %q: too many arguments: expected at most %d, got %d",
			fn.Name(), len(params), len(positional),
		)
	}

	resolved := make(map[string]core.Value, len(params))
	explicit := make(map[string]bool, len(params))

	for _, p := range params {
		if p.Default != nil {
			resolved[p.Name] = p.Default
		}
	}

	for i, val := range positional {
		pName := params[i].Name
		resolved[pName] = val
		explicit[pName] = true
	}

	for argName, val := range named {
		if !slices.ContainsFunc(params, func(p stdlib.Parameter) bool { return p.Name == argName }) {
			return nil, fmt.Errorf("function %q: unknown argument %q", fn.Name(), argName)
		}
		if explicit[argName] {
			return nil, fmt.Errorf(
				"function %q: argument %q provided both positionally and by name",
				fn.Name(), argName)
		}
		resolved[argName] = val
		explicit[argName] = true
	}

	for _, p := range params {
		if p.Required && !explicit[p.Name] {
			return nil, fmt.Errorf(
				"function %q: missing required argument %q",
				fn.Name(), p.Name)
		}
	}

	for _, p := range params {
		if len(p.AcceptedKinds) == 0 {
			continue
		}
		val, ok := resolved[p.Name]
		if !ok {
			continue
		}
		if !slices.Contains(p.AcceptedKinds, val.Kind()) {
			return nil, fmt.Errorf(
				"function %q: argument %q: expected %s, got %s",
				fn.Name(), p.Name,
				joinKinds(p.AcceptedKinds),
				val.Kind(),
			)
		}
	}

	return resolved, nil
}

func joinKinds(kinds []core.ValueKind) string {
	parts := make([]string, len(kinds))
	for i, k := range kinds {
		parts[i] = k.String()
	}
	return strings.Join(parts, " or ")
}
