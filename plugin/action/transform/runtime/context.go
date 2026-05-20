package runtime

import (
	"fmt"

	"github.com/ozontech/file.d/plugin/action/transform/core"
)

// Context carries all runtime state available during expression evaluation.
//
// A single Context is created per Program.Run call and passed down through every Eval call.
type Context struct {
	target   core.Target
	registry *core.Registry
	scope    map[string]core.Value
}

func NewContext(target core.Target, registry *core.Registry) *Context {
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
	resolved, err := c.registry.ResolveArgs(fn, positional, named)
	if err != nil {
		return core.NullValue{}, nil
	}
	result, err := fn.Call(resolved)
	if err != nil {
		return core.NullValue{}, nil
	}
	return result, nil
}
