package runtime

import (
	"fmt"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/ozontech/file.d/plugin/action/transform/stdlib"
)

// Bound the storage retained after an event with many local variables.
const maxRetainedScopeEntries = 64

// Context carries all runtime state available during expression evaluation.
//
// A Context is passed through every Eval call and must not be used concurrently.
// Reset prepares it for another run with the same target and registry.
type Context struct {
	target        core.Target
	registry      *stdlib.Registry
	scope         map[string]core.Value
	scopePeakSize int
}

func NewContext(target core.Target, registry *stdlib.Registry) *Context {
	return &Context{
		target:   target,
		registry: registry,
	}
}

// Reset releases event-local values while retaining a small scope for reuse.
// Values can reference constants or returned results, so cleanup is shallow.
func (c *Context) Reset() {
	clear(c.scope)
	if c.scopePeakSize > maxRetainedScopeEntries {
		c.scope = nil
	}
	c.scopePeakSize = 0
}

func (c *Context) GetVar(name string) (core.Value, bool) {
	v, ok := c.scope[name]
	return v, ok
}

func (c *Context) SetVar(name string, value core.Value) {
	if c.scope == nil {
		c.scope = make(map[string]core.Value)
	}
	c.scope[name] = value
	c.scopePeakSize = max(c.scopePeakSize, len(c.scope))
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
	resolved, err := fn.Resolve(positional, named)
	if err != nil {
		return core.NullValue{}, fmt.Errorf("%s: %w", pos, err)
	}
	result, err := fn.Call(resolved)
	if err != nil {
		return core.NullValue{}, fmt.Errorf("%s: %s: %w", pos, name, err)
	}
	return result, nil
}
