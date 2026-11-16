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
	calls         core.CallState
}

func NewContext(target core.Target, registry *stdlib.Registry) *Context {
	return &Context{
		target:   target,
		registry: registry,
	}
}

// Reset releases event-local values while retaining a small scope and bounded
// argument storage for reuse. No call may be active during Reset.
// Values can reference constants or returned results, so cleanup is shallow.
func (c *Context) Reset() {
	clear(c.scope)
	if c.scopePeakSize > maxRetainedScopeEntries {
		c.scope = nil
	}
	c.scopePeakSize = 0
	c.calls.Reset()
}

// CallState provides invocation storage owned by this execution, not the shared
// program or registry. The Context must not be reset during an active call.
func (c *Context) CallState() *core.CallState {
	return &c.calls
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

// PlanCall preserves path arguments for unprepared specialized calls. Resolve the
// function against this context's registry without mutating the shared AST.
// Ordinary calls still go through CallFunc, including custom implementations.
func (c *Context) PlanCall(expr *core.CallExpr) (core.CallExecutor, error) {
	fn, ok := c.registry.Get(expr.Name)
	if !ok || !fn.RequiresPreparation() {
		return nil, nil
	}
	if err := fn.ValidateCall(expr.Pos(), expr.Args); err != nil {
		return nil, err
	}
	return fn.PrepareCall(expr.Args), nil
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
