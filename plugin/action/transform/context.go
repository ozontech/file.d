package transform

type AbortError struct{}

func (AbortError) Error() string {
	return "abort"
}

func IsAbort(err error) bool {
	_, ok := err.(AbortError)
	return ok
}

// Context carries all runtime state available during expression evaluation.
//
// A single Context is created per Program.Run call and passed down through every Eval call.
type Context struct {
	Target   Target
	Registry *Registry
	scope    map[string]Value
}

func NewContext(target Target, registry *Registry) *Context {
	return &Context{
		Target:   target,
		Registry: registry,
		scope:    make(map[string]Value),
	}
}

func (c *Context) GetVar(name string) (Value, bool) {
	v, ok := c.scope[name]
	return v, ok
}

func (c *Context) SetVar(name string, value Value) {
	c.scope[name] = value
}

func (c *Context) DeleteVar(name string) {
	delete(c.scope, name)
}
