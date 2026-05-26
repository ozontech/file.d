package stdlib

import (
	"fmt"

	"github.com/ozontech/file.d/plugin/action/transform/core"
)

var (
	registry *Registry
)

func init() {
	registry = newRegistry()

	registry.mustRegister(upcase{})
}

func GetRegistry() *Registry {
	return registry
}

// Describes a single parameter of a built-in function.
type Parameter struct {
	// Name is the parameter name as used in named calls: fn(name: value).
	Name string

	// Required - if true the caller must provide this argument.
	// If false and the argument is omitted, Default is used.
	Required bool

	// Default is the value used when the parameter is optional and not provided.
	// A nil interface value means "no default" (only valid when Required is false
	// and the function handles the missing case itself).
	Default core.Value

	// AcceptedKinds lists the value kinds this parameter accepts.
	// An empty slice means any kind is accepted.
	AcceptedKinds []core.ValueKind
}

// Function is the interface every built-in function must implement.
//
// Lifecycle during a call:
//  1. Interpreter evaluates all argument expressions -> positional []Value + named map[string]Value
//  2. Registry.ResolveArgs validates and maps them to the parameter list -> map[string]Value
//  3. Function.Call receives the resolved map and returns a Value
type Function interface {
	// Returns the function name as it appears in source code.
	Name() string

	// Returns the ordered list of parameter descriptors.
	// Order matters for positional argument binding.
	Params() []Parameter

	// Call executes the function with fully-resolved, validated arguments.
	// args is keyed by parameter name and always contains every parameter
	// that has a value (required args + provided optional args + defaults).
	Call(args map[string]core.Value) (core.Value, error)
}

// Registry holds all built-in functions available during program execution.
// It is built once at startup and shared across all Program.Run calls.
type Registry struct {
	functions map[string]Function
}

func newRegistry() *Registry {
	return &Registry{
		functions: make(map[string]Function),
	}
}

func (r *Registry) register(fn Function) error {
	name := fn.Name()
	if _, exists := r.functions[name]; exists {
		return fmt.Errorf("function %q is already registered", name)
	}
	r.functions[name] = fn
	return nil
}

func (r *Registry) mustRegister(fn Function) {
	if err := r.register(fn); err != nil {
		panic(fmt.Sprintf("transform: %s", err))
	}
}

func (r *Registry) Get(name string) (Function, bool) {
	fn, ok := r.functions[name]
	return fn, ok
}
