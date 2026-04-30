package transform

import (
	"fmt"
	"strings"
)

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
	Default Value

	// AcceptedKinds lists the value kinds this parameter accepts.
	// An empty slice means any kind is accepted.
	AcceptedKinds []ValueKind
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
	Call(args map[string]Value) (Value, error)
}

// Registry holds all built-in functions available during program execution.
// It is built once at startup and shared across all Program.Run calls.
type Registry struct {
	functions map[string]Function
}

func NewRegistry() *Registry {
	return &Registry{
		functions: make(map[string]Function),
	}
}

func (r *Registry) Register(fn Function) error {
	name := fn.Name()
	if _, exists := r.functions[name]; exists {
		return fmt.Errorf("function %q is already registered", name)
	}
	r.functions[name] = fn
	return nil
}

func (r *Registry) MustRegister(fn Function) {
	if err := r.Register(fn); err != nil {
		panic(fmt.Sprintf("transform: %s", err))
	}
}

func (r *Registry) Get(name string) (Function, bool) {
	fn, ok := r.functions[name]
	return fn, ok
}

// Maps evaluated argument values to the function's parameter map.
func (r *Registry) ResolveArgs(
	fn Function,
	positional []Value,
	named map[string]Value,
) (map[string]Value, error) {
	params := fn.Params()

	if len(positional) > len(params) {
		return nil, fmt.Errorf(
			"function %q: too many arguments: expected at most %d, got %d",
			fn.Name(), len(params), len(positional),
		)
	}

	resolved := make(map[string]Value, len(params))
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
		if !r.isKnownParam(params, argName) {
			return nil, fmt.Errorf(
				"function %q: unknown argument %q", fn.Name(), argName)
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
		if !kindAccepted(val.Kind(), p.AcceptedKinds) {
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

func (r *Registry) isKnownParam(params []Parameter, name string) bool {
	for _, p := range params {
		if p.Name == name {
			return true
		}
	}
	return false
}

func kindAccepted(k ValueKind, accepted []ValueKind) bool {
	for _, a := range accepted {
		if k == a {
			return true
		}
	}
	return false
}

func joinKinds(kinds []ValueKind) string {
	parts := make([]string, len(kinds))
	for i, k := range kinds {
		parts[i] = k.String()
	}
	return strings.Join(parts, " or ")
}
