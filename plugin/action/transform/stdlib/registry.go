package stdlib

import (
	"fmt"
	"slices"
	"strings"

	"github.com/ozontech/file.d/plugin/action/transform/core"
)

var (
	registry *Registry
)

func init() {
	registry = newRegistry()

	registry.mustRegister(upcase{})
	registry.mustRegister(parseRegex{})
	registry.mustRegister(toString{})
	registry.mustRegister(after{})
	registry.mustRegister(before{})
	registry.mustRegister(between{})
	registry.mustRegister(parseRegexAll{})
	registry.mustRegister(join{})
	registry.mustRegister(trim{})
	registry.mustRegister(trimLeft{})
	registry.mustRegister(trimRight{})
	registry.mustRegister(trimTo{})
	registry.mustRegister(trimToLeft{})
	registry.mustRegister(trimToRight{})
	registry.mustRegister(slice{})
	registry.mustRegister(lookup{})
	registry.mustRegister(exists{})
	registry.mustRegister(rename{})
}

func GetRegistry() *Registry {
	return registry
}

// Describes a single parameter of a built-in function.
//
// Whether a parameter is positional or named is derived from Default,
// mirroring the Python/PHP model:
//
//   - Positional (Default == nil): supplied by position - fn(value) - always
//     required.
//   - Named (Default != nil): supplied by name - fn(name: value) - always
//     optional, falling back to Default when omitted.
//
// All positional parameters must be declared before any named parameter.
type Parameter struct {
	// Name is the parameter name. For named parameters it is the keyword used
	// at the call site: fn(name: value). For positional parameters it is used
	// as the key in the resolved argument map.
	Name string

	// Description documents the parameter for help output and generated docs.
	Description string

	// Default is the value substituted when a named parameter is omitted.
	// Its presence is what makes a parameter named (keyword-only) and optional;
	// a nil Default makes the parameter positional and required.
	Default core.Value

	// AcceptedKinds lists the value kinds this parameter accepts.
	// An empty slice means any kind is accepted.
	AcceptedKinds []core.ValueKind

	// Mode selects value evaluation or path resolution. The zero value keeps
	// ordinary function behavior. Path parameters are required and positional.
	Mode ArgumentMode

	// EventFieldOnly restricts a path parameter to a non-root event path.
	// The resolved leaf and dynamic indices are checked by the target at runtime.
	EventFieldOnly bool
}

type ArgumentMode uint8

const (
	ValueArgument ArgumentMode = iota
	PathArgument
)

// Named reports whether the parameter is keyword-only (bound by name).
func (p *Parameter) Named() bool { return p.Default != nil }

// Required reports whether the parameter must be supplied by the caller.
// Positional parameters are always required; named parameters never are.
func (p *Parameter) Required() bool { return p.Default == nil }

// Callable describes a registered function. Implementations provide exactly
// one execution interface: Function or SpecializedFunction.
type Callable interface {
	Name() string
	Params() []Parameter
}

// Function is the interface for functions that consume only values.
//
// General call path:
//  1. Interpreter evaluates all argument expressions -> positional []Value + named map[string]Value
//  2. CompiledFunc.Resolve validates and maps them to the parameter list -> map[string]Value
//  3. Function.Call receives the resolved map and returns a Value
//
// Prepared calls use the optional indexedFunction entry point (see prepared.go).
type Function interface {
	Callable

	// Call executes the function with fully-resolved, validated arguments.
	// args is keyed by parameter name and always contains every parameter
	// (required args + provided optional args + defaults).
	Call(args map[string]core.Value) (core.Value, error)
}

// SpecializedFunction binds validated expressions to a dedicated executor.
// Use it when evaluation requires paths or other expression-specific behavior.
// Preparation must not capture an event or mutable invocation state.
// Ordinary value functions keep Function and the shared CallPlan implementation.
type SpecializedFunction interface {
	Callable
	PrepareCall(args []core.Argument) core.CallExecutor
}

// CompiledFunc is a Callable paired with its parameter signature precomputed
// once at registration. Argument resolution reads the cached signature instead
// of recomputing it, so nothing about the (static) parameter list is derived
// per call.
type CompiledFunc struct {
	fn            Callable
	valueFunc     Function
	specialized   SpecializedFunction
	params        []Parameter
	numPositional int
	namedSet      map[string]struct{}
	checkKinds    bool
}

func (c *CompiledFunc) Name() string { return c.fn.Name() }

func (c *CompiledFunc) Params() []Parameter { return c.params }

func (c *CompiledFunc) Call(args map[string]core.Value) (core.Value, error) {
	if c.valueFunc == nil {
		return core.NullValue{}, fmt.Errorf("function %q requires call preparation", c.Name())
	}
	return c.valueFunc.Call(args)
}

// RequiresPreparation reports whether evaluated values alone are insufficient.
func (c *CompiledFunc) RequiresPreparation() bool { return c.specialized != nil }

// compile precomputes the parameter signature and validates its shape.
func compile(fn Callable) (*CompiledFunc, error) {
	params := fn.Params()
	valueFunc, isValue := fn.(Function)
	specialized, isSpecialized := fn.(SpecializedFunction)
	if isValue == isSpecialized {
		return nil, fmt.Errorf("function %q must implement exactly one of Function or SpecializedFunction", fn.Name())
	}

	numPositional := 0
	seenNamed := false
	checkKinds := false
	namedSet := make(map[string]struct{})

	for _, p := range params {
		if p.Mode != ValueArgument && p.Mode != PathArgument {
			return nil, fmt.Errorf("function %q: parameter %q has an invalid argument mode", fn.Name(), p.Name)
		}
		if p.Mode == PathArgument && (!isSpecialized || p.Named() || len(p.AcceptedKinds) > 0) {
			return nil, fmt.Errorf("function %q: path parameter %q requires SpecializedFunction, no default and no value kinds", fn.Name(), p.Name)
		}
		if p.EventFieldOnly && p.Mode != PathArgument {
			return nil, fmt.Errorf("function %q: EventFieldOnly requires a path parameter", fn.Name())
		}
		if len(p.AcceptedKinds) > 0 {
			checkKinds = true
		}
		if p.Named() {
			seenNamed = true
			namedSet[p.Name] = struct{}{}
			continue
		}
		// Positional parameter.
		if seenNamed {
			return nil, fmt.Errorf(
				"function %q: positional parameter %q must be declared before named parameters",
				fn.Name(), p.Name)
		}
		numPositional++
	}

	return &CompiledFunc{
		fn:            fn,
		valueFunc:     valueFunc,
		specialized:   specialized,
		params:        params,
		numPositional: numPositional,
		namedSet:      namedSet,
		checkKinds:    checkKinds,
	}, nil
}

// Resolve binds evaluated call arguments to the function's parameters and
// returns them keyed by parameter name.
//
// Binding is strict (Python keyword-only style): positional arguments fill
// positional parameters by position, named arguments fill named parameters by
// name. A positional parameter cannot be supplied by name, and a named
// parameter cannot be supplied positionally.
func (c *CompiledFunc) Resolve(
	positional []core.Value,
	named map[string]core.Value,
) (map[string]core.Value, error) {
	if c.RequiresPreparation() {
		return nil, fmt.Errorf("function %q requires call preparation", c.Name())
	}
	positionalParams := c.params[:c.numPositional]
	namedParams := c.params[c.numPositional:]

	resolved := make(map[string]core.Value, len(c.params))

	// Seed named-parameter defaults (a named parameter always has a non-nil default).
	for _, p := range namedParams {
		resolved[p.Name] = p.Default
	}

	// Bind named arguments by name.
	for argName, val := range named {
		if _, ok := c.namedSet[argName]; ok {
			resolved[argName] = val
			continue
		}
		if containsName(positionalParams, argName) {
			return nil, fmt.Errorf(
				"function %q: argument %q is positional and cannot be passed by name",
				c.fn.Name(), argName)
		}
		return nil, fmt.Errorf("function %q: unknown argument %q", c.fn.Name(), argName)
	}

	// Bind positional arguments by position.
	if len(positional) > len(positionalParams) {
		return nil, fmt.Errorf(
			"function %q: too many arguments: expected at most %d, got %d",
			c.fn.Name(), len(positionalParams), len(positional))
	}
	for i, val := range positional {
		resolved[positionalParams[i].Name] = val
	}
	// Every positional parameter is required.
	if len(positional) < len(positionalParams) {
		return nil, fmt.Errorf(
			"function %q: missing required argument %q",
			c.fn.Name(), positionalParams[len(positional)].Name)
	}

	// Validate accepted kinds. Every parameter is present in resolved by now
	// (positionals are required, named ones are seeded), so no presence check.
	if c.checkKinds {
		for _, p := range c.params {
			if err := c.checkKind(&p, resolved[p.Name]); err != nil {
				return nil, err
			}
		}
	}

	return resolved, nil
}

func (c *CompiledFunc) checkKind(p *Parameter, value core.Value) error {
	if len(p.AcceptedKinds) > 0 && !slices.Contains(p.AcceptedKinds, value.Kind()) {
		return fmt.Errorf("function %q: argument %q: expected %s, got %s",
			c.fn.Name(), p.Name, joinKinds(p.AcceptedKinds), value.Kind())
	}
	return nil
}

func containsName(params []Parameter, name string) bool {
	for _, p := range params {
		if p.Name == name {
			return true
		}
	}
	return false
}

func joinKinds(kinds []core.ValueKind) string {
	parts := make([]string, len(kinds))
	for i, k := range kinds {
		parts[i] = k.String()
	}
	return strings.Join(parts, " or ")
}

// Registry holds all built-in functions available during program execution.
// It is built once at startup and shared across all Program.Run calls.
type Registry struct {
	functions map[string]*CompiledFunc
}

func newRegistry() *Registry {
	return &Registry{
		functions: make(map[string]*CompiledFunc),
	}
}

func (r *Registry) register(fn Callable) error {
	name := fn.Name()
	if _, exists := r.functions[name]; exists {
		return fmt.Errorf("function %q is already registered", name)
	}
	compiled, err := compile(fn)
	if err != nil {
		return err
	}
	r.functions[name] = compiled
	return nil
}

func (r *Registry) mustRegister(fn Callable) {
	if err := r.register(fn); err != nil {
		panic(fmt.Sprintf("transform: %s", err))
	}
}

func (r *Registry) Get(name string) (*CompiledFunc, bool) {
	fn, ok := r.functions[name]
	return fn, ok
}
