package compiler

import (
	"fmt"
	"regexp"
	"time"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/ozontech/file.d/plugin/action/transform/stdlib"
)

// ValidateCalls walks the AST and checks that every function call refers to
// a function that exists in the registry.
// This is a lightweight static check - argument types are validated at runtime
//
// The walk also prepares nodes in place, so it must run before the program is
// evaluated: regex literals are compiled, timestamp literals are parsed, and
// constant sub-expressions are folded into core.ConstExpr (see fold.go).
func ValidateCalls(exprs []core.Expr, registry *stdlib.Registry) error {
	return (&validator{registry: registry}).validateAll(exprs)
}

// Prepare validates and folds a program, then binds supported calls to registry.
// Run it once before publishing the AST; expressions must not change afterward.
// Use ValidateCalls on a separate AST when calls should use EvalContext.CallFunc
// instead, for example with a custom runtime registry.
func Prepare(exprs []core.Expr, registry *stdlib.Registry) error {
	return (&validator{registry: registry, prepareCalls: true}).validateAll(exprs)
}

type validator struct {
	registry     *stdlib.Registry
	prepareCalls bool
}

func (v *validator) validateAll(exprs []core.Expr) error {
	for _, expr := range exprs {
		if err := v.validateExpr(expr); err != nil {
			return err
		}
	}
	return nil
}

func (v *validator) validateExpr(expr core.Expr) error {
	switch e := expr.(type) {
	case *core.CallExpr:
		fn, ok := v.registry.Get(e.Name)
		if !ok {
			return fmt.Errorf("%s: unknown function %q", e.Pos(), e.Name)
		}
		if err := validateArgs(e, fn); err != nil {
			return err
		}
		for _, arg := range e.Args {
			if err := v.validateExpr(arg.Value); err != nil {
				return err
			}
		}
		// Fold after the walk: an argument holding a regex or timestamp literal
		// is only constant once validateExpr has prepared it.
		for i := range e.Args {
			e.Args[i].Value = tryFold(e.Args[i].Value)
		}
		if v.prepareCalls {
			e.Plan = fn.PrepareCall(e.Args)
		}
	case *core.BinaryExpr:
		if err := v.validateExpr(e.Left); err != nil {
			return err
		}
		return v.validateExpr(e.Right)
	case *core.UnaryExpr:
		return v.validateExpr(e.Operand)
	case *core.AssignExpr:
		if err := v.validateExpr(e.Value); err != nil {
			return err
		}
		// Folding the right-hand side keeps `table = { ... }` at the top of a
		// program from rebuilding the object on every event.
		e.Value = tryFold(e.Value)
	case *core.IndexExpr:
		if err := v.validateExpr(e.Object); err != nil {
			return err
		}
		return v.validateExpr(e.Index)
	case *core.ArrayExpr:
		for _, el := range e.Elements {
			if err := v.validateExpr(el); err != nil {
				return err
			}
		}
	case *core.ObjectExpr:
		seen := make(map[string]bool, len(e.Pairs))
		for _, kv := range e.Pairs {
			if seen[kv.Key] {
				return fmt.Errorf("%s: duplicate object key %q", e.Pos(), kv.Key)
			}
			seen[kv.Key] = true
			if err := v.validateExpr(kv.Value); err != nil {
				return err
			}
		}
	case *core.IfExpr:
		if err := v.validateExpr(e.Condition); err != nil {
			return err
		}
		if err := v.validateAll(e.Then); err != nil {
			return err
		}
		return v.validateAll(e.Else)
	case *core.PathExpr:
		for _, seg := range e.Segments {
			if seg.IsIndex() {
				if err := v.validateExpr(seg.Index); err != nil {
					return err
				}
			}
		}
	case *core.DelExpr:
		for _, seg := range e.Target.Segments {
			if seg.IsIndex() {
				if err := v.validateExpr(seg.Index); err != nil {
					return err
				}
			}
		}
	case *core.ForExpr:
		if err := v.validateExpr(e.Iter); err != nil {
			return err
		}
		return v.validateAll(e.Body)
	case *core.RegexLit:
		re, err := regexp.Compile(e.Pattern)
		if err != nil {
			return fmt.Errorf("%s: invalid regex pattern %q: %w", e.Pos(), e.Pattern, err)
		}
		e.Compiled = re
	case *core.TimestampLit:
		layouts := []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02T15:04:05",
			"2006-01-02",
		}
		for _, layout := range layouts {
			if t, err := time.Parse(layout, e.Value); err == nil {
				e.Parsed = t
				return nil
			}
		}
		return fmt.Errorf("%s: cannot parse %q as a timestamp", e.Pos(), e.Value)
	}

	return nil
}

// validateArgs statically checks argument structure against the function's
// parameter list. Only structural issues are checked here — value types
// are validated at runtime since arguments are arbitrary expressions.
//
// Binding is strict: positional arguments fill positional parameters and named
// arguments fill named parameters (see stdlib.CompiledFunc.Resolve).
func validateArgs(e *core.CallExpr, fn stdlib.Function) error {
	params := fn.Params()

	// Positional parameters always precede named ones (enforced at registration).
	numPositional := 0
	for _, p := range params {
		if p.Named() {
			break
		}
		numPositional++
	}
	positionalParams := params[:numPositional]
	namedParams := params[numPositional:]

	var positionalCount int
	seen := make(map[string]bool)

	for _, arg := range e.Args {
		if arg.Name == "" {
			positionalCount++
			continue
		}
		if seen[arg.Name] {
			return fmt.Errorf("%s: function %q: duplicate argument %q",
				e.Pos(), fn.Name(), arg.Name)
		}
		seen[arg.Name] = true

		if paramByName(namedParams, arg.Name) {
			continue
		}
		if paramByName(positionalParams, arg.Name) {
			return fmt.Errorf("%s: function %q: argument %q is positional and cannot be passed by name",
				e.Pos(), fn.Name(), arg.Name)
		}
		return fmt.Errorf("%s: function %q: unknown argument %q",
			e.Pos(), fn.Name(), arg.Name)
	}

	if positionalCount > len(positionalParams) {
		return fmt.Errorf("%s: function %q: too many arguments: expected at most %d, got %d",
			e.Pos(), fn.Name(), len(positionalParams), positionalCount)
	}
	// Every positional parameter is required.
	if positionalCount < len(positionalParams) {
		return fmt.Errorf("%s: function %q: missing required argument %q",
			e.Pos(), fn.Name(), positionalParams[positionalCount].Name)
	}

	return nil
}

func paramByName(params []stdlib.Parameter, name string) bool {
	for _, p := range params {
		if p.Name == name {
			return true
		}
	}
	return false
}
