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
func ValidateCalls(exprs []core.Expr, registry *stdlib.Registry) error {
	for _, expr := range exprs {
		if err := validateExpr(expr, registry); err != nil {
			return err
		}
	}
	return nil
}

func validateExpr(expr core.Expr, registry *stdlib.Registry) error {
	switch e := expr.(type) {
	case *core.CallExpr:
		fn, ok := registry.Get(e.Name)
		if !ok {
			return fmt.Errorf("%s: unknown function %q", e.Pos(), e.Name)
		}
		if err := validateArgs(e, fn); err != nil {
			return err
		}
		for _, arg := range e.Args {
			if err := validateExpr(arg.Value, registry); err != nil {
				return err
			}
		}
	case *core.BinaryExpr:
		if err := validateExpr(e.Left, registry); err != nil {
			return err
		}
		return validateExpr(e.Right, registry)
	case *core.UnaryExpr:
		return validateExpr(e.Operand, registry)
	case *core.AssignExpr:
		return validateExpr(e.Value, registry)
	case *core.IndexExpr:
		if err := validateExpr(e.Object, registry); err != nil {
			return err
		}
		return validateExpr(e.Index, registry)
	case *core.ArrayExpr:
		for _, el := range e.Elements {
			if err := validateExpr(el, registry); err != nil {
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
			if err := validateExpr(kv.Value, registry); err != nil {
				return err
			}
		}
	case *core.IfExpr:
		if err := validateExpr(e.Condition, registry); err != nil {
			return err
		}
		if err := ValidateCalls(e.Then, registry); err != nil {
			return err
		}
		return ValidateCalls(e.Else, registry)
	case *core.PathExpr:
		for _, seg := range e.Segments {
			if seg.IsIndex() {
				if err := validateExpr(seg.Index, registry); err != nil {
					return err
				}
			}
		}
	case *core.DelExpr:
		for _, seg := range e.Target.Segments {
			if seg.IsIndex() {
				if err := validateExpr(seg.Index, registry); err != nil {
					return err
				}
			}
		}
	case *core.ForExpr:
		if err := validateExpr(e.Iter, registry); err != nil {
			return err
		}
		return ValidateCalls(e.Body, registry)
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
// arguments fill named parameters (see runtime.ResolveFunctionArgs).
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
