package transform

import (
	"fmt"
	"regexp"
	"time"
)

// Program is the result of compiling a source string.
// Lifecycle:
// source -> Compile -> Program   (once, at startup)
// Program.Run(event) -> Result  (many times, one per event)
type Program struct {
	exprs    []Expr    // compiled AST
	registry *Registry // function registry
	source   string    // original source, kept for error reporting
}

// Holds the outcome of a single Program.Run call
type Result struct {
	// Value is the result of the last expression in the program.
	// NullValue{} when the program is empty or ends with abort.
	Value Value

	// Aborted reports whether the program terminated via an abort expression.
	Aborted bool
}

// Compile lexes and parses source into a Program ready for execution.
// The provided registry determines which built-in functions are available.
// Returns a CompileError if the source contains lexer or parser errors.
func Compile(source string, registry *Registry) (*Program, error) {
	if registry == nil {
		return nil, fmt.Errorf("compile: registry must not be nil")
	}

	lexer, err := NewLexer()
	if err != nil {
		return nil, &CompileError{Phase: "lexer", Source: "", Cause: err}
	}

	tokens, err := lexer.Tokenize(source)
	if err != nil {
		return nil, &CompileError{Phase: "lexer", Source: source, Cause: err}
	}

	exprs, err := NewParser(tokens).Parse()
	if err != nil {
		return nil, &CompileError{Phase: "parser", Source: source, Cause: err}
	}

	if err := validateCalls(exprs, registry); err != nil {
		return nil, &CompileError{Phase: "validation", Source: source, Cause: err}
	}

	return &Program{
		exprs:    exprs,
		registry: registry,
		source:   source,
	}, nil
}

// Run executes the compiled program.
//
// The program evaluates its expressions in order; the value of the last
// expression is returned in Result.Value.
//
// Abort:
//   - An abort expression stops execution immediately.
//   - Result.Aborted is set to true.
//   - The target may have been partially modified before the abort.
func (p *Program) Run(target Target) (Result, error) {
	ctx := NewContext(target, p.registry)

	var last Value = NullValue{}

	for _, expr := range p.exprs {
		val, err := expr.Eval(ctx)
		if err != nil {
			if IsAbort(err) {
				return Result{Value: NullValue{}, Aborted: true}, nil
			}
			return Result{}, fmt.Errorf("runtime error at %s: %w", expr.Pos(), err)
		}
		last = val
	}

	return Result{Value: last}, nil
}

func (p *Program) Source() string {
	return p.source
}

type CompileError struct {
	Phase  string
	Source string
	Cause  error
}

func (e *CompileError) Error() string {
	return fmt.Sprintf("compile error (%s): %s", e.Phase, e.Cause)
}

func (e *CompileError) Unwrap() error {
	return e.Cause
}

// validateCalls walks the AST and checks that every function call refers to
// a function that exists in the registry.
// This is a lightweight static check - argument types are validated at runtime.
func validateCalls(exprs []Expr, registry *Registry) error {
	for _, expr := range exprs {
		if err := validateExpr(expr, registry); err != nil {
			return err
		}
	}
	return nil
}

func validateExpr(expr Expr, registry *Registry) error {
	switch e := expr.(type) {

	case *CallExpr:
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
	case *BinaryExpr:
		if err := validateExpr(e.Left, registry); err != nil {
			return err
		}
		return validateExpr(e.Right, registry)
	case *UnaryExpr:
		return validateExpr(e.Operand, registry)
	case *AssignExpr:
		return validateExpr(e.Value, registry)
	case *IndexExpr:
		if err := validateExpr(e.Object, registry); err != nil {
			return err
		}
		return validateExpr(e.Index, registry)
	case *ArrayExpr:
		for _, el := range e.Elements {
			if err := validateExpr(el, registry); err != nil {
				return err
			}
		}
	case *ObjectExpr:
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
	case *IfExpr:
		if err := validateExpr(e.Condition, registry); err != nil {
			return err
		}
		if err := validateCalls(e.Then, registry); err != nil {
			return err
		}
		return validateCalls(e.Else, registry)
	case *PathExpr:
		for _, seg := range e.Segments {
			if seg.IsIndex() {
				if err := validateExpr(seg.Index, registry); err != nil {
					return err
				}
			}
		}
	case *DelExpr:
		for _, seg := range e.Target.Segments {
			if seg.IsIndex() {
				if err := validateExpr(seg.Index, registry); err != nil {
					return err
				}
			}
		}
	case *ForExpr:
		if err := validateExpr(e.Iter, registry); err != nil {
			return err
		}
		return validateCalls(e.Body, registry)
	case *RegexLit:
		re, err := regexp.Compile(e.Pattern)
		if err != nil {
			return fmt.Errorf("%s: invalid regex pattern %q: %w", e.Pos(), e.Pattern, err)
		}
		e.compiled = re
	case *TimestampLit:
		layouts := []string{
			time.RFC3339Nano,
			time.RFC3339,
			"2006-01-02T15:04:05",
			"2006-01-02",
		}
		for _, layout := range layouts {
			if t, err := time.Parse(layout, e.Value); err == nil {
				e.parsed = t
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
func validateArgs(e *CallExpr, fn Function) error {
	params := fn.Params()

	var positionalCount int
	named := make(map[string]bool)

	for _, arg := range e.Args {
		if arg.Name == "" {
			positionalCount++
			continue
		}
		if named[arg.Name] {
			return fmt.Errorf("%s: function %q: duplicate argument %q",
				e.Pos(), fn.Name(), arg.Name)
		}
		named[arg.Name] = true
		known := false
		for _, p := range params {
			if p.Name == arg.Name {
				known = true
				break
			}
		}
		if !known {
			return fmt.Errorf("%s: function %q: unknown argument %q",
				e.Pos(), fn.Name(), arg.Name)
		}
	}

	if positionalCount > len(params) {
		return fmt.Errorf("%s: function %q: too many arguments: expected at most %d, got %d",
			e.Pos(), fn.Name(), len(params), positionalCount)
	}

	for i, p := range params {
		if !p.Required {
			continue
		}
		if i < positionalCount || named[p.Name] {
			continue
		}
		return fmt.Errorf("%s: function %q: missing required argument %q",
			e.Pos(), fn.Name(), p.Name)
	}

	return nil
}
