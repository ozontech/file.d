package runtime

import (
	"errors"
	"fmt"

	"github.com/ozontech/file.d/plugin/action/transform/compiler"
	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/ozontech/file.d/plugin/action/transform/parser"
	"github.com/timtadh/lexmachine"
)

// Program is the result of compiling a source string.
// Lifecycle:
// source -> Compile -> Program   (once, at startup)
// Program.Run(event) -> Result  (many times, one per event)
type Program struct {
	exprs    []core.Expr    // compiled AST
	registry *core.Registry // function registry
	source   string         // original source, kept for error reporting
}

// Holds the outcome of a single Program.Run call
type Result struct {
	// Value is the result of the last expression in the program.
	// NullValue{} when the program is empty or ends with abort.
	Value core.Value

	// Aborted reports whether the program terminated via an abort expression.
	Aborted bool
}

// Compile parses and compiles source into a Program ready for execution.
// The provided registry determines which built-in functions are available.
// Returns a CompileError if the source contains parser or compiler errors.
func Compile(source string, registry *core.Registry, lexer *lexmachine.Lexer) (*Program, error) {
	if registry == nil {
		return nil, fmt.Errorf("compile: registry must not be nil")
	}

	parser := parser.NewParser(lexer)
	tokens, err := parser.Parse(source)
	if err != nil {
		return nil, &CompileError{Phase: "parsing", Source: source, Cause: err}
	}

	exprs, err := compiler.NewCompiler(tokens).Compile()
	if err != nil {
		return nil, &CompileError{Phase: "compilation", Source: source, Cause: err}
	}

	if err := compiler.ValidateCalls(exprs, registry); err != nil {
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
func (p *Program) Run(target core.Target) (Result, error) {
	ctx := NewContext(target, p.registry)

	var last core.Value = core.NullValue{}

	for _, expr := range p.exprs {
		val, err := expr.Eval(ctx)
		if err != nil {
			if errors.Is(err, core.AbortError) {
				return Result{Value: core.NullValue{}, Aborted: true}, nil
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
