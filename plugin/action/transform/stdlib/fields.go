package stdlib

import (
	"fmt"

	"github.com/ozontech/file.d/plugin/action/transform/core"
)

type exists struct{}

func (exists) Name() string { return "exists" }

func (exists) Params() []Parameter {
	return []Parameter{
		{
			Name: "path",
			Mode: PathArgument,
		},
	}
}

func (exists) PrepareCall(args []core.Argument) core.CallExecutor {
	return &existsCall{path: args[0].Value.(*core.PathExpr)}
}

type existsCall struct {
	path *core.PathExpr
}

func (c *existsCall) EvalCall(ctx core.EvalContext, expr *core.CallExpr) (core.Value, error) {
	path, err := c.path.ResolvePath(ctx)
	if err != nil {
		return core.NullValue{}, err
	}
	found, err := ctx.GetTarget().Exists(path)
	if err != nil {
		return core.NullValue{}, fmt.Errorf("%s: exists: %w", expr.Pos(), err)
	}
	return core.BoolValue{V: found}, nil
}

type rename struct{}

func (rename) Name() string { return "rename" }

func (rename) Params() []Parameter {
	return []Parameter{
		{
			Name:           "source",
			Mode:           PathArgument,
			EventFieldOnly: true,
		},
		{
			Name:           "destination",
			Mode:           PathArgument,
			EventFieldOnly: true,
		},
		{
			Name:          "override",
			Default:       core.BoolValue{V: false},
			AcceptedKinds: []core.ValueKind{core.KindBool},
		},
	}
}

func (rename) PrepareCall(args []core.Argument) core.CallExecutor {
	call := &renameCall{overrideAt: -1}
	for i, arg := range args {
		switch {
		case arg.Name != "":
			// Folded bools have no effects or errors to preserve at runtime.
			if constant, ok := arg.Value.(*core.ConstExpr); ok {
				if flag, ok := constant.V.(core.BoolValue); ok {
					call.defaultOverride = flag.V
					continue
				}
			}
			call.override = arg.Value
			call.overrideAt = i
		case call.source == nil:
			call.source = arg.Value.(*core.PathExpr)
		default:
			call.destination = arg.Value.(*core.PathExpr)
		}
	}
	return call
}

type renameCall struct {
	source          *core.PathExpr
	destination     *core.PathExpr
	override        core.Expr
	overrideAt      int
	defaultOverride bool
}

func (c *renameCall) EvalCall(ctx core.EvalContext, expr *core.CallExpr) (core.Value, error) {
	override := c.defaultOverride
	kind := core.KindBool
	var err error
	if c.overrideAt == 0 {
		override, kind, err = c.evalOverride(ctx)
		if err != nil {
			return core.NullValue{}, err
		}
	}
	source, err := c.source.ResolvePath(ctx)
	if err != nil {
		return core.NullValue{}, err
	}
	if c.overrideAt == 1 {
		override, kind, err = c.evalOverride(ctx)
		if err != nil {
			return core.NullValue{}, err
		}
	}
	destination, err := c.destination.ResolvePath(ctx)
	if err != nil {
		return core.NullValue{}, err
	}
	if c.overrideAt == 2 {
		override, kind, err = c.evalOverride(ctx)
		if err != nil {
			return core.NullValue{}, err
		}
	}
	// Match ordinary calls: argument evaluation errors precede kind errors.
	if kind != core.KindBool {
		return core.NullValue{}, fmt.Errorf("%s: function %q: argument %q: expected bool, got %s",
			expr.Pos(), "rename", "override", kind)
	}
	moved, err := ctx.GetTarget().Rename(source, destination, override)
	if err != nil {
		return core.NullValue{}, fmt.Errorf("%s: rename: %w", expr.Pos(), err)
	}
	return core.BoolValue{V: moved}, nil
}

// Capture the bool or invalid kind before a later index can mutate its JSON node.
// No other value is needed, and type errors are reported after all arguments.
func (c *renameCall) evalOverride(ctx core.EvalContext) (bool, core.ValueKind, error) {
	value, err := c.override.Eval(ctx)
	if err != nil {
		return false, core.KindNull, err
	}
	kind := value.Kind()
	if kind != core.KindBool {
		return false, kind, nil
	}
	return value.AsBool(), kind, nil
}
