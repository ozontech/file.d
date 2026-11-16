package stdlib

import (
	"errors"
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/stretchr/testify/require"
)

type specializedTestContext struct {
	core.EvalContext
	target core.Target
}

func (c *specializedTestContext) GetTarget() core.Target { return c.target }

type fieldTestTarget struct {
	core.Target
	paths    []core.Path
	override bool
	err      error
}

func (t *fieldTestTarget) Exists(path core.Path) (bool, error) {
	t.paths = []core.Path{path}
	return true, t.err
}

func (t *fieldTestTarget) Rename(source, destination core.Path, override bool) (bool, error) {
	t.paths = []core.Path{source, destination}
	t.override = override
	return true, t.err
}

func registeredFunctionNames(specialized bool) []string {
	var names []string
	for name, fn := range GetRegistry().functions {
		if fn.RequiresPreparation() == specialized {
			names = append(names, name)
		}
	}
	return names
}

func TestRegisteredSpecializedFunctionsPrepareCalls(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name  string
		paths []string
	}
	tests := []testCase{
		{
			name:  "exists",
			paths: []string{"source"},
		},
		{
			name:  "rename",
			paths: []string{"source", "destination"},
		},
	}
	names := make([]string, len(tests))
	for i, tt := range tests {
		names[i] = tt.name
	}
	require.ElementsMatch(t, registeredFunctionNames(true), names)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			fn, ok := GetRegistry().Get(tt.name)
			require.True(t, ok)
			args := make([]core.Argument, len(tt.paths))
			wantPaths := make([]core.Path, len(tt.paths))
			for i, field := range tt.paths {
				args[i].Value = &core.PathExpr{Segments: []core.PathSegment{{Field: field}}}
				wantPaths[i] = core.Path{Segments: []core.Segment{core.FieldSeg(field)}}
			}
			require.NoError(t, fn.ValidateCall(nil, args))
			expr := &core.CallExpr{Args: args, Plan: fn.PrepareCall(args)}
			require.NotNil(t, expr.Plan)
			for range 2 {
				// The same plan must always use the current execution's event.
				target := &fieldTestTarget{}
				ctx := &specializedTestContext{target: target}
				prepared, err := expr.Eval(ctx)
				require.NoError(t, err)
				require.Equal(t, core.BoolValue{V: true}, prepared)
				require.Equal(t, wantPaths, target.paths)
				require.False(t, target.override)
			}
			failure := errors.New("target failed")
			_, err := expr.Eval(&specializedTestContext{target: &fieldTestTarget{err: failure}})
			require.ErrorIs(t, err, failure)
			_, err = fn.Resolve(nil, nil)
			require.ErrorContains(t, err, "requires call preparation")
			_, err = fn.Call(nil)
			require.ErrorContains(t, err, "requires call preparation")
		})
	}
}

type specializedProbe struct {
	name   string
	params []Parameter
	plan   core.CallExecutor
}

func (f *specializedProbe) Name() string                                    { return f.name }
func (f *specializedProbe) Params() []Parameter                             { return f.params }
func (f *specializedProbe) PrepareCall(_ []core.Argument) core.CallExecutor { return f.plan }

func TestRegistryUsesSpecializedExecutor(t *testing.T) {
	t.Parallel()

	args := []core.Argument{
		{
			Value: &core.PathExpr{Segments: []core.PathSegment{{Field: "field"}}},
		},
	}
	probe := &specializedProbe{
		name:   "custom_function",
		params: (exists{}).Params(),
		plan:   (exists{}).PrepareCall(args),
	}
	registry := newRegistry()
	require.NoError(t, registry.register(probe))
	fn, ok := registry.Get(probe.Name())
	require.True(t, ok)
	require.NoError(t, fn.ValidateCall(nil, args))
	expr := &core.CallExpr{Args: args, Plan: fn.PrepareCall(args)}
	require.Same(t, probe.plan, expr.Plan)
	result, err := expr.Eval(&specializedTestContext{target: &fieldTestTarget{}})
	require.NoError(t, err)
	require.Equal(t, core.BoolValue{V: true}, result)
}

type ambiguousFunction struct {
	*mockFn
	SpecializedFunction
}

// Resolve the embedded descriptor ambiguity, but keep both execution methods.
func (f *ambiguousFunction) Name() string        { return f.mockFn.Name() }
func (f *ambiguousFunction) Params() []Parameter { return f.mockFn.Params() }

func TestRegisterRejectsInvalidArgumentModes(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name      string
		param     Parameter
		valueOnly bool
		err       string
	}
	tests := []testCase{
		{
			name: "path in value function",
			param: Parameter{
				Name: "path",
				Mode: PathArgument,
			},
			valueOnly: true,
			err:       "requires SpecializedFunction",
		},
		{
			name: "default path",
			param: Parameter{
				Name:    "path",
				Mode:    PathArgument,
				Default: core.NullValue{},
			},
			err: "no default",
		},
		{
			name: "path with value kinds",
			param: Parameter{
				Name:          "path",
				Mode:          PathArgument,
				AcceptedKinds: []core.ValueKind{core.KindString},
			},
			err: "no value kinds",
		},
		{
			name: "value restricted to event fields",
			param: Parameter{
				Name:           "value",
				EventFieldOnly: true,
			},
			err: "EventFieldOnly requires a path parameter",
		},
		{
			name: "unknown mode",
			param: Parameter{
				Name: "value",
				Mode: ArgumentMode(255),
			},
			err: "invalid argument mode",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var fn Callable = &specializedProbe{params: []Parameter{tt.param}}
			if tt.valueOnly {
				fn = &mockFn{params: []Parameter{tt.param}}
			}
			require.ErrorContains(t, newRegistry().register(fn), tt.err)
		})
	}
	ambiguous := &ambiguousFunction{mockFn: &mockFn{}}
	require.ErrorContains(t, newRegistry().register(ambiguous), "exactly one")
}
