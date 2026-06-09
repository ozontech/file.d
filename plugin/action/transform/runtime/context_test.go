package runtime

import (
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/ozontech/file.d/plugin/action/transform/stdlib"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockFn struct {
	name   string
	params []stdlib.Parameter
	callFn func(args map[string]core.Value) (core.Value, error)
}

func (f *mockFn) Name() string               { return f.name }
func (f *mockFn) Params() []stdlib.Parameter { return f.params }
func (f *mockFn) Call(args map[string]core.Value) (core.Value, error) {
	if f.callFn != nil {
		return f.callFn(args)
	}
	return core.NullValue{}, nil
}

func mkFn(name string, params ...stdlib.Parameter) stdlib.Function {
	return &mockFn{name: name, params: params}
}

func TestContextVarOps(t *testing.T) {
	t.Parallel()

	t.Run("get_missing_returns_false", func(t *testing.T) {
		t.Parallel()

		ctx := NewContext(nil, nil)
		_, ok := ctx.GetVar("undefined")
		assert.False(t, ok)
	})

	t.Run("set_and_get", func(t *testing.T) {
		t.Parallel()

		ctx := NewContext(nil, nil)
		ctx.SetVar("x", core.IntegerValue{V: 42})
		v, ok := ctx.GetVar("x")
		require.True(t, ok)
		assert.Equal(t, core.IntegerValue{V: 42}, v)
	})

	t.Run("overwrite_keeps_last_value", func(t *testing.T) {
		t.Parallel()

		ctx := NewContext(nil, nil)
		ctx.SetVar("y", core.StringValue{V: "first"})
		ctx.SetVar("y", core.StringValue{V: "second"})
		v, ok := ctx.GetVar("y")
		require.True(t, ok)
		assert.Equal(t, core.StringValue{V: "second"}, v)
	})

	t.Run("delete_removes_var", func(t *testing.T) {
		t.Parallel()

		ctx := NewContext(nil, nil)
		ctx.SetVar("z", core.BoolValue{V: true})
		ctx.DeleteVar("z")
		_, ok := ctx.GetVar("z")
		assert.False(t, ok)
	})

	t.Run("delete_missing_is_noop", func(t *testing.T) {
		t.Parallel()

		ctx := NewContext(nil, nil)
		assert.NotPanics(t, func() {
			ctx.DeleteVar("never_existed")
		})
	})
}

func TestContextGetTarget(t *testing.T) {
	t.Parallel()

	root := insaneJSON.Spawn()
	defer insaneJSON.Release(root)
	require.NoError(t, root.DecodeString(`{}`))

	target := NewRootTarget(root, "test.log", map[string]string{})
	ctx := NewContext(target, nil)

	assert.Equal(t, target, ctx.GetTarget())
}

func TestJoinKinds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		kinds []core.ValueKind
		want  string
	}{
		{nil, ""},
		{[]core.ValueKind{core.KindString}, "string"},
		{[]core.ValueKind{core.KindString, core.KindInteger}, "string or integer"},
		{[]core.ValueKind{core.KindNull, core.KindBool, core.KindFloat}, "null or bool or float"},
	}
	for _, tc := range tests {
		t.Run(tc.want, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, joinKinds(tc.kinds))
		})
	}
}

func TestResolveFunctionArgs(t *testing.T) {
	t.Parallel()

	t.Run("positional_args_mapped_in_order", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("add",
			stdlib.Parameter{Name: "a", Required: true},
			stdlib.Parameter{Name: "b", Required: true},
		)
		resolved, err := ResolveFunctionArgs(fn,
			[]core.Value{core.IntegerValue{V: 1}, core.IntegerValue{V: 2}},
			nil,
		)
		require.NoError(t, err)
		assert.Equal(t, core.IntegerValue{V: 1}, resolved["a"])
		assert.Equal(t, core.IntegerValue{V: 2}, resolved["b"])
	})

	t.Run("named_args_mapped_by_name", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("fn",
			stdlib.Parameter{Name: "x"},
			stdlib.Parameter{Name: "y"},
		)
		resolved, err := ResolveFunctionArgs(fn,
			nil,
			map[string]core.Value{
				"y": core.IntegerValue{V: 20},
				"x": core.IntegerValue{V: 10},
			},
		)
		require.NoError(t, err)
		assert.Equal(t, core.IntegerValue{V: 10}, resolved["x"])
		assert.Equal(t, core.IntegerValue{V: 20}, resolved["y"])
	})

	t.Run("default_used_when_param_not_provided", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("fn",
			stdlib.Parameter{Name: "sep", Default: core.StringValue{V: ","}},
		)
		resolved, err := ResolveFunctionArgs(fn, nil, nil)
		require.NoError(t, err)
		assert.Equal(t, core.StringValue{V: ","}, resolved["sep"])
	})

	t.Run("positional_arg_overrides_default", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("fn",
			stdlib.Parameter{Name: "sep", Default: core.StringValue{V: ","}},
		)
		resolved, err := ResolveFunctionArgs(fn,
			[]core.Value{core.StringValue{V: ";"}},
			nil,
		)
		require.NoError(t, err)
		assert.Equal(t, core.StringValue{V: ";"}, resolved["sep"])
	})

	t.Run("named_arg_overrides_default", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("fn",
			stdlib.Parameter{Name: "sep", Default: core.StringValue{V: ","}},
		)
		resolved, err := ResolveFunctionArgs(fn,
			nil,
			map[string]core.Value{"sep": core.StringValue{V: "|"}},
		)
		require.NoError(t, err)
		assert.Equal(t, core.StringValue{V: "|"}, resolved["sep"])
	})

	t.Run("optional_param_absent_not_in_resolved", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("fn",
			stdlib.Parameter{Name: "required", Required: true},
			stdlib.Parameter{Name: "optional", Required: false},
		)
		resolved, err := ResolveFunctionArgs(fn,
			[]core.Value{core.IntegerValue{V: 1}},
			nil,
		)
		require.NoError(t, err)
		_, has := resolved["optional"]
		assert.False(t, has, "optional param without default must not appear in resolved")
	})

	t.Run("no_args_no_params_ok", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("noop")
		resolved, err := ResolveFunctionArgs(fn, nil, nil)
		require.NoError(t, err)
		assert.Empty(t, resolved)
	})

	t.Run("too_many_positional_error", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("fn", stdlib.Parameter{Name: "x"})
		_, err := ResolveFunctionArgs(fn,
			[]core.Value{core.IntegerValue{V: 1}, core.IntegerValue{V: 2}},
			nil,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "too many arguments")
	})

	t.Run("unknown_named_arg_error", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("fn", stdlib.Parameter{Name: "x"})
		_, err := ResolveFunctionArgs(fn,
			nil,
			map[string]core.Value{"unknown": core.IntegerValue{V: 1}},
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown argument")
		assert.Contains(t, err.Error(), `"unknown"`)
	})

	t.Run("arg_provided_both_positionally_and_by_name_error", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("fn", stdlib.Parameter{Name: "x"})
		_, err := ResolveFunctionArgs(fn,
			[]core.Value{core.IntegerValue{V: 1}},
			map[string]core.Value{"x": core.IntegerValue{V: 2}},
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "provided both positionally and by name")
	})

	t.Run("missing_required_arg_error", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("fn", stdlib.Parameter{Name: "x", Required: true})
		_, err := ResolveFunctionArgs(fn, nil, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing required argument")
		assert.Contains(t, err.Error(), `"x"`)
	})

	t.Run("required_param_covered_by_positional_ok", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("fn", stdlib.Parameter{Name: "x", Required: true})
		resolved, err := ResolveFunctionArgs(fn,
			[]core.Value{core.StringValue{V: "v"}},
			nil,
		)
		require.NoError(t, err)
		assert.Equal(t, core.StringValue{V: "v"}, resolved["x"])
	})

	t.Run("wrong_kind_single_accepted_error", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("fn",
			stdlib.Parameter{Name: "s", AcceptedKinds: []core.ValueKind{core.KindString}},
		)
		_, err := ResolveFunctionArgs(fn,
			[]core.Value{core.IntegerValue{V: 42}},
			nil,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected string")
		assert.Contains(t, err.Error(), "got integer")
	})

	t.Run("wrong_kind_multiple_accepted_error", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("fn",
			stdlib.Parameter{Name: "v", AcceptedKinds: []core.ValueKind{core.KindString, core.KindInteger}},
		)
		_, err := ResolveFunctionArgs(fn,
			[]core.Value{core.BoolValue{V: true}},
			nil,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "expected string or integer")
		assert.Contains(t, err.Error(), "got bool")
	})

	t.Run("correct_kind_passes", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("fn",
			stdlib.Parameter{Name: "s", AcceptedKinds: []core.ValueKind{core.KindString}},
		)
		resolved, err := ResolveFunctionArgs(fn,
			[]core.Value{core.StringValue{V: "hello"}},
			nil,
		)
		require.NoError(t, err)
		assert.Equal(t, core.StringValue{V: "hello"}, resolved["s"])
	})

	t.Run("one_of_multiple_accepted_kinds_passes", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("fn",
			stdlib.Parameter{Name: "v", AcceptedKinds: []core.ValueKind{core.KindInteger, core.KindFloat}},
		)
		resolved, err := ResolveFunctionArgs(fn,
			[]core.Value{core.FloatValue{V: 3.14}},
			nil,
		)
		require.NoError(t, err)
		assert.Equal(t, core.FloatValue{V: 3.14}, resolved["v"])
	})

	t.Run("empty_accepted_kinds_allows_any_type", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("fn", stdlib.Parameter{Name: "v"})
		for _, val := range []core.Value{
			core.IntegerValue{V: 1},
			core.StringValue{V: "x"},
			core.BoolValue{V: true},
			core.NullValue{},
		} {
			_, err := ResolveFunctionArgs(fn, []core.Value{val}, nil)
			assert.NoError(t, err, "should accept %s", val.Kind())
		}
	})

	t.Run("kind_check_skipped_when_optional_param_absent", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("fn",
			stdlib.Parameter{
				Name:          "opt",
				Required:      false,
				AcceptedKinds: []core.ValueKind{core.KindString},
			},
		)
		_, err := ResolveFunctionArgs(fn, nil, nil)
		require.NoError(t, err)
	})

	t.Run("error_message_includes_function_name", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("my_func", stdlib.Parameter{Name: "x", Required: true})
		_, err := ResolveFunctionArgs(fn, nil, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "my_func")
		assert.Contains(t, err.Error(), `"x"`)
	})
}

func TestContextCallFuncUnknown(t *testing.T) {
	t.Parallel()

	root := insaneJSON.Spawn()
	defer insaneJSON.Release(root)
	_ = root.DecodeString(`{}`)

	reg := stdlib.GetRegistry()
	target := NewRootTarget(root, "test", map[string]string{})
	ctx := NewContext(target, reg)

	pos := mockPosition{"1:1"}
	_, err := ctx.CallFunc(pos, "nonexistent_fn____", nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown function")
}

type mockPosition struct{ s string }

func (p mockPosition) String() string { return p.s }
