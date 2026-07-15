package stdlib

import (
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockFn struct {
	name   string
	params []Parameter
	callFn func(args map[string]core.Value) (core.Value, error)
}

func (f *mockFn) Name() string        { return f.name }
func (f *mockFn) Params() []Parameter { return f.params }
func (f *mockFn) Call(args map[string]core.Value) (core.Value, error) {
	if f.callFn != nil {
		return f.callFn(args)
	}
	return core.NullValue{}, nil
}

func mkFn(name string, params ...Parameter) Function {
	return &mockFn{name: name, params: params}
}

// resolveArgs compiles fn's signature and resolves the given arguments.
func resolveArgs(t *testing.T, fn Function, positional []core.Value, named map[string]core.Value) (map[string]core.Value, error) {
	t.Helper()
	c, err := compile(fn)
	require.NoError(t, err)
	return c.Resolve(positional, named)
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

func TestCompileRejectsPositionalAfterNamed(t *testing.T) {
	t.Parallel()

	fn := mkFn("fn",
		Parameter{Name: "opt", Default: core.NullValue{}},
		Parameter{Name: "pos"},
	)
	_, err := compile(fn)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must be declared before named parameters")
}

func TestResolveFunctionArgs(t *testing.T) {
	t.Parallel()

	t.Run("positional_args_mapped_in_order", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("add",
			Parameter{Name: "a"},
			Parameter{Name: "b"},
		)
		resolved, err := resolveArgs(t, fn,
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
			Parameter{Name: "x", Default: core.NullValue{}},
			Parameter{Name: "y", Default: core.NullValue{}},
		)
		resolved, err := resolveArgs(t, fn,
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
			Parameter{Name: "sep", Default: core.StringValue{V: ","}},
		)
		resolved, err := resolveArgs(t, fn, nil, nil)
		require.NoError(t, err)
		assert.Equal(t, core.StringValue{V: ","}, resolved["sep"])
	})

	t.Run("named_param_passed_positionally_error", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("fn",
			Parameter{Name: "sep", Default: core.StringValue{V: ","}},
		)
		_, err := resolveArgs(t, fn,
			[]core.Value{core.StringValue{V: ";"}},
			nil,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "too many arguments")
	})

	t.Run("named_arg_overrides_default", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("fn",
			Parameter{Name: "sep", Default: core.StringValue{V: ","}},
		)
		resolved, err := resolveArgs(t, fn,
			nil,
			map[string]core.Value{"sep": core.StringValue{V: "|"}},
		)
		require.NoError(t, err)
		assert.Equal(t, core.StringValue{V: "|"}, resolved["sep"])
	})

	t.Run("named_default_present_when_only_positional_given", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("fn",
			Parameter{Name: "required"},
			Parameter{Name: "optional", Default: core.IntegerValue{V: 7}},
		)
		resolved, err := resolveArgs(t, fn,
			[]core.Value{core.IntegerValue{V: 1}},
			nil,
		)
		require.NoError(t, err)
		assert.Equal(t, core.IntegerValue{V: 1}, resolved["required"])
		assert.Equal(t, core.IntegerValue{V: 7}, resolved["optional"],
			"named param falls back to its default")
	})

	t.Run("no_args_no_params_ok", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("noop")
		resolved, err := resolveArgs(t, fn, nil, nil)
		require.NoError(t, err)
		assert.Empty(t, resolved)
	})

	t.Run("too_many_positional_error", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("fn", Parameter{Name: "x"})
		_, err := resolveArgs(t, fn,
			[]core.Value{core.IntegerValue{V: 1}, core.IntegerValue{V: 2}},
			nil,
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "too many arguments")
	})

	t.Run("unknown_named_arg_error", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("fn", Parameter{Name: "x", Default: core.NullValue{}})
		_, err := resolveArgs(t, fn,
			nil,
			map[string]core.Value{"unknown": core.IntegerValue{V: 1}},
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unknown argument")
		assert.Contains(t, err.Error(), `"unknown"`)
	})

	t.Run("positional_param_passed_by_name_error", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("fn", Parameter{Name: "x"})
		_, err := resolveArgs(t, fn,
			nil,
			map[string]core.Value{"x": core.IntegerValue{V: 2}},
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "positional and cannot be passed by name")
	})

	t.Run("missing_required_arg_error", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("fn", Parameter{Name: "x"})
		_, err := resolveArgs(t, fn, nil, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "missing required argument")
		assert.Contains(t, err.Error(), `"x"`)
	})

	t.Run("required_param_covered_by_positional_ok", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("fn", Parameter{Name: "x"})
		resolved, err := resolveArgs(t, fn,
			[]core.Value{core.StringValue{V: "v"}},
			nil,
		)
		require.NoError(t, err)
		assert.Equal(t, core.StringValue{V: "v"}, resolved["x"])
	})

	t.Run("wrong_kind_single_accepted_error", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("fn",
			Parameter{Name: "s", AcceptedKinds: []core.ValueKind{core.KindString}},
		)
		_, err := resolveArgs(t, fn,
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
			Parameter{Name: "v", AcceptedKinds: []core.ValueKind{core.KindString, core.KindInteger}},
		)
		_, err := resolveArgs(t, fn,
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
			Parameter{Name: "s", AcceptedKinds: []core.ValueKind{core.KindString}},
		)
		resolved, err := resolveArgs(t, fn,
			[]core.Value{core.StringValue{V: "hello"}},
			nil,
		)
		require.NoError(t, err)
		assert.Equal(t, core.StringValue{V: "hello"}, resolved["s"])
	})

	t.Run("one_of_multiple_accepted_kinds_passes", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("fn",
			Parameter{Name: "v", AcceptedKinds: []core.ValueKind{core.KindInteger, core.KindFloat}},
		)
		resolved, err := resolveArgs(t, fn,
			[]core.Value{core.FloatValue{V: 3.14}},
			nil,
		)
		require.NoError(t, err)
		assert.Equal(t, core.FloatValue{V: 3.14}, resolved["v"])
	})

	t.Run("empty_accepted_kinds_allows_any_type", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("fn", Parameter{Name: "v"})
		for _, val := range []core.Value{
			core.IntegerValue{V: 1},
			core.StringValue{V: "x"},
			core.BoolValue{V: true},
			core.NullValue{},
		} {
			_, err := resolveArgs(t, fn, []core.Value{val}, nil)
			assert.NoError(t, err, "should accept %s", val.Kind())
		}
	})

	t.Run("named_default_passes_kind_check", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("fn",
			Parameter{
				Name:          "opt",
				Default:       core.StringValue{V: "x"},
				AcceptedKinds: []core.ValueKind{core.KindString},
			},
		)
		resolved, err := resolveArgs(t, fn, nil, nil)
		require.NoError(t, err)
		assert.Equal(t, core.StringValue{V: "x"}, resolved["opt"])
	})

	t.Run("error_message_includes_function_name", func(t *testing.T) {
		t.Parallel()

		fn := mkFn("my_func", Parameter{Name: "x"})
		_, err := resolveArgs(t, fn, nil, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "my_func")
		assert.Contains(t, err.Error(), `"x"`)
	})
}
