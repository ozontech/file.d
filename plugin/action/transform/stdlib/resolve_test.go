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

// callFn invokes fn the way the interpreter does: arguments are bound through
// the compiled signature, so omitted named parameters get their declared
// defaults instead of being absent from the map.
func callFn(fn Function, positional []core.Value, named map[string]core.Value) (core.Value, error) {
	c, err := compile(fn)
	if err != nil {
		return nil, err
	}
	resolved, err := c.Resolve(positional, named)
	if err != nil {
		return nil, err
	}
	return c.Call(resolved)
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

	str := func(v string) core.Value { return core.StringValue{V: v} }
	num := func(v int64) core.Value { return core.IntegerValue{V: v} }

	tests := []struct {
		name       string
		params     []Parameter
		positional []core.Value
		named      map[string]core.Value
		want       map[string]core.Value
		wantErr    string
	}{
		{
			name:       "positional_args_mapped_in_order",
			params:     []Parameter{{Name: "a"}, {Name: "b"}},
			positional: []core.Value{num(1), num(2)},
			want:       map[string]core.Value{"a": num(1), "b": num(2)},
		},
		{
			name: "named_args_mapped_by_name",
			params: []Parameter{
				{Name: "x", Default: core.NullValue{}},
				{Name: "y", Default: core.NullValue{}},
			},
			named: map[string]core.Value{"y": num(20), "x": num(10)},
			want:  map[string]core.Value{"x": num(10), "y": num(20)},
		},
		{
			name:   "default_used_when_param_not_provided",
			params: []Parameter{{Name: "sep", Default: str(",")}},
			want:   map[string]core.Value{"sep": str(",")},
		},
		{
			name:   "named_arg_overrides_default",
			params: []Parameter{{Name: "sep", Default: str(",")}},
			named:  map[string]core.Value{"sep": str("|")},
			want:   map[string]core.Value{"sep": str("|")},
		},
		{
			name: "named_default_present_when_only_positional_given",
			params: []Parameter{
				{Name: "required"},
				{Name: "optional", Default: num(7)},
			},
			positional: []core.Value{num(1)},
			want:       map[string]core.Value{"required": num(1), "optional": num(7)},
		},
		{
			name: "no_args_no_params_ok",
			want: map[string]core.Value{},
		},
		{
			name:       "required_param_covered_by_positional",
			params:     []Parameter{{Name: "x"}},
			positional: []core.Value{str("v")},
			want:       map[string]core.Value{"x": str("v")},
		},
		{
			name:       "named_param_passed_positionally_error",
			params:     []Parameter{{Name: "sep", Default: str(",")}},
			positional: []core.Value{str(";")},
			wantErr:    "too many arguments",
		},
		{
			name:       "too_many_positional_error",
			params:     []Parameter{{Name: "x"}},
			positional: []core.Value{num(1), num(2)},
			wantErr:    "too many arguments",
		},
		{
			name:    "unknown_named_arg_error",
			params:  []Parameter{{Name: "x", Default: core.NullValue{}}},
			named:   map[string]core.Value{"unknown": num(1)},
			wantErr: `unknown argument "unknown"`,
		},
		{
			name:    "positional_param_passed_by_name_error",
			params:  []Parameter{{Name: "x"}},
			named:   map[string]core.Value{"x": num(2)},
			wantErr: "positional and cannot be passed by name",
		},
		{
			name:    "missing_required_arg_error",
			params:  []Parameter{{Name: "x"}},
			wantErr: `missing required argument "x"`,
		},
		{
			name:       "wrong_kind_single_accepted_error",
			params:     []Parameter{{Name: "s", AcceptedKinds: []core.ValueKind{core.KindString}}},
			positional: []core.Value{num(42)},
			wantErr:    "expected string, got integer",
		},
		{
			name: "wrong_kind_multiple_accepted_error",
			params: []Parameter{
				{Name: "v", AcceptedKinds: []core.ValueKind{core.KindString, core.KindInteger}},
			},
			positional: []core.Value{core.BoolValue{V: true}},
			wantErr:    "expected string or integer, got bool",
		},
		{
			name:       "correct_kind_passes",
			params:     []Parameter{{Name: "s", AcceptedKinds: []core.ValueKind{core.KindString}}},
			positional: []core.Value{str("hello")},
			want:       map[string]core.Value{"s": str("hello")},
		},
		{
			name: "one_of_multiple_accepted_kinds_passes",
			params: []Parameter{
				{Name: "v", AcceptedKinds: []core.ValueKind{core.KindInteger, core.KindFloat}},
			},
			positional: []core.Value{core.FloatValue{V: 3.14}},
			want:       map[string]core.Value{"v": core.FloatValue{V: 3.14}},
		},
		{
			name:       "empty_accepted_kinds_allows_integer",
			params:     []Parameter{{Name: "v"}},
			positional: []core.Value{num(1)},
			want:       map[string]core.Value{"v": num(1)},
		},
		{
			name:       "empty_accepted_kinds_allows_null",
			params:     []Parameter{{Name: "v"}},
			positional: []core.Value{core.NullValue{}},
			want:       map[string]core.Value{"v": core.NullValue{}},
		},
		{
			name: "named_default_passes_kind_check",
			params: []Parameter{
				{Name: "opt", Default: str("x"), AcceptedKinds: []core.ValueKind{core.KindString}},
			},
			want: map[string]core.Value{"opt": str("x")},
		},
		{
			name:    "error_message_includes_function_name",
			params:  []Parameter{{Name: "x"}},
			wantErr: `function "fn": missing required argument "x"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			compiled, err := compile(mkFn("fn", tt.params...))
			require.NoError(t, err)

			resolved, err := compiled.Resolve(tt.positional, tt.named)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, resolved)
		})
	}
}
