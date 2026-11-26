package stdlib

import (
	"slices"
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/stretchr/testify/require"
)

func TestJoin(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name      string
		elements  []core.Value
		separator string
		want      string
		wantError string
	}
	tests := []testCase{
		{
			name:      "separator",
			elements:  []core.Value{str("re1"), str("re2")},
			separator: ",",
			want:      "re1,re2",
		},
		{
			name:     "empty separator",
			elements: []core.Value{str("re1"), str("re2")},
			want:     "re1re2",
		},
		{
			name:      "empty array",
			separator: ",",
		},
		{
			name:      "singleton",
			elements:  []core.Value{str("only")},
			separator: ",",
			want:      "only",
		},
		{
			name:      "empty elements",
			elements:  []core.Value{str(""), str("")},
			separator: ",",
			want:      ",",
		},
		{
			name:      "Unicode and NUL",
			elements:  []core.Value{str("привет"), str("界\x00🙂")},
			separator: "·",
			want:      "привет·界\x00🙂",
		},
		{
			name:      "invalid UTF-8 stays unchanged",
			elements:  []core.Value{str("bad\xff"), str("\xfeend")},
			separator: "\x80",
			want:      "bad\xff\x80\xfeend",
		},
		{
			name:      "invalid singleton",
			elements:  []core.Value{core.IntegerValue{V: 1}},
			separator: ",",
			wantError: "element 0: expected string, got integer",
		},
		{
			name:      "first invalid element wins",
			elements:  []core.Value{core.NullValue{}, core.IntegerValue{V: 1}},
			separator: ",",
			wantError: "element 0: expected string, got null",
		},
		{
			name:      "invalid element after valid prefix",
			elements:  []core.Value{str("a"), core.IntegerValue{V: 1}},
			separator: ",",
			wantError: "element 1: expected string, got integer",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fn, err := compile(join{})
			require.NoError(t, err)
			original := slices.Clone(tt.elements)
			values := []core.Value{core.ArrayValue{V: tt.elements}, str(tt.separator)}
			args := []core.Argument{
				{
					Value: &core.ConstExpr{V: values[0]},
				},
				{
					Value: &core.ConstExpr{V: values[1]},
				},
			}
			expr := &core.CallExpr{
				Node: core.NewNode(preparedTestPosition{}),
				Name: fn.Name(),
				Args: args,
				Plan: fn.PrepareCall(args),
			}
			require.NotNil(t, expr.Plan)
			state := &core.CallState{}
			t.Cleanup(state.Reset)
			type callMode struct {
				name string
				call func() (core.Value, error)
			}
			modes := []callMode{
				{
					name: "general",
					call: func() (core.Value, error) {
						return callFn(join{}, values, nil)
					},
				},
				{
					name: "indexed",
					call: func() (core.Value, error) {
						return join{}.CallIndexed(values)
					},
				},
				{
					name: "stateful nil state",
					call: func() (core.Value, error) {
						return join{}.CallIndexedWithState(nil, values)
					},
				},
				{
					name: "stateful reused state",
					call: func() (core.Value, error) {
						return join{}.CallIndexedWithState(state, values)
					},
				},
				{
					name: "prepared nil state",
					call: func() (core.Value, error) {
						return expr.Eval(nil)
					},
				},
				{
					name: "prepared reused state",
					call: func() (core.Value, error) {
						return expr.Eval(&preparedStateContext{state: state})
					},
				},
			}
			for _, mode := range modes {
				t.Run(mode.name, func(t *testing.T) {
					for range 2 {
						got, callErr := mode.call()
						if tt.wantError != "" {
							require.ErrorContains(t, callErr, tt.wantError)
							require.Equal(t, core.NullValue{}, got)
						} else {
							require.NoError(t, callErr)
							require.Equal(t, str(tt.want), got)
						}
						require.Equal(t, original, tt.elements)
					}
				})
			}
		})
	}
}

func TestJoinReleasesStringBuffer(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name      string
		elements  []core.Value
		want      string
		wantError string
		wantPanic bool
	}
	tests := []testCase{
		{
			name:     "success",
			elements: []core.Value{str("first"), str("second")},
			want:     "first,second",
		},
		{
			name:      "error after partial fill",
			elements:  []core.Value{str("first"), core.IntegerValue{V: 1}},
			wantError: "element 1: expected string, got integer",
		},
		{
			name:      "panic after partial fill",
			elements:  []core.Value{str("first"), nil},
			wantPanic: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			state := &core.CallState{}
			t.Cleanup(state.Reset)
			buffer := state.AcquireStrings(4)
			state.ReleaseStrings(buffer)
			args := []core.Value{core.ArrayValue{V: tt.elements}, str(",")}
			if tt.wantPanic {
				require.Panics(t, func() {
					_, _ = join{}.CallIndexedWithState(state, args)
				})
			} else {
				got, err := join{}.CallIndexedWithState(state, args)
				if tt.wantError != "" {
					require.EqualError(t, err, tt.wantError)
				} else {
					require.NoError(t, err)
					require.Equal(t, str(tt.want), got)
				}
			}
			next := state.AcquireStrings(1)
			require.Same(t, &buffer[0], &next[0])
			require.Equal(t, make([]string, 4), next[:cap(next)])
			state.ReleaseStrings(next)

			args[0] = core.ArrayValue{V: []core.Value{str("next"), str("call")}}
			got, err := join{}.CallIndexedWithState(state, args)
			require.NoError(t, err)
			require.Equal(t, str("next,call"), got)
		})
	}
}
