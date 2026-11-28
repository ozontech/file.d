package stdlib

import (
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/stretchr/testify/require"
)

func TestPreparedSimpleStringCalls(t *testing.T) {
	tests := []struct {
		name     string
		function string
		args     []string
		want     string
	}{
		{
			name:     "after first delimiter",
			function: "after",
			args:     []string{"a -> b -> c", " -> "},
			want:     "b -> c",
		},
		{
			name:     "after absent delimiter",
			function: "after",
			args:     []string{"unchanged", "!"},
			want:     "unchanged",
		},
		{
			name:     "after empty input",
			function: "after",
			args:     []string{"", "!"},
			want:     "",
		},
		{
			name:     "after empty delimiter",
			function: "after",
			args:     []string{"data", ""},
			want:     "data",
		},
		{
			name:     "after Unicode and NUL",
			function: "after",
			args:     []string{"начало界\x00конец", "界\x00"},
			want:     "конец",
		},
		{
			name:     "after invalid UTF-8 delimiter",
			function: "after",
			args:     []string{"before\xffafter\xfe", "\xff"},
			want:     "after\xfe",
		},
		{
			name:     "between first pair",
			function: "between",
			args:     []string{"a <<first>> <<second>>", "<<", ">>"},
			want:     "first",
		},
		{
			name:     "between stops at first closing delimiter",
			function: "between",
			args:     []string{"{a{b}c}", "{", "}"},
			want:     "a{b",
		},
		{
			name:     "between absent opening delimiter",
			function: "between",
			args:     []string{"unchanged]", "[", "]"},
			want:     "unchanged]",
		},
		{
			name:     "between absent closing delimiter",
			function: "between",
			args:     []string{"[unchanged", "[", "]"},
			want:     "[unchanged",
		},
		{
			name:     "between empty input",
			function: "between",
			args:     []string{"", "[", "]"},
			want:     "",
		},
		{
			name:     "between empty opening delimiter",
			function: "between",
			args:     []string{"data]tail", "", "]"},
			want:     "data",
		},
		{
			name:     "between empty closing delimiter",
			function: "between",
			args:     []string{"[data", "[", ""},
			want:     "",
		},
		{
			name:     "between empty delimiters",
			function: "between",
			args:     []string{"data", "", ""},
			want:     "",
		},
		{
			name:     "between Unicode and NUL",
			function: "between",
			args:     []string{"начало«середина\x00»конец", "«", "»"},
			want:     "середина\x00",
		},
		{
			name:     "between invalid UTF-8 delimiters",
			function: "between",
			args:     []string{"prefix\xffdata\xfd\xfetail", "\xff", "\xfe"},
			want:     "data\xfd",
		},
		{
			name:     "trim uses a character set",
			function: "trim",
			args:     []string{"msmdata sms", "ms "},
			want:     "data",
		},
		{
			name:     "trim empty input",
			function: "trim",
			args:     []string{"", "!"},
			want:     "",
		},
		{
			name:     "trim empty cutset",
			function: "trim",
			args:     []string{" data ", ""},
			want:     " data ",
		},
		{
			name:     "trim Unicode and NUL",
			function: "trim",
			args:     []string{"界\x00🎯data🎯\x00界", "界🎯\x00"},
			want:     "data",
		},
		{
			name:     "trim invalid UTF-8",
			function: "trim",
			args:     []string{"\xffdata\xfe", "\xff"},
			want:     "data",
		},
		{
			name:     "trim_left preserves the end",
			function: "trim_left",
			args:     []string{"msmdata sms", "ms "},
			want:     "data sms",
		},
		{
			name:     "trim_left empty input",
			function: "trim_left",
			args:     []string{"", "!"},
			want:     "",
		},
		{
			name:     "trim_left empty cutset",
			function: "trim_left",
			args:     []string{" data ", ""},
			want:     " data ",
		},
		{
			name:     "trim_left Unicode and NUL",
			function: "trim_left",
			args:     []string{"界\x00🎯data🎯", "界🎯\x00"},
			want:     "data🎯",
		},
		{
			name:     "trim_left invalid UTF-8",
			function: "trim_left",
			args:     []string{"\xff\xfedata\xfd", "\xff"},
			want:     "data\xfd",
		},
		{
			name:     "upcase ASCII",
			function: "upcase",
			args:     []string{"aBc-123"},
			want:     "ABC-123",
		},
		{
			name:     "upcase empty input",
			function: "upcase",
			args:     []string{""},
			want:     "",
		},
		{
			name:     "upcase Unicode and NUL",
			function: "upcase",
			args:     []string{"éя\x00🎯"},
			want:     "ÉЯ\x00🎯",
		},
		{
			name:     "upcase replaces invalid UTF-8",
			function: "upcase",
			args:     []string{"a\xffb\xfe"},
			want:     "A\ufffdB\ufffd",
		},
		{
			name:     "trim_to spans first and last occurrences",
			function: "trim_to",
			args:     []string{"prefix END a END b END suffix", "END"},
			want:     "END a END b END",
		},
		{
			name:     "trim_to absent delimiter",
			function: "trim_to",
			args:     []string{"unchanged", "!"},
			want:     "unchanged",
		},
		{
			name:     "trim_to empty input",
			function: "trim_to",
			args:     []string{"", "!"},
			want:     "",
		},
		{
			name:     "trim_to empty delimiter",
			function: "trim_to",
			args:     []string{"data", ""},
			want:     "data",
		},
		{
			name:     "trim_to Unicode and NUL",
			function: "trim_to",
			args:     []string{"prefix界\x00data界\x00suffix", "界\x00"},
			want:     "界\x00data界\x00",
		},
		{
			name:     "trim_to invalid UTF-8 delimiter",
			function: "trim_to",
			args:     []string{"prefix\xffdata\xfe\xffsuffix", "\xff"},
			want:     "\xffdata\xfe\xff",
		},
		{
			name:     "trim_to_left keeps first delimiter",
			function: "trim_to_left",
			args:     []string{"a -> b -> c", "->"},
			want:     "-> b -> c",
		},
		{
			name:     "trim_to_left absent delimiter",
			function: "trim_to_left",
			args:     []string{"unchanged", "!"},
			want:     "unchanged",
		},
		{
			name:     "trim_to_left empty input",
			function: "trim_to_left",
			args:     []string{"", "!"},
			want:     "",
		},
		{
			name:     "trim_to_left empty delimiter",
			function: "trim_to_left",
			args:     []string{"data", ""},
			want:     "data",
		},
		{
			name:     "trim_to_left Unicode and NUL",
			function: "trim_to_left",
			args:     []string{"prefix界\x00data界\x00suffix", "界\x00"},
			want:     "界\x00data界\x00suffix",
		},
		{
			name:     "trim_to_left invalid UTF-8 delimiter",
			function: "trim_to_left",
			args:     []string{"prefix\xffdata\xfe\xffsuffix", "\xff"},
			want:     "\xffdata\xfe\xffsuffix",
		},
		{
			name:     "trim_to_right keeps last delimiter",
			function: "trim_to_right",
			args:     []string{"a -> b -> c", "->"},
			want:     "a -> b ->",
		},
		{
			name:     "trim_to_right absent delimiter",
			function: "trim_to_right",
			args:     []string{"unchanged", "!"},
			want:     "unchanged",
		},
		{
			name:     "trim_to_right empty input",
			function: "trim_to_right",
			args:     []string{"", "!"},
			want:     "",
		},
		{
			name:     "trim_to_right empty delimiter",
			function: "trim_to_right",
			args:     []string{"data", ""},
			want:     "data",
		},
		{
			name:     "trim_to_right Unicode and NUL",
			function: "trim_to_right",
			args:     []string{"prefix界\x00data界\x00suffix", "界\x00"},
			want:     "prefix界\x00data界\x00",
		},
		{
			name:     "trim_to_right invalid UTF-8 delimiter",
			function: "trim_to_right",
			args:     []string{"prefix\xffdata\xfe\xffsuffix", "\xff"},
			want:     "prefix\xffdata\xfe\xff",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fn, ok := GetRegistry().Get(tt.function)
			require.True(t, ok)
			values := make([]core.Value, len(tt.args))
			args := make([]core.Argument, len(tt.args))
			for i, arg := range tt.args {
				values[i] = str(arg)
				args[i] = core.Argument{Value: &core.ConstExpr{V: values[i]}}
			}
			resolved, err := fn.Resolve(values, nil)
			require.NoError(t, err)

			general, generalErr := fn.Call(resolved)
			prepared, preparedErr := preparedTestCall(t, fn, args)

			require.NoError(t, generalErr)
			require.NoError(t, preparedErr)
			require.Equal(t, str(tt.want), general)
			require.Equal(t, general, prepared)
		})
	}
}
