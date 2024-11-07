package substitution

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var lg = zap.NewExample()

func TestParseSubstitution(t *testing.T) {
	tests := []struct {
		name         string
		substitution string
		data         [][]string
		filters      [][][]any
		wantErr      bool
	}{
		{
			name:         "no_filter_no_field",
			substitution: `just value`,
			data: [][]string{
				{"just value"},
			},
			wantErr: false,
		},
		{
			name:         "no_filter_only_field",
			substitution: `${prediction.days}`,
			data: [][]string{
				{"prediction", "days"},
			},
			wantErr: false,
		},
		{
			name:         "no_filter_field",
			substitution: `days till world end ${prediction.days}. so what?`,
			data: [][]string{
				{"days till world end "},
				{"prediction", "days"},
				{". so what?"},
			},
			wantErr: false,
		},
		{
			name:         "no_filter_field_no_ending",
			substitution: `days till world end ${prediction.days}`,
			data: [][]string{
				{"days till world end "},
				{"prediction", "days"},
			},
			wantErr: false,
		},
		{
			name:         "no_filter_no_field_parse_escape",
			substitution: `days till world end $$100`,
			data: [][]string{
				{"days till world end $100"},
			},
			wantErr: false,
		},
		{
			name:         "no_filter_no_field_parse_no_escape",
			substitution: `days till world end $100`,
			data: [][]string{
				{"days till world end $100"},
			},
			wantErr: false,
		},
		{
			name:         "no_filter_no_field_parse_no_escape_2",
			substitution: `days till world end $100$`,
			data: [][]string{
				{"days till world end $100$"},
			},
			wantErr: false,
		},
		{
			name:         "no_filter_no_field_parse_dollar",
			substitution: `$`,
			data: [][]string{
				{"$"},
			},
			wantErr: false,
		},
		{
			name:         "empty_string",
			substitution: ``,
			data:         [][]string{},
			wantErr:      false,
		},
		{
			name:         "with_one_filter",
			substitution: `days till world end ${prediction.days|re("(\\d),(test.+)",-1,[1,2]," , ")}. so what?`,
			data: [][]string{
				{"days till world end "},
				{"prediction", "days"},
				{". so what?"},
			},
			filters: [][][]any{
				nil,
				{
					{
						"(\\d),(test.+)",
						-1,
						[]int{1, 2},
						" , ",
					},
				},
				nil,
			},
			wantErr: false,
		},
		{
			name:         "with_two_filters",
			substitution: `days till world end ${prediction.days|re("(\\d),(test.+)",-1,[1,2]," , ") | re("(test2\\.subtest)",-1, [1], "-||-")}. so what?`,
			data: [][]string{
				{"days till world end "},
				{"prediction", "days"},
				{". so what?"},
			},
			filters: [][][]any{
				nil,
				{
					{
						"(\\d),(test.+)",
						-1,
						[]int{1, 2},
						" , ",
					},
					{
						"(test2\\.subtest)",
						-1,
						[]int{1},
						"-||-",
					},
				},
				nil,
			},
			wantErr: false,
		},
		{
			name:         "with_two_substitutions_one_filter",
			substitution: `days till world end ${prediction.days|re("(\\d),(test.+)",-1,[1,2]," , ")}. Hello, ${name|re("(\\w+)",1,[1],",",true)}`,
			data: [][]string{
				{"days till world end "},
				{"prediction", "days"},
				{". Hello, "},
				{"name"},
			},
			filters: [][][]any{
				nil,
				{
					{
						"(\\d),(test.+)",
						-1,
						[]int{1, 2},
						" , ",
					},
				},
				nil,
				{
					{
						"(\\w+)",
						1,
						[]int{1},
						",",
						true,
					},
				},
			},
			wantErr: false,
		},
		{
			name:         "err_invalid_field",
			substitution: `days till world end ${prediction.days. so what?`,
			wantErr:      true,
		},
		{
			name:         "err_invalid_filter",
			substitution: `test ${field|abcd()} test2`,
			wantErr:      true,
		},
		{
			name:         "err_re_filter_args_empty",
			substitution: `test ${field|re()} test2`,
			wantErr:      true,
		},
		{
			name:         "err_re_filter_invalid_args_count_min",
			substitution: `test ${field|re("invalid", -1, [1,2])} test2`,
			wantErr:      true,
		},
		{
			name:         "err_re_filter_invalid_args_count_max",
			substitution: `test ${field|re("invalid", -1, [1,2], "|", 1, 2)} test2`,
			wantErr:      true,
		},
		{
			name:         "err_re_filter_invalid_args_invalid_first_arg",
			substitution: `test ${field|re('(invalid)',-1,[1,],"|")} test2`,
			wantErr:      true,
		},
		{
			name:         "err_re_filter_invalid_args_invalid_second_arg",
			substitution: `test ${field|re("(invalid)","abcd",[1,],"|")} test2`,
			wantErr:      true,
		},
		{
			name:         "err_re_filter_invalid_args_invalid_third_arg",
			substitution: `test ${field|re("invalid",-1,[invalid],"|")} test2`,
			wantErr:      true,
		},
		{
			name:         "err_re_filter_invalid_args_invalid_fourth_arg",
			substitution: `test ${field|re("(invalid)",-1,[1],'invalid')} test2`,
			wantErr:      true,
		},
		{
			name:         "err_re_filter_invalid_args_invalid_fifth_arg",
			substitution: `test ${field|re("(invalid)",-1,[1],"|",100)} test2`,
			wantErr:      true,
		},
		{
			name:         "err_invalid_args_no_last_bracket",
			substitution: `test ${field|re('invalid'} test2`,
			wantErr:      true,
		},
		{
			name:         "err_invalid_args_bracket_not_closed",
			substitution: `test ${field|re('invalid', -1, [(1,2, "|")} test2`,
			wantErr:      true,
		},
		{
			name:         "err_invalid_args_invalid_bracket_sequence",
			substitution: `test ${field|re('invalid', -1, [1,2, "|")} test2`,
			wantErr:      true,
		},
		{
			name:         "err_invalid_args_invalid_bracket_sequence2",
			substitution: `test ${field|re('invalid', -1, [(1,2], "|")} test2`,
			wantErr:      true,
		},
		{
			name:         "err_invalid_args_no_closing_quotes",
			substitution: `test ${field|re("invalid", -1, [1,2], "|)} test2`,
			wantErr:      true,
		},
		{
			name:         "trim_filter_ok",
			substitution: `test ${field|trim("all","\\n")} test2`,
			data: [][]string{
				{"test "},
				{"field"},
				{" test2"},
			},
			filters: [][][]any{
				nil,
				{
					{
						trimModeAll,
						"\\n",
					},
				},
				nil,
			},
			wantErr: false,
		},
		{
			name:         "err_trim_filter_args_empty",
			substitution: `test ${field|trim()} test2`,
			wantErr:      true,
		},
		{
			name:         "err_trim_filter_invalid_args_count",
			substitution: `test ${field|trim("all")} test2`,
			wantErr:      true,
		},
		{
			name:         "err_trim_filter_invalid_args_invalid_first_arg",
			substitution: `test ${field|trim("invalid","\\n")} test2`,
			wantErr:      true,
		},
		{
			name:         "err_trim_filter_invalid_args_invalid_first_arg_2",
			substitution: `test ${field|trim('invalid',"\\n")} test2`,
			wantErr:      true,
		},
		{
			name:         "err_trim_filter_invalid_args_invalid_second_arg",
			substitution: `test ${field|trim("all",'invalid')} test2`,
			wantErr:      true,
		},
		{
			name:         "trim_to_filter_ok",
			substitution: `test ${field|trim_to("left","{")} test2`,
			data: [][]string{
				{"test "},
				{"field"},
				{" test2"},
			},
			filters: [][][]any{
				nil,
				{
					{
						trimModeLeft,
						"{",
					},
				},
				nil,
			},
			wantErr: false,
		},
		{
			name:         "err_trim_to_filter_args_empty",
			substitution: `test ${field|trim_to()} test2`,
			wantErr:      true,
		},
		{
			name:         "err_trim_to_filter_invalid_args_count",
			substitution: `test ${field|trim_to("all")} test2`,
			wantErr:      true,
		},
		{
			name:         "err_trim_to_filter_invalid_args_invalid_first_arg",
			substitution: `test ${field|trim_to("invalid","}")} test2`,
			wantErr:      true,
		},
		{
			name:         "err_trim_to_filter_invalid_args_invalid_first_arg_2",
			substitution: `test ${field|trim_to('invalid',"}")} test2`,
			wantErr:      true,
		},
		{
			name:         "err_trim_to_filter_invalid_args_invalid_second_arg",
			substitution: `test ${field|trim_to("all",'invalid')} test2`,
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := ParseSubstitution(tt.substitution, nil, lg)
			require.Equal(t, tt.wantErr, err != nil, "no error was expected")
			if tt.wantErr {
				return
			}
			assert.Equal(t, len(tt.data), len(result), "invalid substitution ops len")
			for j := range tt.data {
				assert.Equal(t, len(tt.data[j]), len(result[j].Data), "invalid data len")
				for k := range tt.data[j] {
					assert.Equal(t, tt.data[j][k], result[j].Data[k], "wrong data for")
				}
				if len(tt.filters) == 0 || len(tt.filters[j]) == 0 {
					assert.Nil(t, result[j].Filters)
				} else {
					assert.Equal(t, len(tt.filters[j]), len(result[j].Filters), "invalid filters len")
					for k := range tt.filters[j] {
						assert.NoError(t, result[j].Filters[k].compareArgs(tt.filters[j][k]), "wrong args for filter")
					}
				}
			}
		})
	}
}

func TestFilterApply(t *testing.T) {
	tests := []struct {
		name         string
		substitution string
		data         string
		want         string
	}{
		{
			name:         "ok_single_re_filter",
			substitution: `${field|re("(re\\d)",-1,[1],"|")}`,
			data:         `this is some text re1 end`,
			want:         "re1",
		},
		{
			name:         "ok_two_re_filters",
			substitution: `${field|re("(.*)",-1,[1],"|")|re("(\\d\\.)",-1,[1],"|")}`,
			data:         `1.2.3.4.5.`,
			want:         "1.|2.|3.|4.|5.",
		},
		{
			name:         "ok_single_re_filter_2",
			substitution: `${field|re("(re\\d)",2,[1],"|")}`,
			data:         `this is some text re1 re2 re3 re4 end`,
			want:         "re1|re2",
		},
		{
			name:         "ok_re_filter_empty_on_not_matched_false",
			substitution: `${field|re("(re\\d)",1,[1],"|")}`,
			data:         `this is some text`,
			want:         "this is some text",
		},
		{
			name:         "ok_re_filter_empty_on_not_matched_true",
			substitution: `${field|re("(re\\d)",1,[1],"|",true)}`,
			data:         `this is some text`,
			want:         "",
		},
		{
			name:         "ok_single_trim_filter_trim_all",
			substitution: `${field|trim("all","\\n")}`,
			data:         `\n{"message":"test"}\n`,
			want:         `{"message":"test"}`,
		},
		{
			name:         "ok_single_trim_filter_trim_left",
			substitution: `${field|trim("left","\\n")}`,
			data:         `\n{"message":"test"}\n`,
			want:         `{"message":"test"}\n`,
		},
		{
			name:         "ok_single_trim_filter_trim_right",
			substitution: `${field|trim("right","\\n")}`,
			data:         `\n{"message":"test"}\n`,
			want:         `\n{"message":"test"}`,
		},
		{
			name:         "ok_single_trim_to_filter_trim_all",
			substitution: `${field|trim_to("all","\"")}`,
			data:         `some data "quoted" some another data`,
			want:         `"quoted"`,
		},
		{
			name:         "ok_two_trim_to_filters",
			substitution: `${field|trim_to("left","{")|trim_to("right","}")}`,
			data:         `some data {"message":"test"} some data`,
			want:         `{"message":"test"}`,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			subOps, err := ParseSubstitution(tt.substitution, nil, lg)
			require.NoError(t, err)
			data := make([]byte, 0, len(tt.data))
			data = append(data, []byte(tt.data)...)
			for i := range subOps[0].Filters {
				data = subOps[0].Filters[i].Apply(data, data)
			}
			assert.Equal(t, tt.want, string(data))
		})
	}
}
