package cfg

import (
	"testing"

	"github.com/ozontech/file.d/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var lg = logger.Instance.Desugar()

func TestParseVale(t *testing.T) {
	result, err := ParseSubstitution("just value", lg)

	assert.NoError(t, err, "error occurs")
	assert.Equal(t, 1, len(result), "wrong result")
	assert.Equal(t, "just value", result[0].Data[0], "wrong result")
}

func TestParseField(t *testing.T) {
	result, err := ParseSubstitution("days till world end ${prediction.days}. so what?", lg)

	assert.NoError(t, err, "error occurs")
	assert.Equal(t, 3, len(result), "wrong result")
	assert.Equal(t, "days till world end ", result[0].Data[0], "wrong result")
	assert.Equal(t, "prediction", result[1].Data[0], "wrong result")
	assert.Equal(t, "days", result[1].Data[1], "wrong result")
	assert.Equal(t, ". so what?", result[2].Data[0], "wrong result")
}

func TestParseEnding(t *testing.T) {
	result, err := ParseSubstitution("days till world end ${prediction.days}", lg)

	assert.NoError(t, err, "error occurs")
	assert.Equal(t, 2, len(result), "wrong result")
	assert.Equal(t, "days till world end ", result[0].Data[0], "wrong result")
	assert.Equal(t, "prediction", result[1].Data[0], "wrong result")
	assert.Equal(t, "days", result[1].Data[1], "wrong result")
}

func TestParseEscape(t *testing.T) {
	result, err := ParseSubstitution("days till world end $$100", lg)

	assert.NoError(t, err, "error occurs")
	assert.Equal(t, 1, len(result), "wrong result")
	assert.Equal(t, "days till world end $100", result[0].Data[0], "wrong result")
}

func TestParseErr(t *testing.T) {
	_, err := ParseSubstitution("days till world end ${prediction.days. so what?", lg)

	assert.NotNil(t, err, "no error")
}

func TestParseFieldWithFilter(t *testing.T) {
	tests := []struct {
		name         string
		substitution string
		data         [][]string
		filters      [][][]any
		wantErr      bool
	}{
		{
			name:         "with_one_filter",
			substitution: `days till world end ${prediction.days|re("(\\d),(test.+)",[1,2]," , ")}. so what?`,
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
			substitution: `days till world end ${prediction.days|re("(\\d),(test.+)",[1,2]," , ") | re("(test2\\.subtest)", [1], "-||-")}. so what?`,
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
						[]int{1, 2},
						" , ",
					},
					{
						"(test2\\.subtest)",
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
			substitution: `days till world end ${prediction.days|re("(\\d),(test.+)",[1,2]," , ")}. Hello, ${name|re("(\\w+)",[1],",")}`,
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
						[]int{1, 2},
						" , ",
					},
				},
				nil,
				{
					{
						"(\\w+)",
						[]int{1},
						",",
					},
				},
			},
			wantErr: false,
		},
		{
			name:         "err_invalid_filter",
			substitution: `test ${field|abcd()} test2`,
			wantErr:      true,
		},
		{
			name:         "err_invalid_args_empty",
			substitution: `test ${field|re()} test2`,
			wantErr:      true,
		},
		{
			name:         "err_invalid_args_invalid_first_arg",
			substitution: `test ${field|re('(invalid)',[1,],"|")} test2`,
			wantErr:      true,
		},
		{
			name:         "err_invalid_args_invalid_second_arg",
			substitution: `test ${field|re("invalid",[invalid],"|")} test2`,
			wantErr:      true,
		},
		{
			name:         "err_invalid_args_invalid_third_arg",
			substitution: `test ${field|re("(invalid)",[1],'invalid')} test2`,
			wantErr:      true,
		},
		{
			name:         "err_invalid_args_no_last_bracket",
			substitution: `test ${field|re('invalid'} test2`,
			wantErr:      true,
		},
		{
			name:         "err_invalid_args_bracket_not_closed",
			substitution: `test ${field|re('invalid', [(1,2, "|")} test2`,
			wantErr:      true,
		},
		{
			name:         "err_invalid_args_invalid_bracket_sequence",
			substitution: `test ${field|re('invalid', [1,2, "|")} test2`,
			wantErr:      true,
		},
		{
			name:         "err_invalid_args_invalid_bracket_sequence2",
			substitution: `test ${field|re('invalid', [(1,2], "|")} test2`,
			wantErr:      true,
		},
		{
			name:         "err_invalid_args_no_closing_quotes",
			substitution: `test ${field|re("invalid", [1,2], "|)} test2`,
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := ParseSubstitution(tt.substitution, lg)
			require.Equal(t, tt.wantErr, err != nil, "no error was expected")
			assert.Equal(t, len(tt.data), len(result), "invalid substitution ops len")
			for j := range tt.data {
				assert.Equal(t, len(tt.data[j]), len(result[j].Data), "invalid data len")
				for k := range tt.data[j] {
					assert.Equal(t, tt.data[j][k], result[j].Data[k], "wrong data for")
				}
				if tt.filters[j] == nil {
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

func TestRegexFilterApply(t *testing.T) {
	result, err := ParseSubstitution(`test ${field|re("(re\\d)",[1],"|")}`, lg)
	require.NoError(t, err)
	res := result[1].Filters[0].Apply([]byte(`this is some text re1 end`))
	assert.Equal(t, "re1", string(res))
}
