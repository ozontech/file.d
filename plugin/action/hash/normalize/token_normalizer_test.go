package normalize

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseBuiltinPatterns(t *testing.T) {
	tests := []struct {
		name string

		input   string
		want    int
		wantErr bool
	}{
		{
			name:  "all",
			input: "all",
			want:  pAll,
		},
		{
			name:  "no",
			input: "no",
			want:  pNo,
		},
		{
			name:  "single",
			input: "email",
			want:  pEmail,
		},
		{
			name:  "multiple",
			input: "host|url|single_quoted",
			want:  pHost + pUrl + pSingleQuoted,
		},
		{
			name:    "bad_pattern",
			input:   "host|url|unknown",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := parseBuiltinPatterns(tt.input)
			require.Equal(t, tt.wantErr, err != nil)
			if tt.wantErr {
				return
			}
			require.Equal(t, tt.want, got)
		})
	}
}

func TestHasPattern(t *testing.T) {
	tests := []struct {
		name string

		patterns int
		input    []int

		want bool
	}{
		{
			name:     "all",
			patterns: pAll,
			input:    []int{pEmail, pBool, pDoubleQuoted},
			want:     true,
		},
		{
			name:     "one_true",
			patterns: pEmail,
			input:    []int{pEmail},
			want:     true,
		},
		{
			name:     "one_false",
			patterns: pEmail,
			input:    []int{pUrl},
			want:     false,
		},
		{
			name:     "multiple_true",
			patterns: pEmail + pBool + pDatetime,
			input:    []int{pDuration, pInt, pBool},
			want:     true,
		},
		{
			name:     "multiple_false",
			patterns: pEmail + pBool + pDatetime,
			input:    []int{pDuration, pInt, pFloat},
			want:     false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tt.want, hasPattern(tt.patterns, tt.input...))
		})
	}
}

func TestNormalizeByBytesOnly(t *testing.T) {
	tests := []struct {
		name string

		input []string
		want  string
	}{
		{
			name:  "curly_brackets",
			input: []string{`some {"a":1,b:{"c":2,"d":3},e:[4,5,6]} here`},
			want:  "some <curly_bracketed> here",
		},
		{
			name:  "square_brackets",
			input: []string{`some [val1, val2, [{val3_1}, (val3_2)]] here`},
			want:  "some <square_bracketed> here",
		},
		{
			name:  "parentheses",
			input: []string{`some (asd(gfd)(())) here`},
			want:  "some <parenthesized> here",
		},
		{
			name: "double_quotes",
			input: []string{
				`some "bla bla" here`,
				`some """bla "asd" bla""" here`,
				`some "\"bla\" asd \"bla\"" here`,
			},
			want: "some <double_quoted> here",
		},
		{
			name: "single_quotes",
			input: []string{
				`some 'bla bla' here`,
				`some '''bla 'asd' bla''' here`,
				`some '\'bla\' asd \'bla\'' here`,
			},
			want: "some <single_quoted> here",
		},
		{
			name: "grave_quotes",
			input: []string{
				"some `bla bla` here",
				"some ```bla `asd` bla``` here",
				"some `\\`bla\\` asd \\`bla\\`` here",
			},
			want: "some <grave_quoted> here",
		},
		{
			name:  "partial_token1",
			input: []string{`some "dsadsadasd asd qw`},
			want:  "some <double_quoted>",
		},
		{
			name:  "partial_token2",
			input: []string{`some {"a":1,b:{"c":2,"d":3},e:[4,5,6]`},
			want:  "some <curly_bracketed>",
		},
		{
			name:  "multiple",
			input: []string{`some {"a":1,b:{"c":2,"d":3},e:[4,5,6]} & [val1, val2, [{val3_1}, (val3_2)]] & "bla bla" here`},
			want:  "some <curly_bracketed> & <square_bracketed> & <double_quoted> here",
		},
	}

	const testNormalizeByBytesPattern = "curly_bracketed|square_bracketed|parenthesized|double_quoted|single_quoted|grave_quoted"

	n, err := NewTokenNormalizer(TokenNormalizerParams{
		BuiltinPatterns: testNormalizeByBytesPattern,
	})
	require.NoError(t, err)

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out := make([]byte, 0)

			for _, i := range tt.input {
				out = n.Normalize(out, []byte(i))
				assert.Equal(t, tt.want, string(out), "wrong out with input=%q", i)
			}
		})
	}
}

func TestTokenNormalizerBuiltin(t *testing.T) {
	tests := []struct {
		name string

		inputs   []string
		patterns string

		want string
	}{
		{
			name:     "no_matches",
			inputs:   []string{"Falsehood is s1mple"},
			patterns: "all",
			want:     "Falsehood is s1mple",
		},
		{
			name: "email",
			inputs: []string{
				"some test@host.com here",
				"some test@host1.host2.com here",
			},
			patterns: "email",
			want:     "some <email> here",
		},
		{
			name: "url",
			inputs: []string{
				"some http://some.host.com/page1?a=1 here",
				"some https://some.host.test/page2 here",
				"some ws://some.host1.host2.net here",
				"some wss://some.host1.host2.net here",
				"some ftp://login:pass@serv.example.com:21/function/reg.php here",
			},
			patterns: "url",
			want:     "some <url> here",
		},
		{
			name: "host",
			inputs: []string{
				"some hello-world-123.COM here",
				"some www.weather.jp here",
			},
			patterns: "host",
			want:     "some <host> here",
		},
		{
			name:     "uuid",
			inputs:   []string{"some 7c1811ed-e98f-4c9c-a9f9-58c757ff494f here"},
			patterns: "uuid",
			want:     "some <uuid> here",
		},
		{
			name:     "sha1",
			inputs:   []string{"some a94a8fe5ccb19ba61c4c0873d391e987982fbbd3 here"},
			patterns: "sha1",
			want:     "some <sha1> here",
		},
		{
			name:     "md5",
			inputs:   []string{"some 098f6bcd4621d373cade4e832627b4f6 here"},
			patterns: "md5",
			want:     "some <md5> here",
		},
		{
			name: "datetime",
			inputs: []string{
				"some 2025-01-13T10:20:40Z here",
				"some 2025-01-13T10:20:40.999999999Z here",
				"some 2025-01-13T10:20:40-06:00 here",
				"some 2025-01-13T10:20:40+04:00 here",
				"some 2025-01-13 10:20:40 here",
				"some 2025-01-13 here",
				"some 10:20:40 here",
			},
			patterns: "datetime",
			want:     "some <datetime> here",
		},
		{
			name: "ip",
			inputs: []string{
				"some 1.2.3.4 here",
				"some 01.102.103.104 here",

				// IPv6 Normal
				//"some 2001:db8:3333:4444:5555:DDDD:EEEE:FFFF here",
				//"some :: here",
				//"some 2001:db8:: here",
				//"some ::1234:5678 here",
				//"some 2001:0db8:0001:0000:0000:0ab9:C0A8:0102 here",
				//"some 2001:db8::1234:5678 here",

				// IPv6 Dual
				//"some 2001:db8:3333:4444:5555:6666:1.2.3.4 here",
				//"some ::11.22.33.44 here",
				//"some 2001:db8::123.123.123.123 here",
				//"some ::1234:5678:91.123.4.56 here",
				//"some ::1234:5678:1.2.3.4 here",
				//"some 2001:db8::1234:5678:5.6.7.8 here",
			},
			patterns: "ip",
			want:     "some <ip> here",
		},
		{
			name: "duration",
			inputs: []string{
				"some 1.2m5s here",
				"some -50s20ms10µs here",
				"some 1w2d3h4m5s6ms7us8ns here",
			},
			patterns: "duration",
			want:     "some <duration> here",
		},
		{
			name: "hex",
			inputs: []string{
				"some 0x13eb85e69dfbc0758b12acdaae36287d here",
				"some 0X553026A59C here",
			},
			patterns: "hex",
			want:     "some <hex> here",
		},
		{
			name: "float",
			inputs: []string{
				"some 1.23 here",
				"some -4.56 here",
			},
			patterns: "float",
			want:     "some <float> here",
		},
		{
			name: "int",
			inputs: []string{
				"some 100 here",
				"some -200 here",
			},
			patterns: "int",
			want:     "some <int> here",
		},
		{
			name: "bool",
			inputs: []string{
				"some TRUE here",
				"some FALSE here",
				"some true here",
				"some false here",
				"some tRuE here",
				"some FaLsE here",
			},
			patterns: "bool",
			want:     "some <bool> here",
		},
		{
			name: "all",
			inputs: []string{`
				Today Monday, 2025-01-13.

				Shopping list:
				- 100 apples
				- 10.5 milk
				- true bananas
				- 0X553026A59C onions
				- 7c1811ed-e98f-4c9c-a9f9-58c757ff494f, a94a8fe5ccb19ba61c4c0873d391e987982fbbd3, 098f6bcd4621d373cade4e832627b4f6

				User info:
				- request: www.weather.jp
				- ip: 1.2.3.4
				- email: user@subdomain.domain.org

				Downloaded from https://some.host.test for 5.5s.
			`,
			},
			patterns: "all",
			want: `
				Today Monday, <datetime>.

				Shopping list:
				- <int> apples
				- <float> milk
				- <bool> bananas
				- <hex> onions
				- <uuid>, <sha1>, <md5>

				User info:
				- request: <host>
				- ip: <ip>
				- email: <email>

				Downloaded from <url> for <duration>.
			`,
		},
		{
			name: "not_added_pattern",
			inputs: []string{
				"some TRUE here",
			},
			patterns: "int|float|host",
			want:     "some TRUE here",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			n, err := NewTokenNormalizer(TokenNormalizerParams{BuiltinPatterns: tt.patterns})
			require.NoError(t, err)

			out := make([]byte, 0)

			for _, i := range tt.inputs {
				out = n.Normalize(out, []byte(i))
				assert.Equal(t, tt.want, string(out), "wrong out with input=%q", i)
			}
		})
	}
}

func TestTokenNormalizerCustom(t *testing.T) {
	tests := []struct {
		name string

		params TokenNormalizerParams

		inputs  []string
		want    string
		wantErr bool
	}{
		{
			name: "only_custom",
			params: TokenNormalizerParams{
				BuiltinPatterns: "no",
				CustomPatterns: []TokenPattern{
					{
						Placeholder: "<date>",
						RE:          `\d\d.\d\d.\d\d\d\d`,
					},
				},
			},
			inputs: []string{
				`some "asdfasd" and 10.11.2002 here`,
			},
			want: "some \"asdfasd\" and <date> here",
		},
		{
			name: "custom_with_builtin",
			params: TokenNormalizerParams{
				BuiltinPatterns: "all",
				CustomPatterns: []TokenPattern{
					{
						Placeholder: "<nginx_datetime>",
						RE:          `\d\d\d\d/\d\d/\d\d\ \d\d:\d\d:\d\d`,
						Priority:    patternPriorityLast,
					},
				},
			},
			inputs: []string{
				`2006/01/02 15:04:05 error occurred, client: 10.125.172.251, upstream: "http://10.117.246.15:84/download", host: "mpm-youtube-downloader-38.name.com:84"`,
			},
			want: "<nginx_datetime> error occurred, client: <ip>, upstream: <double_quoted>, host: <double_quoted>",
		},
		{
			name:    "empty_patterns",
			params:  TokenNormalizerParams{},
			wantErr: true,
		},
		{
			name: "bad_patterns",
			params: TokenNormalizerParams{
				BuiltinPatterns: "no",
				CustomPatterns: []TokenPattern{
					{
						Placeholder: "test",
						RE:          "[asd",
					},
				},
			},
			wantErr: true,
		},
	}

	out := make([]byte, 0)

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			n, err := NewTokenNormalizer(tt.params)
			require.Equal(t, tt.wantErr, err != nil || n == nil)
			if tt.wantErr {
				return
			}

			for _, i := range tt.inputs {
				out = n.Normalize(out, []byte(i))
				assert.Equal(t, tt.want, string(out), "wrong out with input=%q", i)
			}
		})
	}
}

func genBenchInput(count int) []byte {
	var examples = []string{
		"s1mple falsehood",                         // no match
		"test@host1.host2.com",                     // email
		"http://some.host.com/page1?a=1",           // url
		"hello-world-123.COM",                      // host
		"7c1811ed-e98f-4c9c-a9f9-58c757ff494f",     // uuid
		"a94a8fe5ccb19ba61c4c0873d391e987982fbbd3", // sha1
		"098f6bcd4621d373cade4e832627b4f6",         // md5
		"2025-01-13T10:20:40Z",                     // datetime
		"1.2.3.4",                                  // ip
		"-1.2m5s",                                  // duration
		"0x13eb85e69dfbc0758b12acdaae36287d",       // hex
		"-4.56",                                    // float
		"123",                                      // int
		"truE faLse",
	}

	var sb strings.Builder
	for i := 0; i < count; i++ {
		for _, e := range examples {
			sb.WriteString(fmt.Sprintf(" %s ", e))
		}
	}
	return []byte(sb.String())
}

var benchCases = []struct {
	input []byte
}{
	{input: genBenchInput(1)},
	{input: genBenchInput(10)},
	{input: genBenchInput(100)},
}

func BenchmarkTokenNormalizer(b *testing.B) {
	n, _ := NewTokenNormalizer(TokenNormalizerParams{BuiltinPatterns: "all"})
	out := make([]byte, 0)
	for _, benchCase := range benchCases {
		name := fmt.Sprintf("input_len_%d", len(benchCase.input))
		b.Run(name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				out = n.Normalize(out, benchCase.input)
			}
		})
	}
}
