package normalize

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTokenNormalizerDefaults(t *testing.T) {
	tests := []struct {
		name string

		inputs []string
		want   string
	}{
		{
			name:   "no_matches",
			inputs: []string{"Falsehood is s1mple"},
			want:   "Falsehood is s1mple",
		},
		{
			name: "email",
			inputs: []string{
				"some test@host.com here",
				"some test@host1.host2.com here",
			},
			want: "some <email> here",
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
			want: "some <url> here",
		},
		{
			name: "host",
			inputs: []string{
				"some hello-world-123.COM here",
				"some www.weather.jp here",
			},
			want: "some <host> here",
		},
		{
			name:   "uuid",
			inputs: []string{"some 7c1811ed-e98f-4c9c-a9f9-58c757ff494f here"},
			want:   "some <uuid> here",
		},
		{
			name:   "sha1",
			inputs: []string{"some a94a8fe5ccb19ba61c4c0873d391e987982fbbd3 here"},
			want:   "some <sha1> here",
		},
		{
			name:   "md5",
			inputs: []string{"some 098f6bcd4621d373cade4e832627b4f6 here"},
			want:   "some <md5> here",
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
			want: "some <datetime> here",
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
			want: "some <ip> here",
		},
		{
			name: "duration",
			inputs: []string{
				"some 1.2m5s here",
				"some -50s20ms10µs here",
				"some 1w2d3h4m5s6ms7us8ns here",
			},
			want: "some <duration> here",
		},
		{
			name: "hex",
			inputs: []string{
				"some 0x13eb85e69dfbc0758b12acdaae36287d here",
				"some 0X553026A59C here",
			},
			want: "some <hex> here",
		},
		{
			name: "float",
			inputs: []string{
				"some 1.23 here",
				"some -4.56 here",
			},
			want: "some <float> here",
		},
		{
			name: "int",
			inputs: []string{
				"some 100 here",
				"some -200 here",
			},
			want: "some <int> here",
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
			want: "some <bool> here",
		},
		{
			name: "all_in",
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
	}

	n, err := NewTokenNormalizer(TokenNormalizerParams{WithDefaults: true})
	require.NoError(t, err)

	out := make([]byte, 0)

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
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
				WithDefaults: false,
				Patterns: []TokenPattern{
					{
						Placeholder: "<quoted_str>",
						RE:          `"[^"]*"`,
					},
					{
						Placeholder: "<date>",
						RE:          `\d\d.\d\d.\d\d\d\d`,
					},
				},
			},
			inputs: []string{
				`some "asldfqb21(!@_$(#@-=12))d" and 10.11.2002 here`,
				`some "10.11.2002" and 10.11.2002 here`,
			},
			want: "some <quoted_str> and <date> here",
		},
		{
			name: "custom_with_defaults",
			params: TokenNormalizerParams{
				WithDefaults: true,
				Patterns: []TokenPattern{
					{
						Placeholder: "<quoted_str>",
						RE:          `"[^"]*"`,
						Priority:    patternPriorityFirst,
					},
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
			want: "<nginx_datetime> error occurred, client: <ip>, upstream: <quoted_str>, host: <quoted_str>",
		},
		{
			name: "empty_patterns",
			params: TokenNormalizerParams{
				WithDefaults: false,
			},
			wantErr: true,
		},
		{
			name: "bad_patterns",
			params: TokenNormalizerParams{
				WithDefaults: false,
				Patterns: []TokenPattern{
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
	n, _ := NewTokenNormalizer(TokenNormalizerParams{WithDefaults: true})
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
