package join_template

import (
	"regexp"
	"testing"

	"github.com/ozontech/file.d/cfg"
	"github.com/stretchr/testify/require"
)

func BenchmarkSharpStartMixedRes(b *testing.B) {
	template, ok := templates["sharp_exception"]
	require.True(b, ok)

	re := regexp.MustCompile(template.startRePat)
	lines := getLines(contentSharpException)

	b.ResetTimer()
	b.Run("explicit", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, line := range lines {
				sharpStartCheck(line)
			}
		}
	})
	b.Run("regexp", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, line := range lines {
				re.MatchString(line)
			}
		}
	})
}

func BenchmarkSharpContinueMixedRes(b *testing.B) {
	template, ok := templates["sharp_exception"]
	require.True(b, ok)

	re := regexp.MustCompile(template.continueRePat)
	lines := getLines(contentSharpException)

	b.ResetTimer()
	b.Run("explicit", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, line := range lines {
				sharpContinueCheck(line)
			}
		}
	})
	b.Run("regexp", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, line := range lines {
				re.MatchString(line)
			}
		}
	})
}

func BenchmarkSharpStartNegativeRes(b *testing.B) {
	template, ok := templates["sharp_exception"]
	require.True(b, ok)

	re := regexp.MustCompile(template.startRePat)
	lines := getRandLines()

	b.ResetTimer()
	b.Run("explicit", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, line := range lines {
				sharpStartCheck(line)
			}
		}
	})
	b.Run("regexp", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, line := range lines {
				re.MatchString(line)
			}
		}
	})
}

func BenchmarkSharpContinueNegativeRes(b *testing.B) {
	template, ok := templates["sharp_exception"]
	require.True(b, ok)

	re := regexp.MustCompile(template.continueRePat)
	lines := getRandLines()

	b.ResetTimer()
	b.Run("explicit", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, line := range lines {
				sharpContinueCheck(line)
			}
		}
	})
	b.Run("regexp", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			for _, line := range lines {
				re.MatchString(line)
			}
		}
	})
}

func TestSharpSameResults(t *testing.T) {
	template, ok := templates["sharp_exception"]
	require.True(t, ok)

	startRe, err := cfg.CompileRegex(template.startRePat)
	require.NoError(t, err)

	continueRe, err := cfg.CompileRegex(template.continueRePat)
	require.NoError(t, err)

	lines := getLines(contentSharpException)

	for _, line := range lines {
		require.Equal(t, startRe.MatchString(line), sharpStartCheck(line))
		require.Equal(t, continueRe.MatchString(line), sharpContinueCheck(line))
	}
}

func TestSharpStart(t *testing.T) {
	positive := []string{
		"\t\n UNHANDLED EXCEPTION    ",
		"Unhandled exception    ",
		"  Unhandled exception  ",
		"    Unhandled exception",
		"Unhandled exception. Tail.",
	}

	negative := []string{
		"\t\n ", // all characters are spaces

		"Unhandled except", // non-space part is not long enough

		"Unhandled\x00exception",
		"\t unhandled_exception",
		"\t UNHANDLED\nexception",
		"Unhandled expression",
	}

	for i, test := range getCases(positive, negative) {
		require.Equal(t, test.res, sharpStartCheck(test.s), i)
	}
}

func TestContainsAt(t *testing.T) {
	positive := []string{
		"at ",
		"\t\n at ",
		"at\t",
		"at\n",
		"at ",
		"   at Some.Path.F()",
	}

	negative := []string{
		"\t\n ", // all characters are spaces
		"a",     // non-space part is not long enough
		"  at",  // no characters after 'at'

		// char after 'at' is not space
		"  at1",
		"  at_",
		"  atX",
	}

	for i, test := range getCases(positive, negative) {
		require.Equal(t, test.res, containsAt(test.s), i)
	}
}

func TestContainsArrow(t *testing.T) {
	positive := []string{
		"--->",
		"--->  ",
		" ---> ",
		"  --->",
		"\t\t--->QWE",
		" ---> System.Net.Name (10): Resource temporarily unavailable",
	}

	negative := []string{
		"\t\n ", // all characters are spaces

		// non-space part is not long enough
		"qwe", "12",
		"   >",
		"  ->",
		" -->",

		// wrong prefix of non-space part
		"abcd",
		"  ---->  ",
		"--> Some.Do",
		"--> --->",
	}

	for i, test := range getCases(positive, negative) {
		require.Equal(t, test.res, containsArrow(test.s), i)
	}
}

func TestContainsEndOf(t *testing.T) {
	positive := []string{
		"--- End of",
		"--- END OF",
		"--- end of",

		"--- end of    ",
		"  --- end of  ",
		"\t\t--- end of",

		"   --- End of inner exception stack trace ---",
	}

	negative := []string{
		"\t\n ", // all characters are spaces

		// non-space part is not long enough
		"qwe", "1234",
		"--- End   ",
		"  --- End ",

		// wrong prefix of non-space part
		"0123456789",
		"--- End ab",
		"--- End ab" + endOfSubstr,
	}

	for i, test := range getCases(positive, negative) {
		require.Equal(t, test.res, containsEndOf(test.s), i)
	}
}

func TestContainsException(t *testing.T) {
	positive := []string{
		"A.Exception:",
		"1.Exception:",
		"_.Exception:",

		"AException:",
		"1Exception:",
		"_Exception:",

		"AB.Exception:",
		"\t \t AB.Exception:",
	}

	negative := []string{
		// no 'Exception:' substr found
		"", "QWE", "123",
		"   Exc",
		"Except",
		"Exception",

		// 'Exception:' found at start
		"Exception:",

		// not found characters before dot
		".Exception:",

		// wrong character before dot
		"@.Exception:",
		" .Exception:",

		// no dot and wrong character
		"@Exception:",
		" Exception:",

		"@.Exception: ... A.Exception:", // only first occurrence counts
	}

	for i, test := range getCases(positive, negative) {
		require.Equal(t, test.res, containsException(test.s), i)
	}
}

func TestEqualCaseInsensitive(t *testing.T) {
	type TestCase struct {
		a, b string
		res  bool
	}

	for _, tt := range []TestCase{
		{"", "", true},
		{"abc", "abc", true},
		{"12", "12", true},
		{"A", "a", true},
		{"a b c", "A B C", true},

		{"qwe", "QWE", true},
		{"QWE", "qwe", true},

		{"some", "SoMe", true},
		{"sOmE", "SoMe", true},

		{
			"=1  A (sd) f Gh !",
			"=1  a (Sd) F gh !",
			true,
		},

		// negative
		{a: "abc", b: "abcd"},

		{a: "ABC", b: "abd"},
		{a: "abd", b: "ABC"},

		{a: "abc D", b: "abc X"},
		{a: "abc D", b: "abc @"},
		{a: "abc D", b: "abc 1"},

		{
			a: "=1  A (sd) f Gh !",
			b: "=1  a (Sd) 1 gh !",
		},
	} {
		require.Equal(t, tt.res, equalCaseInsensitive(tt.a, tt.b), "%s; %s", tt.a, tt.b)
	}
}
