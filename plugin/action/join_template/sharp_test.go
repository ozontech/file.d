package join_template

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
