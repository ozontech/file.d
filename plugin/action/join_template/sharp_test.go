package join_template

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSharpStart(t *testing.T) {
	positive := []string{
		"\t UNHANDLED EXCEPTION    ",
		"Unhandled exception    ",
		"  Unhandled exception  ",
		"    Unhandled exception",
	}

	negative := []string{
		"Unhandled\x00exception",
		"\t unhandled_exception",
		"\t UNHANDLED\nexception",
	}

	for i, test := range getCases(positive, negative) {
		require.Equal(t, test.res, sharpStartCheck(test.s), i)
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
