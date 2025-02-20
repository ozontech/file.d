package template

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestContainsOnlySpaces(t *testing.T) {
	positive := []string{
		"    ",
		"\t\t",
		"\n\n",
		" \n\t ",
		"",
	}

	negative := []string{
		"qwe",
		"a   ",
		"   a",
		"\n\t a",
		"a\n\t ",
	}

	for _, tt := range getCases(positive, negative) {
		require.Equal(t, tt.res, containsOnlySpaces(tt.s), tt.s)
	}
}

func TestContainsOnlyDigits(t *testing.T) {
	positive := []string{
		"", "123", "000000",
	}

	negative := []string{
		"qwe",
		"123qwe",
		"    ",
		"000 ",
		" 000",
	}

	for _, tt := range getCases(positive, negative) {
		require.Equal(t, tt.res, containsOnlyDigits(tt.s), tt.s)
	}
}

func TestFirstNonSpaceIndex(t *testing.T) {
	type TestCase struct {
		s   string
		res int
	}

	for _, tt := range []TestCase{
		{"", -1},
		{"\t\n\n  \t \n \t", -1},

		{"qwe", 0},
		{"qwe   ", 0},
		{"QWE   ", 0},
		{"   123", 3},
		{"\n\n \t\tqwe", 5},
	} {
		require.Equal(t, tt.res, firstNonSpaceIndex(tt.s), tt.s)
	}
}
