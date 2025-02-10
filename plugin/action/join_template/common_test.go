package join_template

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
