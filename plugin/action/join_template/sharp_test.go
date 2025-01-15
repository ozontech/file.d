package join_template

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type testCase struct {
	s   string
	res bool
}

func TestSharpStart(t *testing.T) {
	tests := []testCase{
		// positive
		{s: "\t  UNHANDLED EXCEPTION     ", res: true},
		{s: "Unhandled exception    ", res: true},
		{s: "  Unhandled exception  ", res: true},
		{s: "    Unhandled exception", res: true},

		// negative
		{s: "Unhandled" + string([]byte{0}) + "exception"},
		{s: "unhandled_exception"},
	}

	for i, test := range tests {
		require.Equal(t, test.res, sharpStartCheck(test.s), i)
	}
}

func TestContainsOnlySpaces(t *testing.T) {
	tests := []testCase{
		{s: "    ", res: true},
		{s: "\t\t", res: true},
		{s: "\n\n", res: true},
		{s: " \n\t ", res: true},
		{s: "", res: true},

		// negative
		{s: "qwe"},
		{s: "a   "},
		{s: "   a"},
		{s: "\n\t a"},
		{s: "a\n\t "},
	}

	for i, test := range tests {
		require.Equal(t, test.res, containsOnlySpaces(test.s), i)
	}
}
