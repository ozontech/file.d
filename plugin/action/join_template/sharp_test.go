package join_template

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
