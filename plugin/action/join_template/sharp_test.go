package join_template

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSharpStart(t *testing.T) {
	type TestCase struct {
		s       string
		verdict bool
	}

	tests := []TestCase{
		// positive
		{s: "\t\tUNHANDLED EXCEPTION     ", verdict: true},
		{s: "Unhandled exception    ", verdict: true},
		{s: "  Unhandled exception  ", verdict: true},
		{s: "    Unhandled exception", verdict: true},

		// negative
		{s: "Unhandled" + string([]byte{0}) + "exception"},
	}

	for i, test := range tests {
		require.Equal(t, test.verdict, sharpStartCheck(test.s), i)
	}
}
