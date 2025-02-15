package ascii

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestToLower(t *testing.T) {
	f := make([]byte, 1<<8)
	for x := range f {
		f[x] = byte(x)
	}

	f['A'] = 'a'
	for x := 'B'; x <= 'Z'; x++ {
		f[x] = f[x-1] + 1
	}

	for x := range f {
		require.Equal(t, f[x], ToLower(byte(x)))
	}
}
