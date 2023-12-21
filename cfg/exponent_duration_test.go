package cfg

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGetExponentDuration(t *testing.T) {
	tests := []struct {
		attemptNum int
		expected   int64
	}{
		{
			attemptNum: 0,
			expected:   50,
		},
		{
			attemptNum: 1,
			expected:   100,
		},
		{
			attemptNum: 10,
			expected:   51200,
		},
	}

	for _, test := range tests {
		result := GetExponentDuration(50*time.Millisecond, test.attemptNum)
		assert.Equal(t, test.expected, result.Milliseconds())
	}
}
