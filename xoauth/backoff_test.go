package xoauth

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExponentialJitterBackoff(t *testing.T) {
	cases := []struct {
		name     string
		min      time.Duration
		max      time.Duration
		attempts int
	}{
		{
			name:     "1s_1m_10",
			min:      time.Second,
			max:      time.Minute,
			attempts: 10,
		},
		{
			name:     "1m_1h_10",
			min:      time.Minute,
			max:      time.Hour,
			attempts: 10,
		},
		{
			name:     "1s_1m_1000",
			min:      time.Second,
			max:      time.Minute,
			attempts: 1000,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backoff := exponentialJitterBackoff()

			for attempt := range tt.attempts {
				got := backoff(tt.min, tt.max, attempt)
				require.GreaterOrEqual(t, got, tt.min)
				require.LessOrEqual(t, got, tt.max)
			}
		})
	}
}

func TestLinearJitterBackoff(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		min      time.Duration
		max      time.Duration
		attempts int
	}{
		{
			name:     "1s_1m_60",
			min:      time.Second,
			max:      time.Minute,
			attempts: 60,
		},
		{
			name:     "1s_1s_60",
			min:      time.Second,
			max:      time.Second,
			attempts: 60,
		},
		{
			name:     "1s_2s_1000",
			min:      time.Second,
			max:      time.Second,
			attempts: 1000,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backoff := linearJitterBackoff()

			for attempt := range tt.attempts {
				got := backoff(tt.min, tt.max, attempt)
				require.GreaterOrEqual(t, got, tt.min)
				require.LessOrEqual(t, got, int64(tt.max)*int64(attempt+1))
			}
		})
	}
}
