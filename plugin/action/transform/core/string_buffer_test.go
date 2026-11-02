package core

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCallStateStringBufferReusesStorage(t *testing.T) {
	t.Parallel()

	var state CallState
	first := state.AcquireStrings(4)
	copy(first, []string{"first", "second", "third", "fourth"})
	state.ReleaseStrings(first)
	require.Equal(t, make([]string, 4), first)

	second := state.AcquireStrings(1)
	require.Same(t, &first[0], &second[0])
	require.Len(t, second, 1)
	second[0] = "next event"
	second[:cap(second)][3] = "unused tail"
	state.ReleaseStrings(second)
	require.Equal(t, make([]string, 4), first)
}

func TestCallStateStringBufferResetKeepsClearedStorage(t *testing.T) {
	t.Parallel()

	var state CallState
	state.Reset()
	require.Nil(t, state.stringBuffer)

	values := state.AcquireStrings(4)
	values[3] = "event payload"
	state.ReleaseStrings(values)
	// Idle storage is already clear before the context is reset.
	require.Equal(t, make([]string, 4), values)
	state.Reset()
	require.Equal(t, make([]string, 4), values)

	next := state.AcquireStrings(1)
	require.Same(t, &values[0], &next[0])
	state.ReleaseStrings(next)
}

func TestCallStateStringBufferNilState(t *testing.T) {
	t.Parallel()

	var state *CallState
	first := state.AcquireStrings(2)
	first[0] = "first call"
	second := state.AcquireStrings(2)
	second[0] = "second call"
	require.NotSame(t, &first[0], &second[0])
	require.Equal(t, "first call", first[0])

	state.ReleaseStrings(first)
	state.ReleaseStrings(second)
	// A nil state does not retain storage, so releasing it requires no cleanup.
	require.Equal(t, "first call", first[0])
	require.Equal(t, "second call", second[0])
	state.ReleaseStrings(nil)
}

func TestCallStateStringBufferBoundsCapacity(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name         string
		size         int
		wantCapacity int
	}
	tests := []testCase{
		{
			name:         "empty",
			size:         0,
			wantCapacity: 0,
		},
		{
			name:         "at limit",
			size:         maxRetainedStrings,
			wantCapacity: maxRetainedStrings,
		},
		{
			name:         "above limit",
			size:         maxRetainedStrings + 1,
			wantCapacity: 0,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var state CallState
			values := state.AcquireStrings(tt.size)
			require.Len(t, values, tt.size)
			for i := range values {
				values[i] = "event payload"
			}
			state.ReleaseStrings(values)
			if tt.wantCapacity > 0 {
				for _, value := range values {
					require.Empty(t, value)
				}
			}
			require.Equal(t, tt.wantCapacity, cap(state.stringBuffer))
			state.Reset()
			require.Equal(t, tt.wantCapacity, cap(state.stringBuffer))
		})
	}
}

func TestCallStateStringBufferKeepsActiveBorrowsIndependent(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		releaseOuterFirst bool
	}
	tests := []testCase{
		{
			name:              "inner released first",
			releaseOuterFirst: false,
		},
		{
			name:              "outer released first",
			releaseOuterFirst: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var state CallState
			warm := state.AcquireStrings(4)
			state.ReleaseStrings(warm)
			outer := state.AcquireStrings(4)
			outer[0] = "outer call"
			inner := state.AcquireStrings(2)
			inner[0] = "inner call"
			require.NotSame(t, &outer[0], &inner[0])
			require.Equal(t, "outer call", outer[0])

			if tt.releaseOuterFirst {
				state.ReleaseStrings(outer)
				require.Equal(t, "inner call", inner[0])
				state.ReleaseStrings(inner)
			} else {
				state.ReleaseStrings(inner)
				require.Equal(t, "outer call", outer[0])
				state.ReleaseStrings(outer)
			}
			require.Equal(t, make([]string, 4), outer)
			require.Equal(t, make([]string, 2), inner)

			next := state.AcquireStrings(4)
			require.Same(t, &outer[0], &next[0])
			state.ReleaseStrings(next)
		})
	}
}

func TestCallStateStringBufferKeepsSmallerIdleStorage(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name         string
		size         int
		wantCapacity int
	}
	tests := []testCase{
		{
			name:         "larger bounded borrow",
			size:         8,
			wantCapacity: 8,
		},
		{
			name:         "oversized borrow",
			size:         maxRetainedStrings + 1,
			wantCapacity: 2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var state CallState
			small := state.AcquireStrings(2)
			state.ReleaseStrings(small)
			large := state.AcquireStrings(tt.size)
			large[0] = "large event"

			nested := state.AcquireStrings(1)
			require.Same(t, &small[0], &nested[0])
			nested[0] = "nested event"
			require.Equal(t, "large event", large[0])
			state.ReleaseStrings(nested)
			require.Equal(t, "large event", large[0])
			state.ReleaseStrings(large)

			if tt.size <= maxRetainedStrings {
				require.Equal(t, make([]string, tt.size), large)
			}
			require.Equal(t, make([]string, 2), small)
			require.Equal(t, tt.wantCapacity, cap(state.stringBuffer))
		})
	}
}
