package core

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCallStateReusesArgumentStorage(t *testing.T) {
	t.Parallel()

	var state CallState
	require.Nil(t, state.frames)

	first := state.acquire(4)
	first[0] = StringValue{V: "first"}
	state.release(first)
	require.Zero(t, state.depth)
	require.Equal(t, make([]Value, 4), first)

	second := state.acquire(1)
	require.Same(t, &first[0], &second[0])
	require.Len(t, second, 1)
	require.Equal(t, 4, cap(second))
	second[:cap(second)][3] = StringValue{V: "tail"}
	state.release(second)
	require.Equal(t, make([]Value, 4), first)
}

func TestCallStateCleanupPreservesReturnedValues(t *testing.T) {
	t.Parallel()

	array := ArrayValue{V: []Value{StringValue{V: "array"}}}
	object := ObjectValue{V: map[string]Value{"key": StringValue{V: "object"}}}
	var state CallState
	values := state.acquire(2)
	values[0] = array
	values[1] = object

	state.release(values)
	state.Reset()

	require.Equal(t, []Value{nil, nil}, values)
	require.Equal(t, []Value{StringValue{V: "array"}}, array.V)
	require.Equal(t, map[string]Value{"key": StringValue{V: "object"}}, object.V)
}

func TestCallStateKeepsNestedFramesIndependent(t *testing.T) {
	t.Parallel()

	var state CallState
	active := make([][]Value, maxRetainedCallFrames+2)
	for depth := range active {
		values := state.acquire(1)
		values[0] = IntegerValue{V: int64(depth)}
		active[depth] = values
		for previous := range depth {
			require.Equal(t, IntegerValue{V: int64(previous)}, active[previous][0])
			require.NotSame(t, &active[previous][0], &values[0])
		}
	}

	require.Len(t, state.frames, maxRetainedCallFrames)
	require.Equal(t, maxRetainedCallFrames, cap(state.frames))
	for depth := len(active) - 1; depth >= 0; depth-- {
		state.release(active[depth])
		require.Nil(t, active[depth][0])
	}
	require.Zero(t, state.depth)
	for depth := range maxRetainedCallFrames {
		values := state.acquire(1)
		require.Same(t, &active[depth][0], &values[0])
	}
	for depth := maxRetainedCallFrames - 1; depth >= 0; depth-- {
		state.release(active[depth])
	}
}

func TestCallStateBoundsArgumentCapacity(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name     string
		size     int
		retained bool
	}
	tests := []testCase{
		{
			name:     "empty",
			size:     0,
			retained: true,
		},
		{
			name:     "at limit",
			size:     maxRetainedCallArgs,
			retained: true,
		},
		{
			name:     "above limit",
			size:     maxRetainedCallArgs + 1,
			retained: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var state CallState
			values := state.acquire(tt.size)
			require.Len(t, values, tt.size)
			for i := range values {
				values[i] = StringValue{V: "argument"}
			}
			state.release(values)

			for _, value := range values {
				require.Nil(t, value)
			}
			require.Zero(t, state.depth)
			if tt.retained {
				require.Len(t, state.frames, 1)
				require.Equal(t, tt.size, cap(state.frames[0]))
			} else {
				require.Nil(t, state.frames)
			}
		})
	}
}

func TestCallStateLargeThenSmall(t *testing.T) {
	t.Parallel()

	var state CallState
	small := state.acquire(2)
	state.release(small)
	large := state.acquire(maxRetainedCallArgs + 1)
	large[0] = StringValue{V: "large"}
	nested := state.acquire(3)
	nested[0] = StringValue{V: "nested"}
	state.release(nested)
	state.release(large)

	require.Equal(t, 2, cap(state.frames[0]))
	require.Equal(t, 3, cap(state.frames[1]))
	require.Equal(t, make([]Value, len(large)), large)
	next := state.acquire(1)
	require.Same(t, &small[0], &next[0])
	require.Nil(t, next[0])
	state.release(next)
}

func TestCallStateNestedAfterOversizedFrame(t *testing.T) {
	t.Parallel()

	var state CallState
	large := state.acquire(maxRetainedCallArgs + 1)
	nested := state.acquire(1)
	nested[0] = StringValue{V: "nested"}
	state.release(nested)
	state.release(large)

	require.Len(t, state.frames, 2)
	require.Nil(t, state.frames[0])
	require.Equal(t, 1, cap(state.frames[1]))
	require.Nil(t, nested[0])
	require.Zero(t, state.depth)
}

func TestCallStateResetClearsRetainedCapacity(t *testing.T) {
	t.Parallel()

	var state CallState
	state.Reset()
	require.Nil(t, state.frames)

	first := state.acquire(4)
	second := state.acquire(2)
	state.release(second)
	state.release(first)
	// Reset also clears stale references outside the current frame lengths.
	first[3] = StringValue{V: "first tail"}
	second[1] = StringValue{V: "second tail"}
	state.Reset()

	require.Zero(t, state.depth)
	require.Equal(t, make([]Value, 4), first)
	require.Equal(t, make([]Value, 2), second)
	for _, values := range state.frames {
		require.Empty(t, values)
	}
	next := state.acquire(1)
	require.Same(t, &first[0], &next[0])
	state.release(next)
}
