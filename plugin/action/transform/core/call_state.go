package core

// Bound both retained nesting depth and argument capacity per frame.
const (
	maxRetainedCallFrames = 32
	maxRetainedCallArgs   = 64
)

// CallStateProvider exposes execution-local storage to prepared calls.
// Contexts without it keep allocating independent argument slices.
type CallStateProvider interface {
	CallState() *CallState
}

// CallState reuses argument storage for successive and nested calls.
// It belongs to one execution context and must not be used concurrently.
// The zero value is ready for use.
type CallState struct {
	frames [][]Value
	depth  int
}

func (s *CallState) acquire(size int) []Value {
	depth := s.depth
	s.depth++
	if depth >= maxRetainedCallFrames || size > maxRetainedCallArgs {
		return make([]Value, size)
	}

	if depth >= len(s.frames) {
		if depth >= cap(s.frames) {
			capacity := min(max(depth+1, cap(s.frames)*2), maxRetainedCallFrames)
			frames := make([][]Value, depth+1, capacity)
			copy(frames, s.frames)
			s.frames = frames
		} else {
			s.frames = s.frames[:depth+1]
		}
	}
	values := s.frames[depth]
	if cap(values) < size {
		values = make([]Value, size)
	} else {
		values = values[:size]
	}
	s.frames[depth] = values
	return values
}

func (s *CallState) release(values []Value) {
	// Clear references only; the values themselves may still be used by a
	// returned result or shared with the prepared plan's defaults.
	clear(values[:cap(values)])
	s.depth--
	if s.depth < len(s.frames) {
		s.frames[s.depth] = s.frames[s.depth][:0]
	}
}

// Reset releases references left in retained frames and prepares another run.
// It must be called only after evaluation, when no call is still active.
func (s *CallState) Reset() {
	for i, values := range s.frames {
		clear(values[:cap(values)])
		s.frames[i] = values[:0]
	}
	s.depth = 0
}
