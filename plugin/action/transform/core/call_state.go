package core

// Bound retained argument frames and the idle string scratch buffer.
const (
	maxRetainedCallFrames = 32
	maxRetainedCallArgs   = 64
	maxRetainedStrings    = 64
)

// CallStateProvider exposes execution-local storage to prepared calls.
// Contexts without it keep allocating independent argument slices.
type CallStateProvider interface {
	CallState() *CallState
}

// CallState reuses argument and scratch storage for successive and nested calls.
// It belongs to one execution context and must not be used concurrently.
// The zero value is ready for use.
type CallState struct {
	frames [][]Value
	depth  int

	// Idle storage contains no string references; ReleaseStrings clears it.
	stringBuffer []string
}

// AcquireStrings borrows temporary string slots until ReleaseStrings. The
// caller must not retain the slice or reset the state while it is borrowed.
// Active borrows never share storage. A nil state allocates without reuse.
func (s *CallState) AcquireStrings(size int) []string {
	if s == nil || size > maxRetainedStrings || cap(s.stringBuffer) < size {
		return make([]string, size)
	}
	values := s.stringBuffer[:size]
	s.stringBuffer = nil
	return values
}

// ReleaseStrings clears reusable slots, including unused capacity, and retains
// at most one bounded buffer. Returned values must not refer to these slots.
// Oversized buffers and buffers from nil-state calls are discarded without clearing.
func (s *CallState) ReleaseStrings(values []string) {
	if s == nil || cap(values) > maxRetainedStrings {
		return
	}
	clear(values[:cap(values)])
	if cap(values) > cap(s.stringBuffer) {
		s.stringBuffer = values[:0]
	}
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
