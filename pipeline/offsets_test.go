package pipeline

import "testing"

func TestOffset(t *testing.T) {
	offsets := SliceFromMap(map[StreamName]int64{
		"stream1": 100,
		"stream2": 200,
	})

	offset := NewOffsets(42, offsets)

	// Test Current method
	if got := offset.current; got != 42 {
		t.Errorf("Current() = %v; want 42", got)
	}

	// Test ByStream method for existing stream
	if got := offset.byStream("stream1"); got != 100 {
		t.Errorf("ByStream('stream1') = %v; want 100", got)
	}

	// Test ByStream method for non-existing stream
	if got := offset.byStream("stream3"); got != -1 {
		t.Errorf("ByStream('stream3') = %v; want -1", got)
	}
}
