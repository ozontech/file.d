package pipeline

import (
	"math"

	"github.com/ozontech/file.d/logger"
)

// DeltaWrapper acts as a wrapper around int64
// and returns the difference between
// the new and the old value when a new value is set.
// This is useful for metrics (see the maintenance function).
type DeltaWrapper struct {
	val int64
}

func newDeltaWrapper() *DeltaWrapper {
	return &DeltaWrapper{}
}

// updateValue sets a new value for the wrapper
// and **returns** the difference between
// the current value and the new one.
// Logs an error if the delta is less than 0,
// but doesn't fail, uses the abs(diff).
func (dw *DeltaWrapper) updateValue(newVal int64) float64 {
	diff := float64(newVal - dw.val)
	if diff < 0 {
		logger.Error("delta wrapper diff less than 0!")
	}
	diff = math.Abs(diff)
	dw.val = newVal
	return diff
}

// get returns the current value
func (dw *DeltaWrapper) get() int64 {
	return dw.val
}
