package runtime

import (
	"fmt"
	"slices"

	"github.com/ozontech/file.d/plugin/action/transform/core"
)

func validateRenamePath(path core.Path) error {
	if path.Root != core.EventRoot || len(path.Segments) == 0 {
		return fmt.Errorf("requires event fields, not root or metadata paths")
	}
	if path.Segments[len(path.Segments)-1].IsIndex() {
		return fmt.Errorf("path must end with an object field, not an array index")
	}
	return nil
}

// Paths are borrowed from the caller. Only negative indices need a private
// slice so lookup can normalize them before comparing source and destination.
func copyNegativePath(path core.Path) core.Path {
	for _, segment := range path.Segments {
		if segment.IsIndex() && segment.Idx < 0 {
			path.Segments = slices.Clone(path.Segments)
			break
		}
	}
	return path
}

func overlappingPaths(source, destination core.Path) bool {
	n := min(len(source.Segments), len(destination.Segments))
	return slices.Equal(source.Segments[:n], destination.Segments[:n])
}

// A missing object parent can be created, but missing arrays cannot be grown.
func validateMissingParents(segments []core.Segment) error {
	for _, segment := range segments {
		if segment.IsIndex() {
			return fmt.Errorf("destination array must already exist and its index must be in bounds")
		}
	}
	return nil
}
