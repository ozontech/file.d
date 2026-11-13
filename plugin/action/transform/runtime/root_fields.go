package runtime

import (
	"fmt"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	insaneJSON "github.com/ozontech/insane-json"
)

func (t *RootTarget) Exists(path core.Path) (bool, error) {
	if path.Root == core.MetadataRoot {
		if len(path.Segments) == 0 {
			return true, nil
		}
		if len(path.Segments) != 1 || !path.Segments[0].IsField() {
			return false, fmt.Errorf("metadata path must be a single field name")
		}
		_, exists := t.metadata[path.Segments[0].Field]
		return exists, nil
	}
	node, _ := t.findFieldPath(path)
	return node != nil, nil
}

// findFieldPath follows typed segments without materializing any JSON values.
// Numeric object keys and array indices remain distinct. Incompatible parents
// and out-of-bounds indices are missing paths, just like absent object fields.
func (t *RootTarget) findFieldPath(path core.Path) (*insaneJSON.Node, core.Path) {
	path = copyNegativePath(path)
	node := t.Root.Node
	for i, segment := range path.Segments {
		if segment.IsIndex() {
			if !node.IsArray() {
				return nil, path
			}
			items := node.AsArray()
			index := resolveIndex(segment.Idx, len(items))
			if index < 0 || index >= len(items) {
				return nil, path
			}
			if segment.Idx < 0 {
				path.Segments[i].Idx = index
			}
			node = items[index]
		} else {
			if !node.IsObject() {
				return nil, path
			}
			node = node.Dig(segment.Field)
			if node == nil {
				return nil, path
			}
		}
	}
	return node, path
}

func (t *RootTarget) Rename(source, destination core.Path, override bool) (bool, error) {
	if err := validateRenamePath(source); err != nil {
		return false, err
	}
	if err := validateRenamePath(destination); err != nil {
		return false, err
	}
	node, source := t.findFieldPath(source)
	if node == nil {
		return false, nil
	}
	existing, destination := t.findFieldPath(destination)
	if overlappingPaths(source, destination) {
		if len(source.Segments) == len(destination.Segments) {
			return false, nil
		}
		return false, fmt.Errorf("source and destination must not contain each other")
	}
	if existing != nil && !override {
		return false, nil
	}
	if err := t.validateRenameParents(destination.Segments[:len(destination.Segments)-1]); err != nil {
		return false, err
	}
	// Use assignment's ownership boundary before detaching the source. In
	// particular, MutateToNode would share composite children with local values
	// that still reference the source node. Set preserves raw numbers and makes
	// composite writes independent without materializing the event root.
	if err := t.Set(destination, core.JSONNodeValue{N: node}); err != nil {
		return false, err
	}
	node.Suicide()
	return true, nil
}

func (t *RootTarget) validateRenameParents(segments []core.Segment) error {
	node := t.Root.Node
	for i, segment := range segments {
		if segment.IsIndex() {
			if !node.IsArray() {
				return fmt.Errorf("destination parent is not an array")
			}
			items := node.AsArray()
			index := resolveIndex(segment.Idx, len(items))
			if index < 0 || index >= len(items) {
				return fmt.Errorf("destination index %d out of bounds", segment.Idx)
			}
			node = items[index]
		} else {
			if !node.IsObject() {
				return fmt.Errorf("destination parent is not an object")
			}
			node = node.Dig(segment.Field)
			if node == nil {
				return validateMissingParents(segments[i+1:])
			}
		}
	}
	if !node.IsObject() {
		return fmt.Errorf("destination parent is not an object")
	}
	return nil
}
