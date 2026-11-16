package runtime

import (
	"fmt"

	"github.com/ozontech/file.d/plugin/action/transform/core"
)

func (t *MapTarget) Exists(path core.Path) (bool, error) {
	if path.Root == core.MetadataRoot && len(path.Segments) > 0 &&
		(len(path.Segments) != 1 || !path.Segments[0].IsField()) {
		return false, fmt.Errorf("metadata path must be a single field name")
	}
	_, exists, _ := t.findFieldPath(path)
	return exists, nil
}

func (t *MapTarget) findFieldPath(path core.Path) (core.Value, bool, core.Path) {
	path = copyNegativePath(path)
	var value core.Value = core.ObjectValue{V: t.rootMap(path.Root)}
	for i, segment := range path.Segments {
		if segment.IsIndex() {
			items, ok := value.(core.ArrayValue)
			if !ok {
				return nil, false, path
			}
			index := resolveIndex(segment.Idx, len(items.V))
			if index < 0 || index >= len(items.V) {
				return nil, false, path
			}
			if segment.Idx < 0 {
				path.Segments[i].Idx = index
			}
			value = items.V[index]
		} else {
			object, ok := value.(core.ObjectValue)
			if !ok {
				return nil, false, path
			}
			value, ok = object.V[segment.Field]
			if !ok {
				return nil, false, path
			}
		}
	}
	return value, true, path
}

func (t *MapTarget) Rename(source, destination core.Path, override bool) (bool, error) {
	if err := validateRenamePath(source); err != nil {
		return false, err
	}
	if err := validateRenamePath(destination); err != nil {
		return false, err
	}
	value, exists, source := t.findFieldPath(source)
	if !exists {
		return false, nil
	}
	_, exists, destination = t.findFieldPath(destination)
	if overlappingPaths(source, destination) {
		if len(source.Segments) == len(destination.Segments) {
			return false, nil
		}
		return false, fmt.Errorf("source and destination must not contain each other")
	}
	if exists && !override {
		return false, nil
	}
	if err := t.validateRenameParents(destination.Segments[:len(destination.Segments)-1]); err != nil {
		return false, err
	}
	if err := t.Set(destination, value); err != nil {
		return false, err
	}
	if err := t.Delete(source); err != nil {
		return false, err
	}
	return true, nil
}

func (t *MapTarget) validateRenameParents(segments []core.Segment) error {
	var value core.Value = core.ObjectValue{V: t.event}
	for i, segment := range segments {
		if segment.IsIndex() {
			items, ok := value.(core.ArrayValue)
			if !ok {
				return fmt.Errorf("destination parent is not an array")
			}
			index := resolveIndex(segment.Idx, len(items.V))
			if index < 0 || index >= len(items.V) {
				return fmt.Errorf("destination index %d out of bounds", segment.Idx)
			}
			value = items.V[index]
		} else {
			object, ok := value.(core.ObjectValue)
			if !ok {
				return fmt.Errorf("destination parent is not an object")
			}
			value, ok = object.V[segment.Field]
			if !ok {
				return validateMissingParents(segments[i+1:])
			}
		}
	}
	if _, ok := value.(core.ObjectValue); !ok {
		return fmt.Errorf("destination parent is not an object")
	}
	return nil
}
