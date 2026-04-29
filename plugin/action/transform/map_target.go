package transform

import "fmt"

// MapTarget is the standard in-memory Target.
type MapTarget struct {
	event    map[string]Value
	metadata map[string]Value
}

func NewMapTarget() *MapTarget {
	return &MapTarget{
		event:    make(map[string]Value),
		metadata: make(map[string]Value),
	}
}

func NewMapTargetFrom(event map[string]Value) *MapTarget {
	t := NewMapTarget()
	for k, v := range event {
		t.event[k] = v
	}
	return t
}

func (t *MapTarget) Event() map[string]Value {
	out := make(map[string]Value, len(t.event))
	for k, v := range t.event {
		out[k] = v
	}
	return out
}

func (t *MapTarget) Metadata() map[string]Value {
	out := make(map[string]Value, len(t.metadata))
	for k, v := range t.metadata {
		out[k] = v
	}
	return out
}

func (t *MapTarget) rootMap(r PathRoot) map[string]Value {
	if r == MetadataRoot {
		return t.metadata
	}
	return t.event
}

func (t *MapTarget) Get(path Path) (Value, error) {
	root := t.rootMap(path.Root)

	if len(path.Segments) == 0 {
		snap := make(map[string]Value, len(root))
		for k, v := range root {
			snap[k] = v
		}
		return ObjectValue{V: snap}, nil
	}

	var current Value = ObjectValue{V: root}

	for i, seg := range path.Segments {
		if seg.IsIndex() {
			arr, ok := current.(ArrayValue)
			if !ok {
				return NullValue{}, fmt.Errorf(
					"segment %d: cannot index %s with integer", i, current.Kind())
			}
			idx := resolveIndex(seg.Idx, len(arr.V))
			if idx < 0 || idx >= len(arr.V) {
				return NullValue{}, nil
			}
			current = arr.V[idx]
		} else {
			obj, ok := current.(ObjectValue)
			if !ok {
				return NullValue{}, fmt.Errorf(
					"segment %d: cannot access field %q on %s", i, seg.Field, current.Kind())
			}
			val, exists := obj.V[seg.Field]
			if !exists {
				return NullValue{}, nil
			}
			current = val
		}
	}

	return current, nil
}

func (t *MapTarget) Set(path Path, value Value) error {
	root := t.rootMap(path.Root)

	if len(path.Segments) == 0 {
		obj, ok := value.(ObjectValue)
		if !ok {
			return fmt.Errorf(
				"cannot assign %s to root path: value must be an object", value.Kind())
		}
		for k := range root {
			delete(root, k)
		}
		for k, v := range obj.V {
			root[k] = v
		}
		return nil
	}

	return setInMap(root, path.Segments, value)
}

// setInMap recursively writes value into obj along segs.
func setInMap(obj map[string]Value, segs []Segment, value Value) error {
	head, tail := segs[0], segs[1:]

	if head.IsIndex() {
		return fmt.Errorf("cannot use integer index [%d] at object level", head.Idx)
	}

	if len(tail) == 0 {
		obj[head.Field] = value
		return nil
	}

	existing := obj[head.Field]

	if tail[0].IsIndex() {
		// index -> node must be an array.
		var arr []Value
		if a, ok := existing.(ArrayValue); ok {
			arr = make([]Value, len(a.V))
			copy(arr, a.V)
		}
		newArr, err := setInArray(arr, tail, value)
		if err != nil {
			return fmt.Errorf(".%s: %w", head.Field, err)
		}
		obj[head.Field] = ArrayValue{V: newArr}
	} else {
		// field -> node must be an object.
		var child map[string]Value
		if o, ok := existing.(ObjectValue); ok {
			child = make(map[string]Value, len(o.V))
			for k, v := range o.V {
				child[k] = v
			}
		} else {
			child = make(map[string]Value)
		}
		if err := setInMap(child, tail, value); err != nil {
			return fmt.Errorf(".%s: %w", head.Field, err)
		}
		obj[head.Field] = ObjectValue{V: child}
	}

	return nil
}

// setInArray recursively writes value into arr along segs.
func setInArray(arr []Value, segs []Segment, value Value) ([]Value, error) {
	head, tail := segs[0], segs[1:]

	if !head.IsIndex() {
		return nil, fmt.Errorf("cannot access field .%s on array", head.Field)
	}

	idx := resolveIndex(head.Idx, len(arr))
	if idx < 0 {
		return nil, fmt.Errorf("index %d is out of bounds", head.Idx)
	}

	// Grow with nulls if the index exceeds the current length.
	for len(arr) <= idx {
		arr = append(arr, NullValue{})
	}

	if len(tail) == 0 {
		arr[idx] = value
		return arr, nil
	}

	existing := arr[idx]

	if tail[0].IsIndex() {
		var child []Value
		if a, ok := existing.(ArrayValue); ok {
			child = make([]Value, len(a.V))
			copy(child, a.V)
		}
		newChild, err := setInArray(child, tail, value)
		if err != nil {
			return nil, fmt.Errorf("[%d]: %w", head.Idx, err)
		}
		arr[idx] = ArrayValue{V: newChild}
	} else {
		var child map[string]Value
		if o, ok := existing.(ObjectValue); ok {
			child = make(map[string]Value, len(o.V))
			for k, v := range o.V {
				child[k] = v
			}
		} else {
			child = make(map[string]Value)
		}
		if err := setInMap(child, tail, value); err != nil {
			return nil, fmt.Errorf("[%d]: %w", head.Idx, err)
		}
		arr[idx] = ObjectValue{V: child}
	}

	return arr, nil
}

func (t *MapTarget) Delete(path Path) error {
	root := t.rootMap(path.Root)

	if len(path.Segments) == 0 {
		for k := range root {
			delete(root, k)
		}
		return nil
	}

	return deleteFromMap(root, path.Segments)
}

func deleteFromMap(obj map[string]Value, segs []Segment) error {
	head, tail := segs[0], segs[1:]

	if head.IsIndex() {
		return fmt.Errorf("cannot use integer index [%d] at object level", head.Idx)
	}

	if len(tail) == 0 {
		delete(obj, head.Field)
		return nil
	}

	existing, ok := obj[head.Field]
	if !ok {
		return nil
	}

	if tail[0].IsIndex() {
		a, ok := existing.(ArrayValue)
		if !ok {
			return nil
		}
		arr := make([]Value, len(a.V))
		copy(arr, a.V)
		newArr, err := deleteFromArray(arr, tail)
		if err != nil {
			return fmt.Errorf(".%s: %w", head.Field, err)
		}
		obj[head.Field] = ArrayValue{V: newArr}
	} else {
		o, ok := existing.(ObjectValue)
		if !ok {
			return nil
		}
		child := make(map[string]Value, len(o.V))
		for k, v := range o.V {
			child[k] = v
		}
		if err := deleteFromMap(child, tail); err != nil {
			return fmt.Errorf(".%s: %w", head.Field, err)
		}
		obj[head.Field] = ObjectValue{V: child}
	}

	return nil
}

func deleteFromArray(arr []Value, segs []Segment) ([]Value, error) {
	head, tail := segs[0], segs[1:]

	if !head.IsIndex() {
		return arr, fmt.Errorf("cannot access field .%s on array", head.Field)
	}

	idx := resolveIndex(head.Idx, len(arr))
	if idx < 0 || idx >= len(arr) {
		return arr, nil
	}

	if len(tail) == 0 {
		return append(arr[:idx], arr[idx+1:]...), nil
	}

	existing := arr[idx]

	if tail[0].IsIndex() {
		a, ok := existing.(ArrayValue)
		if !ok {
			return arr, nil
		}
		child := make([]Value, len(a.V))
		copy(child, a.V)
		newChild, err := deleteFromArray(child, tail)
		if err != nil {
			return nil, fmt.Errorf("[%d]: %w", head.Idx, err)
		}
		arr[idx] = ArrayValue{V: newChild}
	} else {
		o, ok := existing.(ObjectValue)
		if !ok {
			return arr, nil
		}
		child := make(map[string]Value, len(o.V))
		for k, v := range o.V {
			child[k] = v
		}
		if err := deleteFromMap(child, tail); err != nil {
			return nil, fmt.Errorf("[%d]: %w", head.Idx, err)
		}
		arr[idx] = ObjectValue{V: child}
	}

	return arr, nil
}

// resolveIndex maps a possibly-negative index to an absolute position.
// -1 -> last element, -2 -> second to last, etc.
func resolveIndex(idx, length int) int {
	if idx < 0 {
		idx = length + idx
	}
	return idx
}
