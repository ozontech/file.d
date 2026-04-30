package transform

import (
	"fmt"
	"strconv"
	"strings"

	insaneJSON "github.com/ozontech/insane-json"
)

type RootTarget struct {
	Root       *insaneJSON.Root
	SourceName string
	metadata   map[string]string

	pathBuffer []string
}

func NewRootTarget(root *insaneJSON.Root, sourceName string, metadata map[string]string) *RootTarget {
	return &RootTarget{
		Root:       root,
		SourceName: sourceName,
		metadata:   metadata,

		pathBuffer: make([]string, 0),
	}
}

func (t *RootTarget) Get(path Path) (Value, error) {
	if path.Root == MetadataRoot {
		return t.getMetadata(path)
	}

	if len(path.Segments) == 0 {
		return jsonNodeToValue(t.Root.Node), nil
	}

	t.pathBuffer = toInsaneJSONPath(path.Segments, t.pathBuffer)
	node := t.Root.Dig(t.pathBuffer...)
	if node == nil {
		return NullValue{}, nil
	}

	return JSONNodeValue{N: node}, nil
}

func (t *RootTarget) Set(path Path, value Value) error {
	if path.Root == MetadataRoot {
		return t.setMetadata(path, value)
	}

	if len(path.Segments) == 0 {
		return fmt.Errorf("set: cannot replace event root")
	}

	t.pathBuffer = toInsaneJSONPath(path.Segments[:len(path.Segments)-1], t.pathBuffer)
	parent := t.Root.Dig(t.pathBuffer...)
	if parent == nil {
		return nil
	}

	encoded, err := valueToJSON(value)
	if err != nil {
		return fmt.Errorf("set %s: %w", formatSegments(path.Segments), err)
	}

	leaf := path.Segments[len(path.Segments)-1]
	if leaf.IsIndex() {
		arr := parent.AsArray()
		idx := resolveIndex(leaf.Idx, len(arr))
		if idx < 0 || idx >= len(arr) {
			return fmt.Errorf("set: index %d out of bounds", leaf.Idx)
		}
		node := arr[idx]
		node.MutateToJSON(t.Root, encoded)
	} else {
		existing := parent.Dig(leaf.Field)
		if existing == nil {
			parent.AddFieldNoAlloc(t.Root, leaf.Field).MutateToJSON(t.Root, encoded)
		} else {
			existing.MutateToJSON(t.Root, encoded)
		}
	}

	return nil
}

func (t *RootTarget) Delete(path Path) error {
	if path.Root == MetadataRoot {
		return t.deleteMetadata(path)
	}

	if len(path.Segments) == 0 {
		return fmt.Errorf("delete: cannot delete event root")
	}

	t.pathBuffer = toInsaneJSONPath(path.Segments, t.pathBuffer)
	node := t.Root.Dig(t.pathBuffer...)
	if node == nil {
		return nil
	}

	node.Suicide()

	return nil
}

func (t *RootTarget) getMetadata(path Path) (Value, error) {
	if len(path.Segments) == 0 {
		obj := make(map[string]Value, len(t.metadata))
		for k, v := range t.metadata {
			obj[k] = StringValue{V: v}
		}
		return ObjectValue{V: obj}, nil
	}

	if len(path.Segments) != 1 || !path.Segments[0].IsField() {
		return NullValue{}, fmt.Errorf("metadata path must be a single field name")
	}

	key := path.Segments[0].Field
	val, ok := t.metadata[key]
	if !ok {
		return NullValue{}, nil
	}
	return StringValue{V: val}, nil
}

func (t *RootTarget) setMetadata(path Path, value Value) error {
	if len(path.Segments) != 1 || !path.Segments[0].IsField() {
		return fmt.Errorf("metadata path must be a single field name")
	}
	s, ok := value.(StringValue)
	if !ok {
		return fmt.Errorf("metadata values must be strings, got %s", value.Kind())
	}
	t.metadata[path.Segments[0].Field] = s.V
	return nil
}

func (t *RootTarget) deleteMetadata(path Path) error {
	if len(path.Segments) != 1 || !path.Segments[0].IsField() {
		return fmt.Errorf("metadata path must be a single field name")
	}
	delete(t.metadata, path.Segments[0].Field)
	return nil
}

func toInsaneJSONPath(segments []Segment, pathBuffer []string) []string {
	lseg := len(segments)
	lpb := len(pathBuffer)

	if lpb < lseg {
		pathBuffer = append(pathBuffer, make([]string, lseg-lpb)...)
	} else {
		pathBuffer = pathBuffer[:lseg]
	}

	for i, seg := range segments {
		if seg.IsField() {
			pathBuffer[i] = seg.Field
			continue
		}
		pathBuffer[i] = strconv.Itoa(seg.Idx)
	}

	return pathBuffer
}

// valueToJSON serialises a Value to a JSON string.
func valueToJSON(v Value) (string, error) {
	switch val := v.(type) {
	case NullValue:
		return "null", nil
	case BoolValue:
		if val.V {
			return "true", nil
		}
		return "false", nil
	case IntegerValue:
		return strconv.FormatInt(val.V, 10), nil
	case FloatValue:
		return strconv.FormatFloat(val.V, 'f', -1, 64), nil
	case StringValue:
		return strconv.Quote(val.V), nil
	case ArrayValue:
		parts := make([]string, len(val.V))
		for i, el := range val.V {
			s, err := valueToJSON(el)
			if err != nil {
				return "", err
			}
			parts[i] = s
		}
		return "[" + strings.Join(parts, ",") + "]", nil
	case ObjectValue:
		parts := make([]string, 0, len(val.V))
		for k, el := range val.V {
			s, err := valueToJSON(el)
			if err != nil {
				return "", err
			}
			parts = append(parts, strconv.Quote(k)+":"+s)
		}
		return "{" + strings.Join(parts, ",") + "}", nil
	case JSONNodeValue:
		node := v.(JSONNodeValue).N
		if node == nil {
			return "null", nil
		}

		return node.EncodeToString(), nil
	}
	return "", fmt.Errorf("cannot serialise %s to JSON", v.Kind())
}

func formatSegments(segs []Segment) string {
	var b strings.Builder
	for _, s := range segs {
		if s.IsIndex() {
			fmt.Fprintf(&b, "[%d]", s.Idx)
		} else {
			fmt.Fprintf(&b, ".%s", s.Field)
		}
	}
	return b.String()
}
