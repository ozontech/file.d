package runtime

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/ozontech/file.d/plugin/action/transform/core"
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

func (t *RootTarget) Get(path core.Path) (core.Value, error) {
	if path.Root == core.MetadataRoot {
		return t.getMetadata(path)
	}

	if len(path.Segments) == 0 {
		return core.JsonNodeToValue(t.Root.Node), nil
	}

	t.pathBuffer = toInsaneJSONPath(path.Segments, t.pathBuffer)
	node := t.Root.Dig(t.pathBuffer...)
	if node == nil {
		return core.NullValue{}, nil
	}

	return core.JSONNodeValue{N: node}, nil
}

func (t *RootTarget) Set(path core.Path, value core.Value) error {
	if path.Root == core.MetadataRoot {
		return t.setMetadata(path, value)
	}

	if len(path.Segments) == 0 {
		return fmt.Errorf("set: cannot replace event root")
	}

	parent, err := t.digOrCreateParent(path.Segments[:len(path.Segments)-1])
	if err != nil {
		return fmt.Errorf("set %s: %w", formatSegments(path.Segments), err)
	}

	// A composite has to be serialized before the tree is touched below:
	// AddFieldNoAlloc splices into the parent's node chain, and encoding a source
	// node that lives under that same parent must happen while the chain is still
	// intact.
	scalar := isScalarValue(value)
	var encoded string
	if !scalar {
		encoded, err = valueToJSON(value)
		if err != nil {
			return fmt.Errorf("set %s: %w", formatSegments(path.Segments), err)
		}
	}

	leaf := path.Segments[len(path.Segments)-1]
	var node *insaneJSON.Node
	if leaf.IsIndex() {
		arr := parent.AsArray()
		idx := resolveIndex(leaf.Idx, len(arr))
		if idx < 0 || idx >= len(arr) {
			return fmt.Errorf("set: index %d out of bounds", leaf.Idx)
		}
		node = arr[idx]
	} else {
		node = parent.Dig(leaf.Field)
		if node == nil {
			node = parent.AddFieldNoAlloc(t.Root, leaf.Field)
		}
	}

	if scalar {
		setScalar(node, value)
	} else {
		node.MutateToJSON(t.Root, encoded)
	}

	return nil
}

// isScalarValue reports whether value can be written with one of insane-json's
// typed mutators, which only set a node's bits and data.
//
// Everything else goes through valueToJSON plus MutateToJSON, which serializes
// the value and parses it back. That round trip is what makes an assignment cost
// a full encode and re-parse of the subtree, and it is also what draws nodes
// from the root's decoder pool on every assignment.
//
// A number read out of the event is deliberately not treated as scalar: the node
// keeps the literal exactly as it was written ("1.50", "1e3"), and rewriting it
// through MutateToFloat would change what the event carries downstream. Numbers
// produced by the language itself carry no literal, so they stay on the fast path.
func isScalarValue(v core.Value) bool {
	switch val := v.(type) {
	case core.NullValue, core.BoolValue, core.IntegerValue, core.FloatValue, core.StringValue:
		return true
	case core.JSONNodeValue:
		n := val.N
		return n == nil || n.IsNull() || n.IsString() || n.IsTrue() || n.IsFalse()
	}
	return false
}

// setScalar writes a value that isScalarValue accepted.
func setScalar(node *insaneJSON.Node, v core.Value) {
	switch val := v.(type) {
	case core.NullValue:
		node.MutateToNull()
	case core.BoolValue:
		node.MutateToBool(val.V)
	case core.IntegerValue:
		node.MutateToInt64(val.V)
	case core.FloatValue:
		node.MutateToFloat(val.V)
	case core.StringValue:
		node.MutateToString(val.V)
	case core.JSONNodeValue:
		switch n := val.N; {
		case n == nil || n.IsNull():
			node.MutateToNull()
		case n.IsString():
			node.MutateToString(n.AsString())
		case n.IsTrue():
			node.MutateToBool(true)
		default:
			node.MutateToBool(false)
		}
	}
}

// digOrCreateParent walks the given parent segments, creating missing object
// nodes along the way so that nested assignments like `.a.b.c = 1` work even
// when `.a`/`.a.b` do not yet exist. Index segments are not auto-grown: an
// index into a missing or out-of-bounds array is reported as an error.
func (t *RootTarget) digOrCreateParent(segments []core.Segment) (*insaneJSON.Node, error) {
	curr := t.Root.Node

	for _, seg := range segments {
		if seg.IsIndex() {
			if !curr.IsArray() {
				return nil, fmt.Errorf("cannot use index [%d] on a non-array node", seg.Idx)
			}
			arr := curr.AsArray()
			idx := resolveIndex(seg.Idx, len(arr))
			if idx < 0 || idx >= len(arr) {
				return nil, fmt.Errorf("index %d out of bounds", seg.Idx)
			}
			curr = arr[idx]
			continue
		}

		next := curr.Dig(seg.Field)
		if next == nil {
			next = curr.AddFieldNoAlloc(t.Root, seg.Field)
			next.MutateToObject()
		} else if !next.IsObject() && !next.IsArray() {
			// A scalar node blocks the path; replace it with an object so the
			// remaining segments have somewhere to live. Existing arrays/objects
			// are left intact.
			next.MutateToObject()
		}
		curr = next
	}

	return curr, nil
}

func (t *RootTarget) Delete(path core.Path) error {
	if path.Root == core.MetadataRoot {
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

func (t *RootTarget) getMetadata(path core.Path) (core.Value, error) {
	if len(path.Segments) == 0 {
		obj := make(map[string]core.Value, len(t.metadata))
		for k, v := range t.metadata {
			obj[k] = core.StringValue{V: v}
		}
		return core.ObjectValue{V: obj}, nil
	}

	if len(path.Segments) != 1 || !path.Segments[0].IsField() {
		return core.NullValue{}, fmt.Errorf("metadata path must be a single field name")
	}

	key := path.Segments[0].Field
	val, ok := t.metadata[key]
	if !ok {
		return core.NullValue{}, nil
	}
	return core.StringValue{V: val}, nil
}

func (t *RootTarget) setMetadata(path core.Path, value core.Value) error {
	if len(path.Segments) != 1 || !path.Segments[0].IsField() {
		return fmt.Errorf("metadata path must be a single field name")
	}
	s, ok := value.(core.StringValue)
	if !ok {
		return fmt.Errorf("metadata values must be strings, got %s", value.Kind())
	}
	t.metadata[path.Segments[0].Field] = s.V
	return nil
}

func (t *RootTarget) deleteMetadata(path core.Path) error {
	if len(path.Segments) != 1 || !path.Segments[0].IsField() {
		return fmt.Errorf("metadata path must be a single field name")
	}
	delete(t.metadata, path.Segments[0].Field)
	return nil
}

func toInsaneJSONPath(segments []core.Segment, pathBuffer []string) []string {
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

// quoteJSON renders a string as a JSON string literal.
//
// strconv.Quote is Go quoting, not JSON quoting: it escapes a control character
// as \x01 and a byte that is not valid UTF-8 as \xff, neither of which JSON
// accepts. Using it here put malformed JSON into the event whenever a log line
// carried such a byte.
func quoteJSON(s string) string {
	encoded, err := json.Marshal(s)
	if err != nil {
		// json.Marshal only fails on unsupported types, never on a string.
		return strconv.Quote(s)
	}
	return string(encoded)
}

// valueToJSON serializes a core.Value to a JSON string.
func valueToJSON(v core.Value) (string, error) {
	switch val := v.(type) {
	case core.NullValue:
		return "null", nil
	case core.BoolValue:
		if val.V {
			return "true", nil
		}
		return "false", nil
	case core.IntegerValue:
		return strconv.FormatInt(val.V, 10), nil
	case core.FloatValue:
		return strconv.FormatFloat(val.V, 'f', -1, 64), nil
	case core.StringValue:
		return quoteJSON(val.V), nil
	case core.ArrayValue:
		parts := make([]string, len(val.V))
		for i, el := range val.V {
			s, err := valueToJSON(el)
			if err != nil {
				return "", err
			}
			parts[i] = s
		}
		return "[" + strings.Join(parts, ",") + "]", nil
	case core.ObjectValue:
		parts := make([]string, 0, len(val.V))
		for k, el := range val.V {
			s, err := valueToJSON(el)
			if err != nil {
				return "", err
			}
			parts = append(parts, quoteJSON(k)+":"+s)
		}
		return "{" + strings.Join(parts, ",") + "}", nil
	case core.JSONNodeValue:
		node := v.(core.JSONNodeValue).N
		if node == nil {
			return "null", nil
		}

		return node.EncodeToString(), nil
	}
	return "", fmt.Errorf("cannot serialize %s to JSON", v.Kind())
}

func formatSegments(segs []core.Segment) string {
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

func resolveIndex(idx, length int) int {
	if idx < 0 {
		idx = length + idx
	}
	return idx
}
