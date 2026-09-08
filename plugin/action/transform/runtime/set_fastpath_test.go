package runtime

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/require"
)

// setViaJSON is what Set did before the scalar fast path: always serialize the
// value and parse it back. It is the reference the fast path must match.
func setViaJSON(t *testing.T, root *insaneJSON.Root, field string, v core.Value) string {
	t.Helper()
	encoded, err := valueToJSON(v)
	require.NoError(t, err)
	node := root.Dig(field)
	if node == nil {
		node = root.AddFieldNoAlloc(root, field)
	}
	node.MutateToJSON(root, encoded)
	return root.EncodeToString()
}

func setViaTarget(t *testing.T, root *insaneJSON.Root, field string, v core.Value) string {
	t.Helper()
	tgt := NewRootTarget(root, "src", map[string]string{})
	require.NoError(t, tgt.Set(core.Path{Segments: []core.Segment{core.FieldSeg(field)}}, v))
	return root.EncodeToString()
}

// Every value the language can assign must land in the event identically
// whether it took the fast path or the JSON round trip.
func TestSetFastPathMatchesJSONPath(t *testing.T) {
	// literal values produced by the language itself
	literals := []core.Value{
		core.NullValue{},
		core.BoolValue{V: true},
		core.BoolValue{V: false},
		core.IntegerValue{V: 0},
		core.IntegerValue{V: -42},
		core.IntegerValue{V: 9007199254740993},
		core.FloatValue{V: 3.14},
		core.FloatValue{V: -0.5},
		core.StringValue{V: ""},
		core.StringValue{V: "plain"},
		core.StringValue{V: `quote " and backslash \`},
		core.StringValue{V: "line\nbreak\ttab"},
		core.StringValue{V: "юникод и эмодзи 🎯"},
		core.StringValue{V: `{"looks":"like json"}`},
		core.StringValue{V: "control\x01char"},
		core.StringValue{V: "\x00null byte"},
		core.ArrayValue{V: []core.Value{core.IntegerValue{V: 1}, core.StringValue{V: "two"}}},
		core.ObjectValue{V: map[string]core.Value{"k": core.StringValue{V: "v"}}},
	}

	for i, v := range literals {
		t.Run(fmt.Sprintf("literal_%d_%s", i, v.Kind()), func(t *testing.T) {
			a, err := insaneJSON.DecodeString(`{"keep":1}`)
			require.NoError(t, err)
			defer insaneJSON.Release(a)

			b, err := insaneJSON.DecodeString(`{"keep":1}`)
			require.NoError(t, err)
			defer insaneJSON.Release(b)

			out := fmt.Sprintf("out %d", i)
			want := setViaJSON(t, a, out, v)
			got := setViaTarget(t, b, out, v)
			require.Equal(t, want, got)
			require.True(t, json.Valid([]byte(got)), "produced invalid JSON: %s", got)
		})
	}

	// values read back out of an event, which is what `.a = .b` produces
	sources := []string{
		`"plain"`,
		`""`,
		`"quote \" and backslash \\"`,
		`"line\nbreak\ttab"`,
		`"юникод и эмодзи 🎯"`,
		`"Aé🎯"`,
		`true`,
		`false`,
		`null`,
		`0`,
		`-42`,
		`1.50`,
		`1e3`,
		`9007199254740993`,
		`{"nested":{"deep":[1,2,3]}}`,
		`[1,"two",null,{"k":"v"}]`,
		`[]`,
		`{}`,
	}

	for i, src := range sources {
		t.Run(fmt.Sprintf("node_%d", i), func(t *testing.T) {
			doc := fmt.Sprintf(`{"keep":1,"src":%s}`, src)

			a, err := insaneJSON.DecodeString(doc)
			require.NoError(t, err)
			defer insaneJSON.Release(a)

			b, err := insaneJSON.DecodeString(doc)
			require.NoError(t, err)
			defer insaneJSON.Release(b)

			require.Equal(t,
				setViaJSON(t, a, "out", core.JSONNodeValue{N: a.Dig("src")}),
				setViaTarget(t, b, "out", core.JSONNodeValue{N: b.Dig("src")}),
				"source %s", src)
		})
	}
}

// A string carrying bytes that are not valid UTF-8 is the one case where the two
// paths still differ, and the split is inside insane-json rather than here:
// escapeString emits \ufffd for an invalid rune, but the shouldEscape check in
// front of it lets a string through untouched when it holds no quote, backslash
// or control byte. So the fast path keeps the original bytes -- which is what the
// rest of file.d does with log data -- while the JSON path sanitizes them.
// Both are well-formed JSON structure; neither loses the event.
func TestSetFastPathInvalidUTF8(t *testing.T) {
	const bad = "bad\xff\xfeutf8"

	a, err := insaneJSON.DecodeString(`{"keep":1}`)
	require.NoError(t, err)
	defer insaneJSON.Release(a)

	b, err := insaneJSON.DecodeString(`{"keep":1}`)
	require.NoError(t, err)
	defer insaneJSON.Release(b)

	viaJSON := setViaJSON(t, a, "out", core.StringValue{V: bad})
	viaFast := setViaTarget(t, b, "out", core.StringValue{V: bad})

	require.Equal(t, `{"keep":1,"out":"bad\ufffd\ufffdutf8"}`, viaJSON)
	require.Equal(t, "{\"keep\":1,\"out\":\"bad\xff\xfeutf8\"}", viaFast)
}

// Overwriting an existing field must behave the same on both paths, including
// when a composite is replaced by a scalar and vice versa.
func TestSetFastPathOverwrite(t *testing.T) {
	cases := []struct {
		doc string
		val core.Value
	}{
		{`{"out":"old"}`, core.StringValue{V: "new"}},
		{`{"out":{"a":1}}`, core.StringValue{V: "new"}},
		{`{"out":[1,2,3]}`, core.NullValue{}},
		{`{"out":"old"}`, core.ArrayValue{V: []core.Value{core.IntegerValue{V: 1}}}},
		{`{"out":null}`, core.BoolValue{V: true}},
	}

	for i, c := range cases {
		t.Run(fmt.Sprintf("overwrite_%d", i), func(t *testing.T) {
			a, err := insaneJSON.DecodeString(c.doc)
			require.NoError(t, err)
			defer insaneJSON.Release(a)

			b, err := insaneJSON.DecodeString(c.doc)
			require.NoError(t, err)
			defer insaneJSON.Release(b)

			require.Equal(t, setViaJSON(t, a, "out", c.val), setViaTarget(t, b, "out", c.val))
		})
	}
}
