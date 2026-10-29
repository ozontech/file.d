package runtime

import (
	"strings"
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/stretchr/testify/require"
)

func TestRootTargetJSONBufferReuseKeepsWrittenValues(t *testing.T) {
	t.Parallel()

	const input = `{"source":{"original_key":"original_value","decimal":1.50,"exponent":1e3,"large":9007199254740993}}`
	root, release := newJSONRoot(t, input)
	defer release()
	target := NewRootTarget(root, "first.log", nil)
	require.Nil(t, target.jsonBuffer)
	require.NoError(t, target.Set(eventPath("scalar"), core.StringValue{V: "first"}))
	require.Nil(t, target.jsonBuffer, "scalar writes should not allocate JSON scratch")

	target.jsonBuffer = make([]byte, 0, 4096)
	backing := target.jsonBuffer[:cap(target.jsonBuffer)]
	literal := core.ObjectValue{V: map[string]core.Value{
		"native_key": core.ArrayValue{V: []core.Value{
			core.StringValue{V: "native_value"},
			core.IntegerValue{V: 9007199254740993},
			core.FloatValue{V: 1.5},
		}},
	}}
	require.NoError(t, target.Set(eventPath("literal"), literal))
	require.NoError(t, target.Set(eventPath("borrowed"), core.JSONNodeValue{N: root.Dig("source")}))
	require.NoError(t, target.Set(eventPath("number_copy"), core.JSONNodeValue{N: root.Dig("source", "decimal")}))
	require.Empty(t, target.jsonBuffer)
	require.Same(t, &backing[0], &target.jsonBuffer[:cap(target.jsonBuffer)][0])

	// Overwrite every byte, including data beyond the last short serialization.
	for i := range backing {
		backing[i] = 'x'
	}
	require.JSONEq(t, `{"native_key":["native_value",9007199254740993,1.5]}`, root.Dig("literal").EncodeToString())
	require.Equal(t, `{"original_key":"original_value","decimal":1.50,"exponent":1e3,"large":9007199254740993}`, root.Dig("borrowed").EncodeToString())
	require.Equal(t, "1.50", root.Dig("number_copy").EncodeToString())
	previousOutput := strings.Clone(root.EncodeToString())

	target.Reset()
	require.Empty(t, target.jsonBuffer)
	require.Same(t, &backing[0], &target.jsonBuffer[:cap(target.jsonBuffer)][0])
	nextRoot, releaseNext := newJSONRoot(t, `{}`)
	defer releaseNext()
	target.Root = nextRoot
	target.SourceName = "next.log"
	for _, text := range []string{strings.Repeat("replacement", 100), "short", "different"} {
		value := core.ObjectValue{V: map[string]core.Value{"replacement_key": core.StringValue{V: text}}}
		require.NoError(t, target.Set(eventPath("next"), value))
		require.Equal(t, text, nextRoot.Dig("next", "replacement_key").AsString())
		require.Equal(t, previousOutput, root.EncodeToString())
	}
	target.Reset()
	require.Equal(t, previousOutput, root.EncodeToString())
	require.JSONEq(t, `{"next":{"replacement_key":"different"}}`, nextRoot.EncodeToString())
}

func TestRootTargetJSONBufferAliasedSources(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name       string
		input      string
		path       core.Path
		sourcePath []string
		wrap       bool
		want       string
	}

	tests := []testCase{
		{
			name:       "self_assignment",
			input:      `{"a":{"key":"value","number":1.50}}`,
			path:       eventPath("a"),
			sourcePath: []string{"a"},
			want:       `{"a":{"key":"value","number":1.50}}`,
		},
		{
			name:       "sibling_insertion",
			input:      `{"a":{"key":"value"}}`,
			path:       eventPath("copy"),
			sourcePath: []string{"a"},
			want:       `{"a":{"key":"value"},"copy":{"key":"value"}}`,
		},
		{
			name:       "parent_into_child",
			input:      `{"a":{"key":"value"}}`,
			path:       eventPath("a", "copy"),
			sourcePath: []string{"a"},
			want:       `{"a":{"key":"value","copy":{"key":"value"}}}`,
		},
		{
			name:       "child_into_parent",
			input:      `{"a":{"child":{"key":"value"},"old":true}}`,
			path:       eventPath("a"),
			sourcePath: []string{"a", "child"},
			want:       `{"a":{"key":"value"}}`,
		},
		{
			name:       "array_into_its_element",
			input:      `{"a":[{"key":"value"},1.50]}`,
			path:       indexedPath("a", 0),
			sourcePath: []string{"a"},
			want:       `{"a":[[{"key":"value"},1.50],1.50]}`,
		},
		{
			name:       "created_parent_is_in_source_snapshot",
			input:      `{"a":{"x":1}}`,
			path:       eventPath("a", "b", "c"),
			sourcePath: []string{"a"},
			want:       `{"a":{"x":1,"b":{"c":{"x":1,"b":{}}}}}`,
		},
		{
			name:       "source_scalar_becomes_parent_before_serialization",
			input:      `{"a":1.50}`,
			path:       eventPath("a", "b"),
			sourcePath: []string{"a"},
			want:       `{"a":{"b":{}}}`,
		},
		{
			name:       "composite_containing_parent_source",
			input:      `{"a":{"key":"value"}}`,
			path:       eventPath("a", "copy"),
			sourcePath: []string{"a"},
			wrap:       true,
			want:       `{"a":{"key":"value","copy":[{"key":"value"},"tail"]}}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			target, release := newTestTarget(t, tc.input, nil)
			defer release()
			var value core.Value = core.JSONNodeValue{N: target.Root.Dig(tc.sourcePath...)}
			if tc.wrap {
				value = core.ArrayValue{V: []core.Value{value, core.StringValue{V: "tail"}}}
			}
			require.NoError(t, target.Set(tc.path, value))
			require.JSONEq(t, tc.want, target.Root.EncodeToString())
			require.NoError(t, target.Set(eventPath("scratch"), core.ArrayValue{V: []core.Value{core.StringValue{V: "overwrite"}}}))
			require.NoError(t, target.Delete(eventPath("scratch")))
			require.JSONEq(t, tc.want, target.Root.EncodeToString())
		})
	}
}

func TestRootTargetJSONBufferErrorOrdering(t *testing.T) {
	t.Parallel()

	unsupported := core.ArrayValue{V: []core.Value{core.StringValue{V: "partial"}, core.RegexValue{}}}
	type testCase struct {
		name    string
		input   string
		path    core.Path
		value   core.Value
		wantErr string
		want    string
	}

	tests := []testCase{
		{
			name:    "parents_created_before_serialization_error",
			input:   `{}`,
			path:    eventPath("created", "child", "leaf"),
			value:   unsupported,
			wantErr: "set .created.child.leaf: cannot serialize regex to JSON",
			want:    `{"created":{"child":{}}}`,
		},
		{
			name:    "scalar_parent_replaced_before_serialization_error",
			input:   `{"parent":42,"keep":true}`,
			path:    eventPath("parent", "leaf"),
			value:   unsupported,
			wantErr: "set .parent.leaf: cannot serialize regex to JSON",
			want:    `{"parent":{},"keep":true}`,
		},
		{
			name:    "serialization_error_precedes_leaf_index_error",
			input:   `{"items":[]}`,
			path:    indexedPath("items", 5),
			value:   unsupported,
			wantErr: "set .items[5]: cannot serialize regex to JSON",
			want:    `{"items":[]}`,
		},
		{
			name:    "leaf_index_error_after_serialization",
			input:   `{"items":[]}`,
			path:    indexedPath("items", 5),
			value:   core.ArrayValue{V: []core.Value{core.StringValue{V: "serialized"}}},
			wantErr: "set: index 5 out of bounds",
			want:    `{"items":[]}`,
		},
		{
			name:  "parent_index_error_precedes_serialization",
			input: `{}`,
			path: core.Path{
				Root:     core.EventRoot,
				Segments: []core.Segment{core.FieldSeg("created"), core.IndexSeg(0), core.FieldSeg("leaf")},
			},
			value:   unsupported,
			wantErr: "set .created[0].leaf: cannot use index [0] on a non-array node",
			want:    `{"created":{}}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			target, release := newTestTarget(t, tc.input, nil)
			defer release()
			require.EqualError(t, target.Set(tc.path, tc.value), tc.wantErr)
			require.JSONEq(t, tc.want, target.Root.EncodeToString())
			require.Empty(t, target.jsonBuffer)
			require.LessOrEqual(t, cap(target.jsonBuffer), maxRetainedJSONBufferSize)

			require.NoError(t, target.Set(eventPath("recovered"), core.ArrayValue{V: []core.Value{core.StringValue{V: "ok"}}}))
			require.Equal(t, `["ok"]`, target.Root.Dig("recovered").EncodeToString())
			require.NoError(t, target.Delete(eventPath("recovered")))
			require.JSONEq(t, tc.want, target.Root.EncodeToString())
		})
	}
}

func TestRootTargetJSONBufferOversizedRetention(t *testing.T) {
	t.Parallel()

	large := core.StringValue{V: strings.Repeat("x", maxRetainedJSONBufferSize)}
	type testCase struct {
		name    string
		path    core.Path
		value   core.Value
		wantErr string
	}

	tests := []testCase{
		{
			name:  "successful_write",
			path:  eventPath("large"),
			value: core.ArrayValue{V: []core.Value{large}},
		},
		{
			name:    "serialization_error",
			path:    eventPath("large"),
			value:   core.ArrayValue{V: []core.Value{large, core.RegexValue{}}},
			wantErr: "cannot serialize regex to JSON",
		},
		{
			name:    "invalid_leaf_index",
			path:    indexedPath("items", 5),
			value:   core.ArrayValue{V: []core.Value{large}},
			wantErr: "index 5 out of bounds",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			target, release := newTestTarget(t, `{"items":[]}`, nil)
			defer release()
			err := target.Set(tc.path, tc.value)
			if tc.wantErr == "" {
				require.NoError(t, err)
				require.Equal(t, large.V, target.Root.Dig("large").AsArray()[0].AsString())
			} else {
				require.ErrorContains(t, err, tc.wantErr)
				require.JSONEq(t, `{"items":[]}`, target.Root.EncodeToString())
			}
			require.Nil(t, target.jsonBuffer, "oversized scratch must be dropped after every composite Set")

			require.NoError(t, target.Set(eventPath("small"), core.ArrayValue{V: []core.Value{core.StringValue{V: "ok"}}}))
			require.NotNil(t, target.jsonBuffer)
			require.Empty(t, target.jsonBuffer)
			require.LessOrEqual(t, cap(target.jsonBuffer), maxRetainedJSONBufferSize)
			require.Equal(t, `["ok"]`, target.Root.Dig("small").EncodeToString())
			capacity := cap(target.jsonBuffer)
			target.Reset()
			require.Empty(t, target.jsonBuffer)
			require.Equal(t, capacity, cap(target.jsonBuffer))
		})
	}
}

func TestRootTargetJSONBufferResetRetention(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name         string
		capacity     int
		wantRetained bool
	}

	tests := []testCase{
		{
			name:         "at_limit",
			capacity:     maxRetainedJSONBufferSize,
			wantRetained: true,
		},
		{
			name:         "over_limit",
			capacity:     maxRetainedJSONBufferSize + 1,
			wantRetained: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			target := NewRootTarget(nil, "", nil)
			target.jsonBuffer = make([]byte, 1, tc.capacity)
			target.Reset()
			require.Empty(t, target.jsonBuffer)
			require.Equal(t, tc.wantRetained, target.jsonBuffer != nil)
			if tc.wantRetained {
				require.Equal(t, tc.capacity, cap(target.jsonBuffer))
			}
		})
	}
}
