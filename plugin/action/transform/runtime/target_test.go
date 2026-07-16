package runtime

import (
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newJSONRoot(t *testing.T, json string) (*insaneJSON.Root, func()) {
	t.Helper()
	root := insaneJSON.Spawn()
	require.NoError(t, root.DecodeString(json), "bad fixture JSON: %s", json)
	return root, func() { insaneJSON.Release(root) }
}

func newTestTarget(t *testing.T, json string, meta map[string]string) (*RootTarget, func()) {
	t.Helper()
	if meta == nil {
		meta = map[string]string{}
	}
	root, release := newJSONRoot(t, json)
	return NewRootTarget(root, "test.log", meta), release
}

func eventPath(fields ...string) core.Path {
	segs := make([]core.Segment, len(fields))
	for i, f := range fields {
		segs[i] = core.FieldSeg(f)
	}
	return core.Path{Root: core.EventRoot, Segments: segs}
}

func metaPath(field string) core.Path {
	return core.Path{Root: core.MetadataRoot, Segments: []core.Segment{core.FieldSeg(field)}}
}

func indexedPath(field string, idx int) core.Path {
	return core.Path{
		Root:     core.EventRoot,
		Segments: []core.Segment{core.FieldSeg(field), core.IndexSeg(idx)},
	}
}

func TestRootTargetGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		json     string
		path     core.Path
		wantStr  string
		wantKind core.ValueKind
	}{
		{
			name:     "simple_integer_field",
			json:     `{"a": 42}`,
			path:     eventPath("a"),
			wantStr:  "42",
			wantKind: core.KindInteger,
		},
		{
			name:     "string_field",
			json:     `{"msg": "hello"}`,
			path:     eventPath("msg"),
			wantStr:  "hello",
			wantKind: core.KindString,
		},
		{
			name:     "bool_field",
			json:     `{"ok": true}`,
			path:     eventPath("ok"),
			wantStr:  "true",
			wantKind: core.KindBool,
		},
		{
			name:     "nested_field",
			json:     `{"user": {"name": "alice"}}`,
			path:     eventPath("user", "name"),
			wantStr:  "alice",
			wantKind: core.KindString,
		},
		{
			name:     "array_element_by_index",
			json:     `{"tags": ["a", "b", "c"]}`,
			path:     indexedPath("tags", 1),
			wantStr:  "b",
			wantKind: core.KindString,
		},
		{
			name:     "missing_field_returns_null",
			json:     `{"a": 1}`,
			path:     eventPath("missing"),
			wantKind: core.KindNull,
		},
		{
			name:     "deeply_missing_path_returns_null",
			json:     `{}`,
			path:     eventPath("a", "b", "c"),
			wantKind: core.KindNull,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			target, release := newTestTarget(t, tc.json, nil)
			defer release()

			val, err := target.Get(tc.path)
			require.NoError(t, err)
			assert.Equal(t, tc.wantKind, val.Kind())
			if tc.wantStr != "" {
				assert.Equal(t, tc.wantStr, val.String())
			}
		})
	}
}

func TestRootTargetGetEmptyPath(t *testing.T) {
	t.Parallel()

	target, release := newTestTarget(t, `{"a": 1}`, nil)
	defer release()

	path := core.Path{Root: core.EventRoot, Segments: nil}
	val, err := target.Get(path)
	require.NoError(t, err)
	assert.Equal(t, core.KindObject, val.Kind())
}

func TestRootTargetSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		json     string
		path     core.Path
		value    core.Value
		wantJSON string // full event JSON after the Set
		wantErr  string
	}{
		{
			name:     "update_existing_field",
			json:     `{"a":1}`,
			path:     eventPath("a"),
			value:    core.IntegerValue{V: 99},
			wantJSON: `{"a":99}`,
		},
		{
			name:     "create_new_field",
			json:     `{"a":1}`,
			path:     eventPath("b"),
			value:    core.StringValue{V: "new"},
			wantJSON: `{"a":1,"b":"new"}`,
		},
		{
			name:     "set_nested_field",
			json:     `{"user":{"name":"bob","age":30}}`,
			path:     eventPath("user", "name"),
			value:    core.StringValue{V: "alice"},
			wantJSON: `{"user":{"name":"alice","age":30}}`,
		},
		{
			name:     "array_element_positive_index",
			json:     `{"arr":[1,2,3]}`,
			path:     indexedPath("arr", 1),
			value:    core.IntegerValue{V: 99},
			wantJSON: `{"arr":[1,99,3]}`,
		},
		{
			name:     "array_element_negative_index",
			json:     `{"arr":[1,2,3]}`,
			path:     indexedPath("arr", -1),
			value:    core.IntegerValue{V: 77},
			wantJSON: `{"arr":[1,2,77]}`,
		},
		{
			name:     "null_value",
			json:     `{"x":0}`,
			path:     eventPath("x"),
			value:    core.NullValue{},
			wantJSON: `{"x":null}`,
		},
		{
			name:     "bool_value",
			json:     `{"x":0}`,
			path:     eventPath("x"),
			value:    core.BoolValue{V: false},
			wantJSON: `{"x":false}`,
		},
		{
			name:     "string_value",
			json:     `{"x":0}`,
			path:     eventPath("x"),
			value:    core.StringValue{V: "hello"},
			wantJSON: `{"x":"hello"}`,
		},
		{
			name:     "float_value",
			json:     `{"x":0}`,
			path:     eventPath("x"),
			value:    core.FloatValue{V: 1.5},
			wantJSON: `{"x":1.5}`,
		},
		{
			name:     "creates_missing_parents",
			json:     `{}`,
			path:     eventPath("user", "name"),
			value:    core.StringValue{V: "alice"},
			wantJSON: `{"user":{"name":"alice"}}`,
		},
		{
			name:     "creates_deeply_nested_parents",
			json:     `{}`,
			path:     eventPath("a", "b", "c"),
			value:    core.IntegerValue{V: 1},
			wantJSON: `{"a":{"b":{"c":1}}}`,
		},
		{
			name:    "root_error",
			json:    `{}`,
			path:    core.Path{Root: core.EventRoot},
			value:   core.IntegerValue{V: 1},
			wantErr: "cannot replace event root",
		},
		{
			name:    "array_out_of_bounds_error",
			json:    `{"arr":[1]}`,
			path:    indexedPath("arr", 5),
			value:   core.IntegerValue{V: 9},
			wantErr: "out of bounds",
		},
		{
			name:    "array_negative_out_of_bounds_error",
			json:    `{"arr":[1]}`,
			path:    indexedPath("arr", -5),
			value:   core.IntegerValue{V: 9},
			wantErr: "out of bounds",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			target, release := newTestTarget(t, tt.json, nil)
			defer release()

			err := target.Set(tt.path, tt.value)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantJSON, target.Root.EncodeToString())
		})
	}
}

func TestRootTargetDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		json     string
		path     core.Path
		wantJSON string // full event JSON after the Delete
		wantErr  string
	}{
		{
			name:     "existing_field",
			json:     `{"a":1,"b":2}`,
			path:     eventPath("a"),
			wantJSON: `{"b":2}`,
		},
		{
			name:     "nested_field",
			json:     `{"user":{"name":"alice","age":30}}`,
			path:     eventPath("user", "age"),
			wantJSON: `{"user":{"name":"alice"}}`,
		},
		{
			name:     "missing_field_is_noop",
			json:     `{"a":1}`,
			path:     eventPath("gone"),
			wantJSON: `{"a":1}`,
		},
		{
			name:    "root_error",
			json:    `{}`,
			path:    core.Path{Root: core.EventRoot},
			wantErr: "cannot delete event root",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			target, release := newTestTarget(t, tt.json, nil)
			defer release()

			err := target.Delete(tt.path)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantJSON, target.Root.EncodeToString())
		})
	}
}

func TestRootTargetMetadataGet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		meta    map[string]string
		path    core.Path
		want    core.Value
		wantErr string
	}{
		{
			name: "single_field",
			meta: map[string]string{"source": "kafka"},
			path: metaPath("source"),
			want: core.StringValue{V: "kafka"},
		},
		{
			name: "missing_field_returns_null",
			meta: map[string]string{},
			path: metaPath("missing"),
			want: core.NullValue{},
		},
		{
			name: "empty_segments_return_all_metadata",
			meta: map[string]string{"k1": "v1", "k2": "v2"},
			path: core.Path{Root: core.MetadataRoot},
			want: core.ObjectValue{V: map[string]core.Value{
				"k1": core.StringValue{V: "v1"},
				"k2": core.StringValue{V: "v2"},
			}},
		},
		{
			name: "multi_segment_error",
			meta: map[string]string{"a": "b"},
			path: core.Path{
				Root:     core.MetadataRoot,
				Segments: []core.Segment{core.FieldSeg("a"), core.FieldSeg("b")},
			},
			wantErr: "metadata path must be a single field name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			target, release := newTestTarget(t, `{}`, tt.meta)
			defer release()

			got, err := target.Get(tt.path)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestRootTargetMetadataSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		meta     map[string]string
		field    string
		value    core.Value
		wantMeta map[string]string
		wantErr  string
	}{
		{
			name:     "set_new_field",
			meta:     map[string]string{},
			field:    "env",
			value:    core.StringValue{V: "prod"},
			wantMeta: map[string]string{"env": "prod"},
		},
		{
			name:     "overwrite_existing_field",
			meta:     map[string]string{"env": "dev"},
			field:    "env",
			value:    core.StringValue{V: "prod"},
			wantMeta: map[string]string{"env": "prod"},
		},
		{
			name:    "non_string_value_error",
			meta:    map[string]string{},
			field:   "n",
			value:   core.IntegerValue{V: 42},
			wantErr: "metadata values must be strings",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			target, release := newTestTarget(t, `{}`, tt.meta)
			defer release()

			err := target.Set(metaPath(tt.field), tt.value)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantMeta, tt.meta)
		})
	}
}

func TestRootTargetMetadataDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		meta     map[string]string
		field    string
		wantMeta map[string]string
	}{
		{
			name:     "existing_field",
			meta:     map[string]string{"key": "value", "other": "stays"},
			field:    "key",
			wantMeta: map[string]string{"other": "stays"},
		},
		{
			name:     "missing_field_is_noop",
			meta:     map[string]string{"k": "v"},
			field:    "missing",
			wantMeta: map[string]string{"k": "v"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			target, release := newTestTarget(t, `{}`, tt.meta)
			defer release()

			require.NoError(t, target.Delete(metaPath(tt.field)))
			assert.Equal(t, tt.wantMeta, tt.meta)
		})
	}
}

func TestToInsaneJSONPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		segs    []core.Segment
		initBuf []string
		want    []string
	}{
		{
			name: "all_field_segments",
			segs: []core.Segment{core.FieldSeg("a"), core.FieldSeg("b")},
			want: []string{"a", "b"},
		},
		{
			name: "field_and_integer_index",
			segs: []core.Segment{core.FieldSeg("items"), core.IndexSeg(2)},
			want: []string{"items", "2"},
		},
		{
			name: "negative_index_preserved_as_string",
			segs: []core.Segment{core.IndexSeg(-1)},
			want: []string{"-1"},
		},
		{
			name:    "empty_segments",
			segs:    []core.Segment{},
			initBuf: []string{},
			want:    []string{},
		},
		{
			name:    "buffer_grows_when_shorter",
			segs:    []core.Segment{core.FieldSeg("a"), core.FieldSeg("b"), core.FieldSeg("c")},
			initBuf: []string{"x"},
			want:    []string{"a", "b", "c"},
		},
		{
			name:    "buffer_shrinks_when_longer",
			segs:    []core.Segment{core.FieldSeg("a")},
			initBuf: []string{"x", "y", "z"},
			want:    []string{"a"},
		},
		{
			name:    "buffer_reused_same_length",
			segs:    []core.Segment{core.FieldSeg("p"), core.FieldSeg("q")},
			initBuf: []string{"x", "y"},
			want:    []string{"p", "q"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result := toInsaneJSONPath(tc.segs, tc.initBuf)
			assert.Equal(t, tc.want, result)
		})
	}
}

func TestValueToJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		value   core.Value
		want    string
		wantErr bool
	}{

		{"null", core.NullValue{}, "null", false},
		{"bool_true", core.BoolValue{V: true}, "true", false},
		{"bool_false", core.BoolValue{V: false}, "false", false},
		{"integer_positive", core.IntegerValue{V: 42}, "42", false},
		{"integer_negative", core.IntegerValue{V: -7}, "-7", false},
		{"integer_zero", core.IntegerValue{V: 0}, "0", false},
		{"float_basic", core.FloatValue{V: 3.14}, "3.14", false},
		{"float_whole", core.FloatValue{V: 3.0}, "3", false},
		{"float_large", core.FloatValue{V: 1.5e10}, "15000000000", false},
		{"string_simple", core.StringValue{V: "hello"}, `"hello"`, false},
		{"string_with_inner_quotes", core.StringValue{V: `say "hi"`}, `"say \"hi\""`, false},
		{"string_empty", core.StringValue{V: ""}, `""`, false},
		{"array_empty", core.ArrayValue{V: []core.Value{}}, "[]", false},
		{
			"array_ints",
			core.ArrayValue{V: []core.Value{core.IntegerValue{V: 1}, core.IntegerValue{V: 2}}},
			"[1,2]",
			false,
		},
		{
			"object_single_key",
			core.ObjectValue{V: map[string]core.Value{"k": core.IntegerValue{V: 1}}},
			`{"k":1}`,
			false,
		},
		{"json_node_nil", core.JSONNodeValue{N: nil}, "null", false},
		{"regex_error", core.RegexValue{}, "", true},
		{"timestamp_error", core.TimestampValue{}, "", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			result, err := valueToJSON(tc.value)
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "cannot serialize")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, result)
		})
	}

	t.Run("json_node_value_real_int_node", func(t *testing.T) {
		t.Parallel()

		root := insaneJSON.Spawn()
		defer insaneJSON.Release(root)
		require.NoError(t, root.DecodeString(`{"n": 42}`))
		node := root.Dig("n")
		require.NotNil(t, node)

		result, err := valueToJSON(core.JSONNodeValue{N: node})
		require.NoError(t, err)
		assert.Equal(t, "42", result)
	})

	t.Run("array_with_string_elements", func(t *testing.T) {
		t.Parallel()

		arr := core.ArrayValue{V: []core.Value{
			core.StringValue{V: "a"},
			core.StringValue{V: "b"},
		}}
		result, err := valueToJSON(arr)
		require.NoError(t, err)
		assert.Equal(t, `["a","b"]`, result)
	})

	t.Run("nested_array_in_array", func(t *testing.T) {
		t.Parallel()

		inner := core.ArrayValue{V: []core.Value{core.IntegerValue{V: 1}}}
		outer := core.ArrayValue{V: []core.Value{inner, core.IntegerValue{V: 2}}}
		result, err := valueToJSON(outer)
		require.NoError(t, err)
		assert.Equal(t, "[[1],2]", result)
	})
}

func TestFormatSegments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		segs []core.Segment
		want string
	}{
		{"empty", nil, ""},
		{"fields_only", []core.Segment{core.FieldSeg("a"), core.FieldSeg("b")}, ".a.b"},
		{"index_only", []core.Segment{core.IndexSeg(0), core.IndexSeg(2)}, "[0][2]"},
		{"mixed", []core.Segment{core.FieldSeg("arr"), core.IndexSeg(1), core.FieldSeg("name")}, ".arr[1].name"},
		{"negative_index", []core.Segment{core.FieldSeg("x"), core.IndexSeg(-1)}, ".x[-1]"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, formatSegments(tc.segs))
		})
	}
}

func TestResolveIndexRuntime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		idx    int
		length int
		want   int
	}{
		{"first", 0, 5, 0},
		{"second", 1, 5, 1},
		{"last", 4, 5, 4},
		{"one_past_end", 5, 5, 5},
		{"negative_last", -1, 5, 4},
		{"negative_first", -5, 5, 0},
		{"negative_out_of_bounds", -6, 5, -1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, resolveIndex(tc.idx, tc.length))
		})
	}
}
