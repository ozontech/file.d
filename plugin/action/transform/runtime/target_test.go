package runtime

import (
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newJSONRoot(t *testing.T, json string) *insaneJSON.Root {
	t.Helper()
	root := insaneJSON.Spawn()
	require.NoError(t, root.DecodeString(json), "bad fixture JSON: %s", json)
	return root
}

func newTestTarget(t *testing.T, json string, meta map[string]string) (*RootTarget, *insaneJSON.Root) {
	t.Helper()
	if meta == nil {
		meta = map[string]string{}
	}
	root := newJSONRoot(t, json)
	return NewRootTarget(root, "test.log", meta), root
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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			target, root := newTestTarget(t, tc.json, nil)
			defer insaneJSON.Release(root)

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
	target, root := newTestTarget(t, `{"a": 1}`, nil)
	defer insaneJSON.Release(root)

	path := core.Path{Root: core.EventRoot, Segments: nil}
	val, err := target.Get(path)
	require.NoError(t, err)
	assert.Equal(t, core.KindObject, val.Kind())
}

func TestRootTargetSet(t *testing.T) {
	t.Run("update_existing_field", func(t *testing.T) {
		target, root := newTestTarget(t, `{"a": 1}`, nil)
		defer insaneJSON.Release(root)

		require.NoError(t, target.Set(eventPath("a"), core.IntegerValue{V: 99}))
		assert.Equal(t, "99", root.Dig("a").AsString())
	})

	t.Run("create_new_field", func(t *testing.T) {
		target, root := newTestTarget(t, `{"a": 1}`, nil)
		defer insaneJSON.Release(root)

		require.NoError(t, target.Set(eventPath("b"), core.StringValue{V: "new"}))
		require.NotNil(t, root.Dig("b"))
		assert.Equal(t, "new", root.Dig("b").AsString())
	})

	t.Run("set_nested_field", func(t *testing.T) {
		target, root := newTestTarget(t, `{"user": {"name": "bob", "age": 30}}`, nil)
		defer insaneJSON.Release(root)

		require.NoError(t, target.Set(eventPath("user", "name"), core.StringValue{V: "alice"}))
		assert.Equal(t, "alice", root.Dig("user", "name").AsString())
		assert.Equal(t, "30", root.Dig("user", "age").AsString())
	})

	t.Run("set_array_element_positive_index", func(t *testing.T) {
		target, root := newTestTarget(t, `{"arr": [1, 2, 3]}`, nil)
		defer insaneJSON.Release(root)

		require.NoError(t, target.Set(indexedPath("arr", 1), core.IntegerValue{V: 99}))
		assert.Equal(t, "99", root.Dig("arr", "1").AsString())
		assert.Equal(t, "1", root.Dig("arr", "0").AsString())
		assert.Equal(t, "3", root.Dig("arr", "2").AsString())
	})

	t.Run("set_array_element_negative_index", func(t *testing.T) {
		target, root := newTestTarget(t, `{"arr": [1, 2, 3]}`, nil)
		defer insaneJSON.Release(root)

		require.NoError(t, target.Set(indexedPath("arr", -1), core.IntegerValue{V: 77}))
		assert.Equal(t, "77", root.Dig("arr", "2").AsString())
	})

	t.Run("set_various_value_types", func(t *testing.T) {
		vals := []struct {
			name    string
			value   core.Value
			wantStr string
		}{
			{"null", core.NullValue{}, "null"},
			{"bool", core.BoolValue{V: false}, "false"},
			{"string", core.StringValue{V: "hello"}, "hello"},
			{"float", core.FloatValue{V: 1.5}, "1.5"},
		}
		for _, v := range vals {
			v := v
			t.Run(v.name, func(t *testing.T) {
				target, root := newTestTarget(t, `{"x": 0}`, nil)
				defer insaneJSON.Release(root)

				require.NoError(t, target.Set(eventPath("x"), v.value))
				assert.Equal(t, v.wantStr, root.Dig("x").AsString())
			})
		}
	})

	t.Run("set_root_error", func(t *testing.T) {
		target, root := newTestTarget(t, `{}`, nil)
		defer insaneJSON.Release(root)

		err := target.Set(core.Path{Root: core.EventRoot}, core.IntegerValue{V: 1})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot replace event root")
	})

	t.Run("set_parent_missing_is_silent_noop", func(t *testing.T) {
		target, root := newTestTarget(t, `{}`, nil)
		defer insaneJSON.Release(root)

		err := target.Set(eventPath("user", "name"), core.StringValue{V: "alice"})
		require.NoError(t, err)
		assert.Nil(t, root.Dig("user"))
	})

	t.Run("set_array_out_of_bounds_error", func(t *testing.T) {
		target, root := newTestTarget(t, `{"arr": [1]}`, nil)
		defer insaneJSON.Release(root)

		err := target.Set(indexedPath("arr", 5), core.IntegerValue{V: 9})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "out of bounds")
	})

	t.Run("set_array_negative_out_of_bounds_error", func(t *testing.T) {
		target, root := newTestTarget(t, `{"arr": [1]}`, nil)
		defer insaneJSON.Release(root)

		err := target.Set(indexedPath("arr", -5), core.IntegerValue{V: 9})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "out of bounds")
	})
}

func TestRootTargetDelete(t *testing.T) {
	t.Run("delete_existing_field", func(t *testing.T) {
		target, root := newTestTarget(t, `{"a": 1, "b": 2}`, nil)
		defer insaneJSON.Release(root)

		require.NoError(t, target.Delete(eventPath("a")))
		assert.Nil(t, root.Dig("a"), "deleted field must be gone")
		assert.NotNil(t, root.Dig("b"), "sibling field must survive")
	})

	t.Run("delete_nested_field", func(t *testing.T) {
		target, root := newTestTarget(t, `{"user": {"name": "alice", "age": 30}}`, nil)
		defer insaneJSON.Release(root)

		require.NoError(t, target.Delete(eventPath("user", "age")))
		assert.Nil(t, root.Dig("user", "age"))
		assert.Equal(t, "alice", root.Dig("user", "name").AsString())
	})

	t.Run("delete_missing_field_is_noop", func(t *testing.T) {
		target, root := newTestTarget(t, `{"a": 1}`, nil)
		defer insaneJSON.Release(root)

		require.NoError(t, target.Delete(eventPath("gone")))
		assert.NotNil(t, root.Dig("a"))
	})

	t.Run("delete_root_error", func(t *testing.T) {
		target, root := newTestTarget(t, `{}`, nil)
		defer insaneJSON.Release(root)

		err := target.Delete(core.Path{Root: core.EventRoot})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "cannot delete event root")
	})
}

func TestRootTargetMetadata(t *testing.T) {
	t.Run("get_single_field", func(t *testing.T) {
		meta := map[string]string{"source": "kafka"}
		target, root := newTestTarget(t, `{}`, meta)
		defer insaneJSON.Release(root)

		val, err := target.Get(metaPath("source"))
		require.NoError(t, err)
		assert.Equal(t, "kafka", val.String())
	})

	t.Run("get_missing_metadata_returns_null", func(t *testing.T) {
		target, root := newTestTarget(t, `{}`, map[string]string{})
		defer insaneJSON.Release(root)

		val, err := target.Get(metaPath("missing"))
		require.NoError(t, err)
		assert.Equal(t, core.KindNull, val.Kind())
	})

	t.Run("get_all_metadata_with_empty_segments", func(t *testing.T) {
		meta := map[string]string{"k1": "v1", "k2": "v2"}
		target, root := newTestTarget(t, `{}`, meta)
		defer insaneJSON.Release(root)

		path := core.Path{Root: core.MetadataRoot}
		val, err := target.Get(path)
		require.NoError(t, err)
		require.Equal(t, core.KindObject, val.Kind())
		obj := val.(core.ObjectValue)
		require.Len(t, obj.V, 2)
		assert.Equal(t, core.StringValue{V: "v1"}, obj.V["k1"])
		assert.Equal(t, core.StringValue{V: "v2"}, obj.V["k2"])
	})

	t.Run("get_multi_segment_metadata_error", func(t *testing.T) {
		target, root := newTestTarget(t, `{}`, map[string]string{"a": "b"})
		defer insaneJSON.Release(root)

		path := core.Path{Root: core.MetadataRoot, Segments: []core.Segment{core.FieldSeg("a"), core.FieldSeg("b")}}
		_, err := target.Get(path)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "metadata path must be a single field name")
	})

	t.Run("set_metadata_string", func(t *testing.T) {
		meta := map[string]string{}
		target, root := newTestTarget(t, `{}`, meta)
		defer insaneJSON.Release(root)

		require.NoError(t, target.Set(metaPath("env"), core.StringValue{V: "prod"}))
		assert.Equal(t, "prod", meta["env"])
	})

	t.Run("overwrite_existing_metadata", func(t *testing.T) {
		meta := map[string]string{"env": "dev"}
		target, root := newTestTarget(t, `{}`, meta)
		defer insaneJSON.Release(root)

		require.NoError(t, target.Set(metaPath("env"), core.StringValue{V: "prod"}))
		assert.Equal(t, "prod", meta["env"])
	})

	t.Run("set_metadata_non_string_error", func(t *testing.T) {
		target, root := newTestTarget(t, `{}`, map[string]string{})
		defer insaneJSON.Release(root)

		err := target.Set(metaPath("n"), core.IntegerValue{V: 42})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "metadata values must be strings")
	})

	t.Run("delete_metadata_field", func(t *testing.T) {
		meta := map[string]string{"key": "value", "other": "stays"}
		target, root := newTestTarget(t, `{}`, meta)
		defer insaneJSON.Release(root)

		require.NoError(t, target.Delete(metaPath("key")))
		_, ok := meta["key"]
		assert.False(t, ok)
		assert.Equal(t, "stays", meta["other"])
	})

	t.Run("delete_missing_metadata_is_noop", func(t *testing.T) {
		meta := map[string]string{"k": "v"}
		target, root := newTestTarget(t, `{}`, meta)
		defer insaneJSON.Release(root)

		require.NoError(t, target.Delete(metaPath("missing")))
		assert.Equal(t, "v", meta["k"])
	})
}

func TestToInsaneJSONPath(t *testing.T) {
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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			result := toInsaneJSONPath(tc.segs, tc.initBuf)
			assert.Equal(t, tc.want, result)
		})
	}
}

func TestValueToJSON(t *testing.T) {
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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
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
		arr := core.ArrayValue{V: []core.Value{
			core.StringValue{V: "a"},
			core.StringValue{V: "b"},
		}}
		result, err := valueToJSON(arr)
		require.NoError(t, err)
		assert.Equal(t, `["a","b"]`, result)
	})

	t.Run("nested_array_in_array", func(t *testing.T) {
		inner := core.ArrayValue{V: []core.Value{core.IntegerValue{V: 1}}}
		outer := core.ArrayValue{V: []core.Value{inner, core.IntegerValue{V: 2}}}
		result, err := valueToJSON(outer)
		require.NoError(t, err)
		assert.Equal(t, "[[1],2]", result)
	})
}

func TestFormatSegments(t *testing.T) {
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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, formatSegments(tc.segs))
		})
	}
}

func TestResolveIndexRuntime(t *testing.T) {
	tests := []struct {
		idx    int
		length int
		want   int
	}{
		{0, 5, 0},
		{1, 5, 1},
		{4, 5, 4},
		{5, 5, 5},
		{-1, 5, 4},
		{-5, 5, 0},
		{-6, 5, -1},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(
			func() string {
				if tc.idx < 0 {
					return "idx_neg_" + string(rune('0'-tc.idx)) + "_len_" + string(rune('0'+tc.length))
				}
				return "idx_" + string(rune('0'+tc.idx)) + "_len_" + string(rune('0'+tc.length))
			}(),
			func(t *testing.T) {
				assert.Equal(t, tc.want, resolveIndex(tc.idx, tc.length))
			},
		)
	}
}
