package runtime

import (
	"strconv"
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/ozontech/file.d/plugin/action/transform/stdlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContextResetLazyScopeReuse(t *testing.T) {
	t.Parallel()

	target := NewRootTarget(nil, "", nil)
	registry := stdlib.GetRegistry()
	ctx := NewContext(target, registry)
	require.Nil(t, ctx.scope)
	_, ok := ctx.GetVar("missing")
	require.False(t, ok)
	ctx.DeleteVar("missing")
	ctx.Reset()
	require.Nil(t, ctx.scope)

	ctx.SetVar("previous", core.StringValue{V: "first"})
	scope := ctx.scope
	_, err := ctx.CallFunc(mockPosition{"1:1"}, "unknown_function", nil, nil)
	require.Error(t, err)
	ctx.Reset()
	ctx.Reset()

	assert.Empty(t, scope)
	assert.Same(t, target, ctx.GetTarget())
	assert.Same(t, registry, ctx.registry)
	_, ok = ctx.GetVar("previous")
	assert.False(t, ok)

	ctx.SetVar("next", core.IntegerValue{V: 2})
	assert.Equal(t, core.IntegerValue{V: 2}, scope["next"], "small scopes should be reused")
	value, err := ctx.CallFunc(mockPosition{"2:1"}, "upcase", []core.Value{core.StringValue{V: "next"}}, nil)
	require.NoError(t, err)
	assert.Equal(t, core.StringValue{V: "NEXT"}, value)
}

func TestContextResetScopeRetention(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		entries           int
		deleteBeforeReset bool
		wantRetained      bool
	}

	tests := []testCase{
		{
			name:         "at_limit",
			entries:      maxRetainedScopeEntries,
			wantRetained: true,
		},
		{
			name:         "over_limit",
			entries:      maxRetainedScopeEntries + 1,
			wantRetained: false,
		},
		{
			name:              "over_limit_then_deleted",
			entries:           maxRetainedScopeEntries + 1,
			deleteBeforeReset: true,
			wantRetained:      false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := NewContext(nil, nil)
			for i := range tc.entries {
				ctx.SetVar(strconv.Itoa(i), core.IntegerValue{V: int64(i)})
			}
			if tc.deleteBeforeReset {
				for i := range tc.entries {
					ctx.DeleteVar(strconv.Itoa(i))
				}
			}
			ctx.Reset()
			assert.Empty(t, ctx.scope)
			assert.Zero(t, ctx.scopePeakSize)
			assert.Equal(t, tc.wantRetained, ctx.scope != nil)

			// Repeated writes to one key must not inherit the previous peak or
			// count as new entries in the next event.
			for i := range maxRetainedScopeEntries + 1 {
				ctx.SetVar("next", core.IntegerValue{V: int64(i)})
			}
			ctx.Reset()
			assert.NotNil(t, ctx.scope)
			assert.Empty(t, ctx.scope)
		})
	}
}

func TestRootTargetResetLazyMetadataReuse(t *testing.T) {
	t.Parallel()

	root, release := newJSONRoot(t, `{"message":"first"}`)
	defer release()
	target := NewRootTarget(root, "first.log", nil)
	require.Nil(t, target.metadata)
	require.Nil(t, target.pathBuffer)

	value, err := target.Get(metaPath("missing"))
	require.NoError(t, err)
	assert.Equal(t, core.NullValue{}, value)
	require.NoError(t, target.Delete(metaPath("missing")))
	value, err = target.Get(core.Path{Root: core.MetadataRoot})
	require.NoError(t, err)
	assert.Empty(t, value.(core.ObjectValue).V)
	require.Error(t, target.Set(metaPath("bad"), core.IntegerValue{V: 1}))
	require.Error(t, target.Set(core.Path{Root: core.MetadataRoot}, core.StringValue{V: "bad"}))
	target.Reset()
	require.Nil(t, target.metadata)

	target.Root = root
	target.SourceName = "first.log"
	require.NoError(t, target.Set(metaPath("previous"), core.StringValue{V: "first"}))
	metadata := target.metadata
	_, err = target.Get(eventPath("message"))
	require.NoError(t, err)
	require.Error(t, target.Set(eventPath(), core.StringValue{V: "bad"}))
	target.Reset()
	target.Reset()

	assert.Nil(t, target.Root)
	assert.Empty(t, target.SourceName)
	assert.Empty(t, metadata)
	assert.Empty(t, target.pathBuffer)

	nextRoot, releaseNext := newJSONRoot(t, `{"message":"next"}`)
	defer releaseNext()
	target.Root = nextRoot
	target.SourceName = "next.log"
	require.NoError(t, target.Set(metaPath("next"), core.StringValue{V: "second"}))
	assert.Equal(t, "second", metadata["next"], "small metadata maps should be reused")
	value, err = target.Get(eventPath("message"))
	require.NoError(t, err)
	assert.Equal(t, "next", value.String())
}

func TestRootTargetResetMetadataRetention(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name              string
		entries           int
		supplied          bool
		deleteBeforeReset bool
		wantRetained      bool
	}

	tests := []testCase{
		{
			name:         "at_limit",
			entries:      maxRetainedMetadataEntries,
			wantRetained: true,
		},
		{
			name:         "over_limit",
			entries:      maxRetainedMetadataEntries + 1,
			wantRetained: false,
		},
		{
			name:              "over_limit_then_deleted",
			entries:           maxRetainedMetadataEntries + 1,
			deleteBeforeReset: true,
			wantRetained:      false,
		},
		{
			name:         "supplied_at_limit",
			entries:      maxRetainedMetadataEntries,
			supplied:     true,
			wantRetained: true,
		},
		{
			name:              "supplied_over_limit_then_deleted",
			entries:           maxRetainedMetadataEntries + 1,
			supplied:          true,
			deleteBeforeReset: true,
			wantRetained:      false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var supplied map[string]string
			if tc.supplied {
				supplied = make(map[string]string)
				for i := range tc.entries {
					supplied[strconv.Itoa(i)] = "value"
				}
			}
			target := NewRootTarget(nil, "", supplied)
			if tc.supplied {
				require.NoError(t, target.Delete(metaPath("0")))
				assert.NotContains(t, supplied, "0", "the caller's metadata map must be preserved")
			} else {
				for i := range tc.entries {
					require.NoError(t, target.Set(metaPath(strconv.Itoa(i)), core.StringValue{V: "value"}))
				}
			}
			if tc.deleteBeforeReset {
				for i := range tc.entries {
					require.NoError(t, target.Delete(metaPath(strconv.Itoa(i))))
				}
			}
			target.Reset()
			assert.Empty(t, supplied)
			assert.Empty(t, target.metadata)
			assert.Zero(t, target.metadataPeakSize)
			assert.Equal(t, tc.wantRetained, target.metadata != nil)

			for i := range maxRetainedMetadataEntries + 1 {
				require.NoError(t, target.Set(metaPath("next"), core.StringValue{V: strconv.Itoa(i)}))
			}
			target.Reset()
			assert.NotNil(t, target.metadata)
			assert.Empty(t, target.metadata)
		})
	}
}

func TestRootTargetResetPathBuffer(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name         string
		capacity     int
		wantRetained bool
	}

	tests := []testCase{
		{
			name:         "at_limit",
			capacity:     maxRetainedPathSegments,
			wantRetained: true,
		},
		{
			name:         "over_limit",
			capacity:     maxRetainedPathSegments + 1,
			wantRetained: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			target, release := newTestTarget(t, `{"short":"value"}`, nil)
			defer release()
			root := target.Root
			target.pathBuffer = make([]string, 0, tc.capacity)
			fields := make([]string, tc.capacity)
			for i := range fields {
				fields[i] = "field_" + strconv.Itoa(i)
			}
			_, err := target.Get(eventPath(fields...))
			require.NoError(t, err)
			backing := target.pathBuffer[:cap(target.pathBuffer)]
			_, err = target.Get(eventPath("short"))
			require.NoError(t, err)
			require.Len(t, target.pathBuffer, 1)
			require.NotEmpty(t, backing[len(backing)-1], "the previous path should leave a tail")

			target.Reset()
			assert.Empty(t, target.pathBuffer)
			assert.Equal(t, tc.wantRetained, target.pathBuffer != nil)
			for _, field := range backing {
				assert.Empty(t, field, "reset must clear the entire backing array")
			}
			if tc.wantRetained {
				require.Equal(t, tc.capacity, cap(target.pathBuffer))
			}
			target.Root = root
			_, err = target.Get(eventPath("next"))
			require.NoError(t, err)
			if tc.wantRetained {
				assert.Equal(t, "next", backing[0], "small path buffers should be reused")
			}
			target.Reset()
			assert.NotNil(t, target.pathBuffer)
			assert.LessOrEqual(t, cap(target.pathBuffer), maxRetainedPathSegments)
		})
	}
}

func TestResetPreservesBorrowedValues(t *testing.T) {
	t.Parallel()

	root, release := newJSONRoot(t, `{"payload":{"value":"original"}}`)
	defer release()
	target := NewRootTarget(root, "first.log", nil)
	ctx := NewContext(target, nil)
	object := core.ObjectValue{V: map[string]core.Value{"value": core.StringValue{V: "constant"}}}
	array := core.ArrayValue{V: []core.Value{object}}
	ctx.SetVar("object", object)
	ctx.SetVar("array", array)
	ctx.SetVar("node", core.JSONNodeValue{N: root.Dig("payload")})
	require.NoError(t, target.Set(metaPath("value"), core.StringValue{V: "original"}))
	snapshot, err := target.Get(core.Path{Root: core.MetadataRoot})
	require.NoError(t, err)

	ctx.Reset()
	target.Reset()
	require.NoError(t, target.Set(metaPath("value"), core.StringValue{V: "next"}))
	assert.Equal(t, core.StringValue{V: "constant"}, object.V["value"])
	assert.Equal(t, object, array.V[0])
	assert.Equal(t, core.StringValue{V: "original"}, snapshot.(core.ObjectValue).V["value"])
	assert.JSONEq(t, `{"payload":{"value":"original"}}`, root.EncodeToString())
}
