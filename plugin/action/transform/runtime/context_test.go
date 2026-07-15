package runtime

import (
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/ozontech/file.d/plugin/action/transform/stdlib"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestContextVarOps(t *testing.T) {
	t.Parallel()

	t.Run("get_missing_returns_false", func(t *testing.T) {
		t.Parallel()

		ctx := NewContext(nil, nil)
		_, ok := ctx.GetVar("undefined")
		assert.False(t, ok)
	})

	t.Run("set_and_get", func(t *testing.T) {
		t.Parallel()

		ctx := NewContext(nil, nil)
		ctx.SetVar("x", core.IntegerValue{V: 42})
		v, ok := ctx.GetVar("x")
		require.True(t, ok)
		assert.Equal(t, core.IntegerValue{V: 42}, v)
	})

	t.Run("overwrite_keeps_last_value", func(t *testing.T) {
		t.Parallel()

		ctx := NewContext(nil, nil)
		ctx.SetVar("y", core.StringValue{V: "first"})
		ctx.SetVar("y", core.StringValue{V: "second"})
		v, ok := ctx.GetVar("y")
		require.True(t, ok)
		assert.Equal(t, core.StringValue{V: "second"}, v)
	})

	t.Run("delete_removes_var", func(t *testing.T) {
		t.Parallel()

		ctx := NewContext(nil, nil)
		ctx.SetVar("z", core.BoolValue{V: true})
		ctx.DeleteVar("z")
		_, ok := ctx.GetVar("z")
		assert.False(t, ok)
	})

	t.Run("delete_missing_is_noop", func(t *testing.T) {
		t.Parallel()

		ctx := NewContext(nil, nil)
		assert.NotPanics(t, func() {
			ctx.DeleteVar("never_existed")
		})
	})
}

func TestContextGetTarget(t *testing.T) {
	t.Parallel()

	root := insaneJSON.Spawn()
	defer insaneJSON.Release(root)
	require.NoError(t, root.DecodeString(`{}`))

	target := NewRootTarget(root, "test.log", map[string]string{})
	ctx := NewContext(target, nil)

	assert.Equal(t, target, ctx.GetTarget())
}

func TestContextCallFuncUnknown(t *testing.T) {
	t.Parallel()

	root := insaneJSON.Spawn()
	defer insaneJSON.Release(root)
	_ = root.DecodeString(`{}`)

	reg := stdlib.GetRegistry()
	target := NewRootTarget(root, "test", map[string]string{})
	ctx := NewContext(target, reg)

	pos := mockPosition{"1:1"}
	_, err := ctx.CallFunc(pos, "nonexistent_fn____", nil, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown function")
}

type mockPosition struct{ s string }

func (p mockPosition) String() string { return p.s }
