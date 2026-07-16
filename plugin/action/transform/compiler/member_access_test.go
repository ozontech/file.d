package compiler

import (
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemberAccess(t *testing.T) {
	t.Parallel()

	t.Run("ident_dot_field_is_index_expr", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `m.level`)
		idx, ok := expr.(*core.IndexExpr)
		require.True(t, ok, "expected IndexExpr, got %T", expr)

		obj, ok := idx.Object.(*core.IdentExpr)
		require.True(t, ok)
		assert.Equal(t, "m", obj.Name)

		key, ok := idx.Index.(*core.StringLit)
		require.True(t, ok)
		assert.Equal(t, "level", key.Value)
	})

	t.Run("chained_member_access", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `m.a.b`)
		outer, ok := expr.(*core.IndexExpr)
		require.True(t, ok)
		key, ok := outer.Index.(*core.StringLit)
		require.True(t, ok)
		assert.Equal(t, "b", key.Value)

		inner, ok := outer.Object.(*core.IndexExpr)
		require.True(t, ok)
		innerKey, ok := inner.Index.(*core.StringLit)
		require.True(t, ok)
		assert.Equal(t, "a", innerKey.Value)
	})

	t.Run("member_access_on_call_result", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `upcase("x").len`)
		idx, ok := expr.(*core.IndexExpr)
		require.True(t, ok)
		_, ok = idx.Object.(*core.CallExpr)
		require.True(t, ok)
	})

	t.Run("member_access_string_key", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `m."key with spaces"`)
		idx, ok := expr.(*core.IndexExpr)
		require.True(t, ok)
		key, ok := idx.Index.(*core.StringLit)
		require.True(t, ok)
		assert.Equal(t, "key with spaces", key.Value)
	})

	t.Run("member_access_is_assignable", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `m.level = "INFO"`)
		asg, ok := expr.(*core.AssignExpr)
		require.True(t, ok)
		_, ok = asg.Target.(*core.IndexExpr)
		require.True(t, ok)
	})

	t.Run("dot_on_new_line_starts_event_path", func(t *testing.T) {
		t.Parallel()

		exprs := compileN(t, "x = m\n.level = 1")
		require.Len(t, exprs, 2, "newline dot must start a new statement")

		_, ok := exprs[0].(*core.AssignExpr)
		require.True(t, ok)

		second, ok := exprs[1].(*core.AssignExpr)
		require.True(t, ok)
		_, ok = second.Target.(*core.PathExpr)
		require.True(t, ok, "second statement must target an event path")
	})

	t.Run("event_path_unaffected", func(t *testing.T) {
		t.Parallel()

		expr := compile(t, `.a.b.c`)
		path, ok := expr.(*core.PathExpr)
		require.True(t, ok, "expected PathExpr, got %T", expr)
		require.Len(t, path.Segments, 3)
	})

	t.Run("missing_field_after_dot_error", func(t *testing.T) {
		t.Parallel()

		err := mustFail(t, `m.`)
		assert.Contains(t, err.Error(), "expected field name after '.'")
	})
}
