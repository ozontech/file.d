package runtime

import (
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/ozontech/file.d/plugin/action/transform/stdlib"
	"github.com/stretchr/testify/require"
)

func TestUnpreparedTargetCallUsesCurrentRegistryAndEvent(t *testing.T) {
	t.Parallel()

	expr := &core.CallExpr{
		Name: "exists",
		Args: []core.Argument{{Value: &core.PathExpr{Segments: []core.PathSegment{{Field: "field"}}}}},
	}
	type testCase struct {
		name     string
		registry *stdlib.Registry
		event    map[string]core.Value
		want     bool
		err      string
	}
	tests := []testCase{
		{
			name:     "present null",
			registry: stdlib.GetRegistry(),
			event:    map[string]core.Value{"field": core.NullValue{}},
			want:     true,
		},
		{
			name:     "different registry",
			registry: &stdlib.Registry{},
			err:      `unknown function "exists"`,
		},
		{
			name:     "missing field",
			registry: stdlib.GetRegistry(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			target := NewMapTarget()
			target.event = tt.event
			ctx := NewContext(target, tt.registry)
			result, err := expr.Eval(ctx)
			if tt.err != "" {
				require.ErrorContains(t, err, tt.err)
			} else {
				require.NoError(t, err)
				require.Equal(t, core.BoolValue{V: tt.want}, result)
			}
			require.Nil(t, expr.Plan, "unprepared calls must not cache a context's registry on a shared AST")
		})
	}
}

func TestUnpreparedTargetCallValidatesPaths(t *testing.T) {
	t.Parallel()

	expr := &core.CallExpr{
		Name: "exists",
		Args: []core.Argument{{Value: &core.StringLit{Value: "field"}}},
	}
	ctx := NewContext(NewMapTarget(), stdlib.GetRegistry())
	_, err := expr.Eval(ctx)
	require.ErrorContains(t, err, `argument "path" must be a path`)
}
