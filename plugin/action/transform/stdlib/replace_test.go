package stdlib

import (
	"testing"

	"github.com/ozontech/file.d/plugin/action/transform/core"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplace(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string

		value string
		old   string
		new   string
		count int

		want string
	}{
		{
			name:  "with_count",
			value: "old old old",
			old:   "old",
			new:   "new",
			count: 2,
			want:  "new new old",
		},
		{
			name:  "without_count",
			value: "old old old",
			old:   "old",
			new:   "new",
			want:  "new new new",
		},
		{
			name:  "with_negative_count",
			value: "old old old",
			old:   "old",
			new:   "new",
			count: -10,
			want:  "new new new",
		},
		{
			name:  "empty_new",
			value: "old old old",
			old:   "old",
			new:   "",
			want:  "  ",
		},
		{
			name:  "unicode",
			value: "лук лук лук",
			old:   "лук",
			new:   "лак",
			count: 1,
			want:  "лак лук лук",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			named := map[string]core.Value{}
			if tt.count != 0 {
				named["count"] = core.IntegerValue{V: int64(tt.count)}
			}
			got, err := callFn(
				replace{},
				[]core.Value{str(tt.value), str(tt.old), str(tt.new)},
				named,
			)
			require.NoError(t, err)

			assert.Equal(t, tt.want, got.(core.StringValue).V)
		})
	}
}
