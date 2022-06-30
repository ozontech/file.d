package pipeline

import (
	"testing"

	"github.com/stretchr/testify/require"
	insaneJSON "github.com/vitkovskii/insane-json"
)

func TestCreateNestedFieldPositive(t *testing.T) {
	type Args struct {
		Root string
		Path []string
	}
	tests := []struct {
		Name string
		Args Args
		Want string
	}{
		{
			Name: "it works",
			Args: Args{
				Root: "{}",
				Path: []string{"a", "b", "c"},
			},
			Want: `{"a":{"b":{"c":{}}}}`,
		},
		{
			Name: "it works with dots",
			Args: Args{
				Root: "{}",
				Path: []string{"path.to", "my", "file.d"},
			},
			Want: `{"path.to":{"my":{"file.d":{}}}}`,
		},
		{
			Name: "it works with existing objects",
			Args: Args{
				Root: `{"a": {"b":{}}}`,
				Path: []string{"a", "b", "c"},
			},
			Want: `{"a":{"b":{"c":{}}}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			root := insaneJSON.Spawn()
			defer insaneJSON.Release(root)
			require.NoError(t, root.DecodeString(tt.Args.Root))

			_, err := CreateNestedField(root, tt.Args.Path)
			require.NoError(t, err)
			require.Equal(t, tt.Want, root.EncodeToString())
		})
	}
}

func TestCreateNestedFieldNegative(t *testing.T) {
	type Args struct {
		Root string
		Path []string
	}
	tests := []struct {
		Name string
		Args Args
		Want error
	}{
		{
			Name: "err if number",
			Args: Args{
				Root: `{"a":1}`,
				Path: []string{"a", "b", "c"},
			},
			Want: ErrFieldNotObject,
		},
		{
			Name: "err if array is nested",
			Args: Args{
				Root: `{"a":{"b":[]}}`,
				Path: []string{"a", "b", "c"},
			},
			Want: ErrFieldNotObject,
		},
		{
			Name: "err if array is deeply nested",
			Args: Args{
				Root: `{"a":{"b":{"c":[]}}}`,
				Path: []string{"a", "b", "c"},
			},
			Want: ErrFieldNotObject,
		},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			root := insaneJSON.Spawn()
			defer insaneJSON.Release(root)
			require.NoError(t, root.DecodeString(tt.Args.Root))

			_, err := CreateNestedField(root, tt.Args.Path)
			require.ErrorIs(t, err, tt.Want)
		})
	}
}

func TestLevelParsing(t *testing.T) {
	testData := []string{"0", "1", "2", "3", "4", "5", "6", "7"}
	for _, level := range testData {
		expected := ParseLevelAsNumber(level)
		got := ParseLevelAsNumber(ParseLevelAsString(level))

		require.GreaterOrEqual(t, expected, 0)
		require.Equal(t, got, expected)
	}
}
