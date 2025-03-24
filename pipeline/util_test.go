package pipeline

import (
	"testing"

	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/require"
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
		{
			Name: "override array",
			Args: Args{
				Root: `{"a": {"b":[]}}`,
				Path: []string{"a", "b", "c"},
			},
			Want: `{"a":{"b":{"c":{}}}}`,
		},
		{
			Name: "override number",
			Args: Args{
				Root: `{"a": {"b":1}}`,
				Path: []string{"a", "b", "c"},
			},
			Want: `{"a":{"b":{"c":{}}}}`,
		},
		{
			Name: "override null",
			Args: Args{
				Root: `{"a": {"b":null}}`,
				Path: []string{"a", "b", "c"},
			},
			Want: `{"a":{"b":{"c":{}}}}`,
		},
		{
			Name: "override string",
			Args: Args{
				Root: `{"a": {"b":null}}`,
				Path: []string{"a", "b", "c"},
			},
			Want: `{"a":{"b":{"c":{}}}}`,
		},
		{
			Name: "override object",
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

			_ = CreateNestedField(root, tt.Args.Path)
			require.Equal(t, tt.Want, root.EncodeToString())
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
