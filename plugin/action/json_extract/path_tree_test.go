package json_extract

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPathTree(t *testing.T) {
	paths := [][]string{
		{"f1", "f2", "f3", "f4"},
		{"f1", "f5"},
		{"f1", "f2", "f6"},
		{"f7"},
	}
	want := &pathNode{
		children: []*pathNode{
			{
				data: "f1",
				children: []*pathNode{
					{
						data: "f2",
						children: []*pathNode{
							{
								data: "f3",
								children: []*pathNode{
									{
										data:     "f4",
										children: []*pathNode{},
									},
								},
							},
							{
								data:     "f6",
								children: []*pathNode{},
							},
						},
					},
					{
						data:     "f5",
						children: []*pathNode{},
					},
				},
			},
			{
				data:     "f7",
				children: []*pathNode{},
			},
		},
	}

	l := newPathTree()
	for _, p := range paths {
		l.add(p)
	}

	require.Equal(t, want, l.root)
}
