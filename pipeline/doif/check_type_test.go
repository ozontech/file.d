package doif

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	insaneJSON "github.com/vitkovskii/insane-json"
)

type testCheckTypeNode struct {
	field  string
	values [][]byte
}

func TestCheckType(t *testing.T) {
	t.Parallel()
	type argsResp struct {
		eventStr string
		want     bool
	}

	tests := []struct {
		name string
		node testCheckTypeNode
		data []argsResp
	}{
		{
			name: "ok_type_obj",
			node: testCheckTypeNode{
				field:  "log",
				values: [][]byte{[]byte("obj")},
			},
			data: []argsResp{
				{
					eventStr: `{"log":{"sublog":"test"}}`,
					want:     true,
				},
				{
					eventStr: `{"log":"test"}`,
					want:     false,
				},
				{
					eventStr: `"test"`,
					want:     false,
				},
			},
		},
		{
			name: "ok_type_arr",
			node: testCheckTypeNode{
				field:  "log",
				values: [][]byte{[]byte("arr")},
			},
			data: []argsResp{
				{
					eventStr: `{"log":[{"sublog":"test"}]}`,
					want:     true,
				},
				{
					eventStr: `{"log":"test"}`,
					want:     false,
				},
				{
					eventStr: `"test"`,
					want:     false,
				},
			},
		},
		{
			name: "ok_type_number",
			node: testCheckTypeNode{
				field:  "log",
				values: [][]byte{[]byte("number")},
			},
			data: []argsResp{
				{
					eventStr: `{"log":123}`,
					want:     true,
				},
				{
					eventStr: `{"log":"test"}`,
					want:     false,
				},
				{
					eventStr: `"test"`,
					want:     false,
				},
			},
		},
		{
			name: "ok_type_string",
			node: testCheckTypeNode{
				field:  "log",
				values: [][]byte{[]byte("string")},
			},
			data: []argsResp{
				{
					eventStr: `{"log":"test"}`,
					want:     true,
				},
				{
					eventStr: `{"log":{"subfield":"test"}}`,
					want:     false,
				},
				{
					eventStr: `"test"`,
					want:     false,
				},
			},
		},
		{
			name: "ok_type_null",
			node: testCheckTypeNode{
				field:  "log",
				values: [][]byte{[]byte("null")},
			},
			data: []argsResp{
				{
					eventStr: `{"log":null}`,
					want:     true,
				},
				{
					eventStr: `{"log":"test"}`,
					want:     false,
				},
				{
					eventStr: `"test"`,
					want:     false,
				},
			},
		},
		{
			name: "ok_type_nil",
			node: testCheckTypeNode{
				field:  "log",
				values: [][]byte{[]byte("nil")},
			},
			data: []argsResp{
				{
					eventStr: `{"not_log":null}`,
					want:     true,
				},
				{
					eventStr: `{"log":"test"}`,
					want:     false,
				},
				{
					eventStr: `"test"`,
					want:     true,
				},
			},
		},
		{
			name: "ok_multiple_types",
			node: testCheckTypeNode{
				field:  "log",
				values: [][]byte{[]byte("obj"), []byte("arr")},
			},
			data: []argsResp{
				{
					eventStr: `{"log":{"subfield":"test"}}`,
					want:     true,
				},
				{
					eventStr: `{"log":[{"subfield":"test"}]}`,
					want:     true,
				},
				{
					eventStr: `{"log":"test"}`,
					want:     false,
				},
				{
					eventStr: `"test"`,
					want:     false,
				},
			},
		},
		{
			name: "ok_root_is_obj_or_arr",
			node: testCheckTypeNode{
				field:  "",
				values: [][]byte{[]byte("obj"), []byte("arr")},
			},
			data: []argsResp{
				{
					eventStr: `{"log":{"subfield":"test"}}`,
					want:     true,
				},
				{
					eventStr: `[{"log":{"subfield":"test"}}]`,
					want:     true,
				},
				{
					eventStr: `"test"`,
					want:     false,
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var eventRoot *insaneJSON.Root
			node, err := NewCheckTypeOpNode(tt.node.field, tt.node.values)
			require.NoError(t, err)
			for _, d := range tt.data {
				if d.eventStr == "" {
					eventRoot = nil
				} else {
					eventRoot, err = insaneJSON.DecodeString(d.eventStr)
					require.NoError(t, err)
				}
				got := node.Check(eventRoot)
				assert.Equal(t, d.want, got, "invalid result for event %q", d.eventStr)
			}
		})
	}
}
