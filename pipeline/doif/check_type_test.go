package doif

import (
	"testing"

	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestCheckTypeDuplicateValues(t *testing.T) {
	t.Parallel()

	logsMap := map[checkTypeVal]string{
		checkTypeObj:    `{"log":{"sublog":"test"}}`,
		checkTypeArr:    `{"log":[{"sublog":"test"}]}`,
		checkTypeNumber: `{"log":123}`,
		checkTypeString: `{"log":"test"}`,
		checkTypeNull:   `{"log":null}`,
		checkTypeNil:    `{"notlog":"test"}`,
	}

	type argsResp struct {
		checkType checkTypeVal
		want      bool
	}

	tests := []struct {
		name         string
		node         testCheckTypeNode
		expectedVals int
		data         []argsResp
	}{
		{
			name: "ok_multi_same_dup",
			node: testCheckTypeNode{
				field: "log",
				values: [][]byte{
					[]byte("obj"), []byte("obj"), []byte("obj"), []byte("obj"), []byte("obj"), []byte("obj"),
				},
			},
			expectedVals: 1,
			data: []argsResp{
				{checkType: checkTypeObj, want: true},
				{checkType: checkTypeArr, want: false},
				{checkType: checkTypeNumber, want: false},
				{checkType: checkTypeString, want: false},
				{checkType: checkTypeNull, want: false},
				{checkType: checkTypeNil, want: false},
			},
		},
		{
			name: "ok_dup_obj_alias",
			node: testCheckTypeNode{
				field: "log",
				values: [][]byte{
					[]byte("object"), []byte("obj"),
				},
			},
			expectedVals: 1,
			data: []argsResp{
				{checkType: checkTypeObj, want: true},
				{checkType: checkTypeArr, want: false},
				{checkType: checkTypeNumber, want: false},
				{checkType: checkTypeString, want: false},
				{checkType: checkTypeNull, want: false},
				{checkType: checkTypeNil, want: false},
			},
		},
		{
			name: "ok_dup_arr_alias",
			node: testCheckTypeNode{
				field: "log",
				values: [][]byte{
					[]byte("array"), []byte("arr"),
				},
			},
			expectedVals: 1,
			data: []argsResp{
				{checkType: checkTypeObj, want: false},
				{checkType: checkTypeArr, want: true},
				{checkType: checkTypeNumber, want: false},
				{checkType: checkTypeString, want: false},
				{checkType: checkTypeNull, want: false},
				{checkType: checkTypeNil, want: false},
			},
		},
		{
			name: "ok_dup_number_alias",
			node: testCheckTypeNode{
				field: "log",
				values: [][]byte{
					[]byte("num"), []byte("number"),
				},
			},
			expectedVals: 1,
			data: []argsResp{
				{checkType: checkTypeObj, want: false},
				{checkType: checkTypeArr, want: false},
				{checkType: checkTypeNumber, want: true},
				{checkType: checkTypeString, want: false},
				{checkType: checkTypeNull, want: false},
				{checkType: checkTypeNil, want: false},
			},
		},
		{
			name: "ok_dup_str_alias",
			node: testCheckTypeNode{
				field: "log",
				values: [][]byte{
					[]byte("str"), []byte("string"),
				},
			},
			expectedVals: 1,
			data: []argsResp{
				{checkType: checkTypeObj, want: false},
				{checkType: checkTypeArr, want: false},
				{checkType: checkTypeNumber, want: false},
				{checkType: checkTypeString, want: true},
				{checkType: checkTypeNull, want: false},
				{checkType: checkTypeNil, want: false},
			},
		},
		{
			name: "ok_multi_dup_with_alias",
			node: testCheckTypeNode{
				field: "log",
				values: [][]byte{
					[]byte("null"), []byte("nil"), []byte("null"), []byte("nil"), []byte("obj"), []byte("object"),
				},
			},
			expectedVals: 3,
			data: []argsResp{
				{checkType: checkTypeObj, want: true},
				{checkType: checkTypeArr, want: false},
				{checkType: checkTypeNumber, want: false},
				{checkType: checkTypeString, want: false},
				{checkType: checkTypeNull, want: true},
				{checkType: checkTypeNil, want: true},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			node, err := NewCheckTypeOpNode(tt.node.field, tt.node.values)
			require.NoError(t, err, "must be no error on NewCheckTypeOpNode")
			ctnode, ok := node.(*checkTypeOpNode)
			require.True(t, ok, "must be *checkTypeOpNode type")
			assert.Equal(t, tt.expectedVals, len(ctnode.checkTypeFns))
			for i, d := range tt.data {
				eventStr := logsMap[d.checkType]
				eventRoot, err := insaneJSON.DecodeString(eventStr)
				require.NoError(t, err, "must be no error on decode checkEvent")
				got := ctnode.Check(eventRoot)
				assert.Equal(t, d.want, got, "invalid result for check %d of type %q", i, d.checkType)
			}
		})
	}
}
