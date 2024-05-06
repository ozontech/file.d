package pipeline

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	insaneJSON "github.com/vitkovskii/insane-json"
)

type treeNode struct {
	fieldOp       string
	fieldName     string
	caseSensitive bool
	values        [][]byte

	logicalOp string
	operands  []treeNode

	byteLenCmpOp string
	cmpValue     int
}

// nolint:gocritic
func buildTree(node treeNode) (DoIfNode, error) {
	switch {
	case node.fieldOp != "":
		return NewFieldOpNode(
			node.fieldOp,
			node.fieldName,
			node.caseSensitive,
			node.values,
		)
	case node.logicalOp != "":
		operands := make([]DoIfNode, 0)
		for _, operandNode := range node.operands {
			operand, err := buildTree(operandNode)
			if err != nil {
				return nil, fmt.Errorf("failed to build tree: %w", err)
			}
			operands = append(operands, operand)
		}
		return NewLogicalNode(
			node.logicalOp,
			operands,
		)
	case node.byteLenCmpOp != "":
		return NewByteLengthCmpNode(node.fieldName, node.byteLenCmpOp, node.cmpValue)
	default:
		return nil, errors.New("unknown type of node")
	}
}

func checkDoIfNode(t *testing.T, want, got DoIfNode) {
	require.Equal(t, want.Type(), got.Type())
	switch want.Type() {
	case DoIfNodeFieldOp:
		wantNode := want.(*doIfFieldOpNode)
		gotNode := got.(*doIfFieldOpNode)
		assert.Equal(t, wantNode.op, gotNode.op)
		assert.Equal(t, 0, slices.Compare[[]string](wantNode.fieldPath, gotNode.fieldPath))
		assert.Equal(t, wantNode.fieldPathStr, gotNode.fieldPathStr)
		assert.Equal(t, wantNode.caseSensitive, gotNode.caseSensitive)
		if wantNode.values == nil {
			assert.Equal(t, wantNode.values, gotNode.values)
		} else {
			require.Equal(t, len(wantNode.values), len(gotNode.values))
			for i := 0; i < len(wantNode.values); i++ {
				wantValues := wantNode.values[i]
				gotValues := gotNode.values[i]
				assert.Equal(t, 0, slices.Compare[[]byte](wantValues, gotValues))
			}
		}
		if wantNode.valuesBySize == nil {
			assert.Equal(t, wantNode.valuesBySize, gotNode.valuesBySize)
		} else {
			require.Equal(t, len(wantNode.valuesBySize), len(gotNode.valuesBySize))
			for k, wantVals := range wantNode.valuesBySize {
				gotVals, ok := gotNode.valuesBySize[k]
				assert.True(t, ok, "values by key %d not present in got node", k)
				if ok {
					require.Equal(t, len(wantVals), len(gotVals))
					for i := 0; i < len(wantVals); i++ {
						assert.Equal(t, 0, slices.Compare[[]byte](wantVals[i], gotVals[i]))
					}
				}
			}
		}
		assert.Equal(t, wantNode.minValLen, gotNode.minValLen)
		assert.Equal(t, wantNode.maxValLen, gotNode.maxValLen)
	case DoIfNodeLogicalOp:
		wantNode := want.(*doIfLogicalNode)
		gotNode := got.(*doIfLogicalNode)
		assert.Equal(t, wantNode.op, gotNode.op)
		require.Equal(t, len(wantNode.operands), len(gotNode.operands))
		for i := 0; i < len(wantNode.operands); i++ {
			checkDoIfNode(t, wantNode.operands[i], gotNode.operands[i])
		}
	case DoIfNodeByteLenCmpOp:
		wantNode := want.(*doIfByteLengthCmpNode)
		gotNode := got.(*doIfByteLengthCmpNode)
		assert.Equal(t, wantNode.cmpOp, gotNode.cmpOp)
		assert.Equal(t, wantNode.cmpValue, gotNode.cmpValue)
		assert.Equal(t, 0, slices.Compare[[]string](wantNode.fieldPath, gotNode.fieldPath))
	default:
		t.Error("unknown node type")
	}
}

func TestBuildDoIfNodes(t *testing.T) {
	tests := []struct {
		name    string
		tree    treeNode
		want    DoIfNode
		wantErr bool
	}{
		{
			name: "ok_field_op_node",
			tree: treeNode{
				fieldOp:       "equal",
				fieldName:     "log.pod",
				caseSensitive: true,
				values:        [][]byte{[]byte(`test-111`), []byte(`test-2`), []byte(`test-3`), []byte(`test-12345`)},
			},
			want: &doIfFieldOpNode{
				op:            doIfFieldEqualOp,
				fieldPath:     []string{"log", "pod"},
				fieldPathStr:  "log.pod",
				caseSensitive: true,
				values:        nil,
				valuesBySize: map[int][][]byte{
					6: [][]byte{
						[]byte(`test-2`),
						[]byte(`test-3`),
					},
					8: [][]byte{
						[]byte(`test-111`),
					},
					10: [][]byte{
						[]byte(`test-12345`),
					},
				},
				minValLen: 6,
				maxValLen: 10,
			},
		},
		{
			name: "ok_field_op_node_case_insensitive",
			tree: treeNode{
				fieldOp:       "equal",
				fieldName:     "log.pod",
				caseSensitive: false,
				values:        [][]byte{[]byte(`TEST-111`), []byte(`Test-2`), []byte(`tesT-3`), []byte(`TeSt-12345`)},
			},
			want: &doIfFieldOpNode{
				op:            doIfFieldEqualOp,
				fieldPath:     []string{"log", "pod"},
				fieldPathStr:  "log.pod",
				caseSensitive: false,
				values:        nil,
				valuesBySize: map[int][][]byte{
					6: [][]byte{
						[]byte(`test-2`),
						[]byte(`test-3`),
					},
					8: [][]byte{
						[]byte(`test-111`),
					},
					10: [][]byte{
						[]byte(`test-12345`),
					},
				},
				minValLen: 6,
				maxValLen: 10,
			},
		},
		{
			name: "ok_logical_op_node_or",
			tree: treeNode{
				logicalOp: "or",
				operands: []treeNode{
					{
						fieldOp:       "equal",
						fieldName:     "log.pod",
						caseSensitive: true,
						values:        [][]byte{[]byte(`test-111`), []byte(`test-2`), []byte(`test-3`), []byte(`test-12345`)},
					},
					{
						fieldOp:       "contains",
						fieldName:     "service.msg",
						caseSensitive: true,
						values:        [][]byte{[]byte(`test-0987`), []byte(`test-11`)},
					},
				},
			},
			want: &doIfLogicalNode{
				op: doIfLogicalOr,
				operands: []DoIfNode{
					&doIfFieldOpNode{
						op:            doIfFieldEqualOp,
						fieldPath:     []string{"log", "pod"},
						fieldPathStr:  "log.pod",
						caseSensitive: true,
						values:        nil,
						valuesBySize: map[int][][]byte{
							6: [][]byte{
								[]byte(`test-2`),
								[]byte(`test-3`),
							},
							8: [][]byte{
								[]byte(`test-111`),
							},
							10: [][]byte{
								[]byte(`test-12345`),
							},
						},
						minValLen: 6,
						maxValLen: 10,
					},
					&doIfFieldOpNode{
						op:            doIfFieldContainsOp,
						fieldPath:     []string{"service", "msg"},
						fieldPathStr:  "service.msg",
						caseSensitive: true,
						values: [][]byte{
							[]byte(`test-0987`),
							[]byte(`test-11`),
						},
						minValLen: 7,
						maxValLen: 9,
					},
				},
			},
		},
		{
			name: "ok_byte_len_cmp_op_node",
			tree: treeNode{
				byteLenCmpOp: "lt",
				fieldName:    "pod",
				cmpValue:     100,
			},
			want: &doIfByteLengthCmpNode{
				fieldPath: []string{"pod"},
				cmpOp:     "lt",
				cmpValue:  100,
			},
		},
		{
			name: "ok_byte_len_cmp_op_node_empty_selector",
			tree: treeNode{
				byteLenCmpOp: "lt",
				fieldName:    "",
				cmpValue:     100,
			},
			want: &doIfByteLengthCmpNode{
				fieldPath: []string{},
				cmpOp:     "lt",
				cmpValue:  100,
			},
		},
		{
			name: "err_field_op_node_empty_field",
			tree: treeNode{
				fieldOp: "equal",
			},
			wantErr: true,
		},
		{
			name: "err_field_op_node_empty_values",
			tree: treeNode{
				fieldOp:   "equal",
				fieldName: "pod",
			},
			wantErr: true,
		},
		{
			name: "err_field_op_node_invalid_regex",
			tree: treeNode{
				fieldOp:   "regex",
				fieldName: "pod",
				values:    [][]byte{[]byte(`\`)},
			},
			wantErr: true,
		},
		{
			name: "err_field_op_node_invalid_op_type",
			tree: treeNode{
				fieldOp:   "noop",
				fieldName: "pod",
				values:    [][]byte{[]byte(`test`)},
			},
			wantErr: true,
		},
		{
			name: "err_byte_len_op_node_invalid_op_type",
			tree: treeNode{
				byteLenCmpOp: "no-op",
				fieldName:    "pod",
				cmpValue:     100,
			},
			wantErr: true,
		},
		{
			name: "err_byte_len_op_node_negative_cmp_value",
			tree: treeNode{
				byteLenCmpOp: "lt",
				fieldName:    "pod",
				cmpValue:     -1,
			},
			wantErr: true,
		},
		{
			name: "err_logical_op_node_empty_operands",
			tree: treeNode{
				logicalOp: "or",
			},
			wantErr: true,
		},
		{
			name: "err_logical_op_node_invalid_op_type",
			tree: treeNode{
				logicalOp: "noop",
				operands: []treeNode{
					{
						fieldOp:       "contains",
						fieldName:     "service.msg",
						caseSensitive: true,
						values:        [][]byte{[]byte(`test-0987`), []byte(`test-11`)},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "err_logical_op_node_too_much_operands",
			tree: treeNode{
				logicalOp: "not",
				operands: []treeNode{
					{
						fieldOp:       "contains",
						fieldName:     "service.msg",
						caseSensitive: true,
						values:        [][]byte{[]byte(`test-0987`), []byte(`test-11`)},
					},
					{
						fieldOp:       "contains",
						fieldName:     "service.msg",
						caseSensitive: true,
						values:        [][]byte{[]byte(`test-0987`), []byte(`test-11`)},
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := buildTree(tt.tree)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			checkDoIfNode(t, tt.want, got)
		})
	}
}

func TestCheck(t *testing.T) {
	type argsResp struct {
		eventStr string
		want     bool
	}

	tests := []struct {
		name           string
		tree           treeNode
		data           []argsResp
		wantNewNodeErr bool
	}{
		{
			name: "ok_equal",
			tree: treeNode{
				fieldOp:       "equal",
				fieldName:     "pod",
				caseSensitive: true,
				values:        [][]byte{[]byte("test-1"), []byte("test-2"), []byte("test-pod-123"), []byte("po-32")},
			},
			data: []argsResp{
				{eventStr: `{"pod":"test-1"}`, want: true},
				{eventStr: `{"pod":"test-2"}`, want: true},
				{eventStr: `{"pod":"test-3"}`, want: false},
				{eventStr: `{"pod":"TEST-2"}`, want: false},
				{eventStr: `{"pod":"test-pod-123"}`, want: true},
				{eventStr: `{"pod":"po-32"}`, want: true},
				{eventStr: `{"pod":"p-32"}`, want: false},
				{eventStr: `{"service":"test-1"}`, want: false},
				{eventStr: `{"pod":"test-123456789"}`, want: false},
				{eventStr: ``, want: false},
			},
		},
		{
			name: "ok_contains",
			tree: treeNode{
				fieldOp:       "contains",
				fieldName:     "pod",
				caseSensitive: true,
				values:        [][]byte{[]byte("test-1"), []byte("test-2")},
			},
			data: []argsResp{
				{`{"pod":"my-test-1-pod"}`, true},
				{`{"pod":"my-test-2-pod"}`, true},
				{`{"pod":"my-test-3-pod"}`, false},
				{`{"pod":"my-TEST-2-pod"}`, false},
			},
		},
		{
			name: "ok_prefix",
			tree: treeNode{
				fieldOp:       "prefix",
				fieldName:     "pod",
				caseSensitive: true,
				values:        [][]byte{[]byte("test-1"), []byte("test-2")},
			},
			data: []argsResp{
				{`{"pod":"test-1-pod"}`, true},
				{`{"pod":"test-2-pod"}`, true},
				{`{"pod":"test-3-pod"}`, false},
				{`{"pod":"TEST-2-pod"}`, false},
			},
		},
		{
			name: "ok_suffix",
			tree: treeNode{
				fieldOp:       "suffix",
				fieldName:     "pod",
				caseSensitive: true,
				values:        [][]byte{[]byte("test-1"), []byte("test-2")},
			},
			data: []argsResp{
				{`{"pod":"my-test-1"}`, true},
				{`{"pod":"my-test-2"}`, true},
				{`{"pod":"my-test-3"}`, false},
				{`{"pod":"my-TEST-2"}`, false},
			},
		},
		{
			name: "ok_regex",
			tree: treeNode{
				fieldOp:   "regex",
				fieldName: "pod",
				values:    [][]byte{[]byte(`test-\d`)},
			},
			data: []argsResp{
				{`{"pod":"my-test-1-pod"}`, true},
				{`{"pod":"my-test-2-pod"}`, true},
				{`{"pod":"my-test-3-pod"}`, true},
				{`{"pod":"my-test-pod"}`, false},
				{`{"pod":"my-pod-3-pod"}`, false},
				{`{"pod":"my-TEST-4-pod"}`, false},
			},
		},
		{
			name: "ok_or",
			tree: treeNode{
				logicalOp: "or",
				operands: []treeNode{
					{
						fieldOp:       "equal",
						fieldName:     "pod",
						caseSensitive: true,
						values:        [][]byte{[]byte("test-1"), []byte("test-2")},
					},
					{
						fieldOp:       "equal",
						fieldName:     "pod",
						caseSensitive: true,
						values:        [][]byte{[]byte("test-3"), []byte("test-4")},
					},
				},
			},
			data: []argsResp{
				{`{"pod":"test-1"}`, true},
				{`{"pod":"test-2"}`, true},
				{`{"pod":"test-3"}`, true},
				{`{"pod":"test-4"}`, true},
				{`{"pod":"test-5"}`, false},
				{`{"pod":"TEST-1"}`, false},
				{`{"pod":"TEST-3"}`, false},
			},
		},
		{
			name: "ok_and",
			tree: treeNode{
				logicalOp: "and",
				operands: []treeNode{
					{
						fieldOp:       "prefix",
						fieldName:     "pod",
						caseSensitive: true,
						values:        [][]byte{[]byte("test")},
					},
					{
						fieldOp:       "suffix",
						fieldName:     "pod",
						caseSensitive: true,
						values:        [][]byte{[]byte("pod")},
					},
				},
			},
			data: []argsResp{
				{`{"pod":"test-1-pod"}`, true},
				{`{"pod":"test-2-pod"}`, true},
				{`{"pod":"test-3"}`, false},
				{`{"pod":"service-test-4-pod"}`, false},
				{`{"pod":"service-test-5"}`, false},
				{`{"pod":"TEST-6-pod"}`, false},
				{`{"pod":"test-7-POD"}`, false},
			},
		},
		{
			name: "ok_not",
			tree: treeNode{
				logicalOp: "not",
				operands: []treeNode{
					{
						fieldOp:       "equal",
						fieldName:     "pod",
						caseSensitive: true,
						values:        [][]byte{[]byte("test-1"), []byte("test-2")},
					},
				},
			},
			data: []argsResp{
				{`{"pod":"test-1"}`, false},
				{`{"pod":"test-2"}`, false},
				{`{"pod":"TEST-2"}`, true},
				{`{"pod":"test-3"}`, true},
				{`{"pod":"test-4"}`, true},
			},
		},
		{
			name: "ok_equal_case_insensitive",
			tree: treeNode{
				fieldOp:       "equal",
				fieldName:     "pod",
				caseSensitive: false,
				values:        [][]byte{[]byte("Test-1"), []byte("tesT-2")},
			},
			data: []argsResp{
				{eventStr: `{"pod":"tEST-1"}`, want: true},
				{eventStr: `{"pod":"test-2"}`, want: true},
				{eventStr: `{"pod":"test-3"}`, want: false},
				{eventStr: `{"pod":"TEST-2"}`, want: true},
			},
		},
		{
			name: "ok_contains_case_insensitive",
			tree: treeNode{
				fieldOp:       "contains",
				fieldName:     "pod",
				caseSensitive: false,
				values:        [][]byte{[]byte("Test-1"), []byte("tesT-2")},
			},
			data: []argsResp{
				{`{"pod":"my-tEST-1-pod"}`, true},
				{`{"pod":"my-test-2-pod"}`, true},
				{`{"pod":"my-test-3-pod"}`, false},
				{`{"pod":"my-TEST-2-pod"}`, true},
			},
		},
		{
			name: "ok_prefix_case_insensitive",
			tree: treeNode{
				fieldOp:       "prefix",
				fieldName:     "pod",
				caseSensitive: false,
				values:        [][]byte{[]byte("Test-1"), []byte("tesT-2")},
			},
			data: []argsResp{
				{`{"pod":"tEST-1-pod"}`, true},
				{`{"pod":"test-2-pod"}`, true},
				{`{"pod":"test-3-pod"}`, false},
				{`{"pod":"TEST-2-pod"}`, true},
			},
		},
		{
			name: "ok_suffix_case_insensitive",
			tree: treeNode{
				fieldOp:       "suffix",
				fieldName:     "pod",
				caseSensitive: false,
				values:        [][]byte{[]byte("Test-1"), []byte("tesT-2")},
			},
			data: []argsResp{
				{`{"pod":"my-teST-1"}`, true},
				{`{"pod":"my-test-2"}`, true},
				{`{"pod":"my-test-3"}`, false},
				{`{"pod":"my-TEST-2"}`, true},
			},
		},
		{
			name: "ok_equal_nil_or_empty_string",
			tree: treeNode{
				fieldOp:       "equal",
				fieldName:     "test-field",
				caseSensitive: false,
				values:        [][]byte{nil, []byte("")},
			},
			data: []argsResp{
				{`{"pod":"my-teST-1"}`, true},
				{`{"pod":"my-test-2","test-field":null}`, true},
				{`{"pod":"my-test-3","test-field":""}`, true},
				{`{"pod":"my-TEST-2","test-field":"non-empty"}`, false},
			},
		},
		{
			name: "ok_byte_len_cmp_lt",
			tree: treeNode{
				byteLenCmpOp: "lt",
				fieldName:    "msg",
				cmpValue:     4,
			},
			data: []argsResp{
				{`{"msg":""}`, true},
				{`{"msg":1}`, true},
				{`{"msg":12}`, true},
				{`{"msg":123}`, true},
				{`{"msg":1234}`, false},
				{`{"msg":12345}`, false},
				{`{"msg":123456}`, false},
			},
		},
		{
			name: "ok_byte_len_cmp_ge",
			tree: treeNode{
				byteLenCmpOp: "ge",
				fieldName:    "msg",
				cmpValue:     4,
			},
			data: []argsResp{
				{`{"msg":""}`, false},
				{`{"msg":1}`, false},
				{`{"msg":12}`, false},
				{`{"msg":123}`, false},
				{`{"msg":1234}`, true},
				{`{"msg":12345}`, true},
				{`{"msg":123456}`, true},
			},
		},
		{
			name: "ok_byte_len_cmp_lt_empty_selector",
			tree: treeNode{
				byteLenCmpOp: "lt",
				fieldName:    "",
				cmpValue:     4,
			},
			data: []argsResp{
				{`""`, true},
				{`1`, true},
				{`12`, true},
				{`123`, true},
				{`1234`, false},
				{`12345`, false},
				{`123456`, false},
			},
		},
		{
			name: "ok_byte_len_cmp_eq",
			tree: treeNode{
				byteLenCmpOp: "eq",
				fieldName:    "msg",
				cmpValue:     2,
			},
			data: []argsResp{
				{`{"msg":1}`, false},
				{`{"msg":12}`, true},
				{`{"msg":123}`, false},
			},
		},
		{
			name: "ok_byte_len_cmp_ne",
			tree: treeNode{
				byteLenCmpOp: "ne",
				fieldName:    "msg",
				cmpValue:     2,
			},
			data: []argsResp{
				{`{"msg":1}`, true},
				{`{"msg":12}`, false},
				{`{"msg":123}`, true},
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			var root DoIfNode
			var eventRoot *insaneJSON.Root
			var err error
			t.Parallel()
			root, err = buildTree(tt.tree)
			if tt.wantNewNodeErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			checker := NewDoIfChecker(root)
			for _, d := range tt.data {
				if d.eventStr == "" {
					eventRoot = nil
				} else {
					eventRoot, err = insaneJSON.DecodeString(d.eventStr)
					require.NoError(t, err)
				}
				got := checker.Check(eventRoot)
				assert.Equal(t, d.want, got, "invalid result for event %q", d.eventStr)
			}
		})
	}
}

const userInfoRawJSON = `
{
	"name": "jack",
	"age": 120,
	"hobbies": ["football", "diving"]
}`

func dryJSON(rawJSON string) string {
	s := rawJSON

	s = strings.ReplaceAll(s, " ", "")
	s = strings.ReplaceAll(s, "\n", "")
	s = strings.ReplaceAll(s, "\t", "")

	return s
}

var userInfoDryJSON = dryJSON(userInfoRawJSON)

func TestCheckLenCmpLtObject(t *testing.T) {
	type TestCase struct {
		cmpValue int
		result   bool
	}

	tests := []TestCase{
		{
			cmpValue: len(userInfoDryJSON) - 1,
			result:   false,
		},
		{
			cmpValue: len(userInfoDryJSON),
			result:   false,
		},
		{
			cmpValue: len(userInfoDryJSON) + 1,
			result:   true,
		},
		{
			cmpValue: len(userInfoDryJSON) + 2,
			result:   true,
		},
	}

	rowJSON := fmt.Sprintf(`{"user_info": %s}`, userInfoRawJSON)
	eventRoot, err := insaneJSON.DecodeString(rowJSON)
	require.NoError(t, err)

	for index, test := range tests {
		root, err := buildTree(treeNode{
			fieldName:    "user_info",
			byteLenCmpOp: "lt",
			cmpValue:     test.cmpValue,
		})
		require.NoError(t, err)

		checker := NewDoIfChecker(root)
		result := checker.Check(eventRoot)
		require.Equal(t, test.result, result, "invalid result; test id: %d", index)
	}
}

func TestDoIfNodeIsEqual(t *testing.T) {
	singleNode1 := treeNode{
		fieldOp:       "equal",
		fieldName:     "service",
		caseSensitive: true,
		values:        [][]byte{[]byte("test-1"), []byte("test-2")},
	}
	singleNode2 := treeNode{
		byteLenCmpOp: "lt",
		fieldName:    "msg",
		cmpValue:     100,
	}
	twoNodes := treeNode{
		logicalOp: "not",
		operands: []treeNode{
			{
				fieldOp:       "equal",
				fieldName:     "service",
				caseSensitive: true,
				values:        [][]byte{[]byte("test-1"), []byte("test-2")},
			},
		},
	}
	multiNodes := treeNode{
		logicalOp: "not",
		operands: []treeNode{
			{
				logicalOp: "or",
				operands: []treeNode{
					{
						fieldOp:       "equal",
						fieldName:     "service",
						caseSensitive: true,
						values:        [][]byte{nil, []byte(""), []byte("null")},
					},
					{
						fieldOp:       "contains",
						fieldName:     "pod",
						caseSensitive: false,
						values:        [][]byte{[]byte("pod-1"), []byte("pod-2")},
					},
					{
						byteLenCmpOp: "lt",
						fieldName:    "msg",
						cmpValue:     100,
					},
					{
						logicalOp: "and",
						operands: []treeNode{
							{
								fieldOp:       "prefix",
								fieldName:     "message",
								caseSensitive: true,
								values:        [][]byte{[]byte("test-msg-1"), []byte("test-msg-2")},
							},
							{
								fieldOp:       "suffix",
								fieldName:     "message",
								caseSensitive: true,
								values:        [][]byte{[]byte("test-msg-3"), []byte("test-msg-4")},
							},
							{
								fieldOp:       "regex",
								fieldName:     "msg",
								caseSensitive: true,
								values:        [][]byte{[]byte("test-\\d+"), []byte("test-000-\\d+")},
							},
						},
					},
				},
			},
		},
	}
	tests := []struct {
		name    string
		t1      treeNode
		t2      treeNode
		wantErr bool
	}{
		{
			name:    "equal_single_node",
			t1:      singleNode1,
			t2:      singleNode1,
			wantErr: false,
		},
		{
			name:    "equal_byte_len_cmp_node",
			t1:      singleNode2,
			t2:      singleNode2,
			wantErr: false,
		},
		{
			name:    "equal_two_nodes",
			t1:      twoNodes,
			t2:      twoNodes,
			wantErr: false,
		},
		{
			name:    "equal_multiple_nodes",
			t1:      multiNodes,
			t2:      multiNodes,
			wantErr: false,
		},
		{
			name:    "not_equal_type_mismatch_1",
			t1:      singleNode1,
			t2:      singleNode2,
			wantErr: true,
		},
		{
			name:    "not_equal_type_mismatch_2",
			t1:      singleNode1,
			t2:      multiNodes,
			wantErr: true,
		},
		{
			name:    "not_equal_type_mismatch_3",
			t1:      singleNode2,
			t2:      multiNodes,
			wantErr: true,
		},
		{
			name: "not_equal_field_op_mismatch",
			t1: treeNode{
				fieldOp:       "equal",
				fieldName:     "service",
				caseSensitive: false,
				values:        [][]byte{[]byte("test-1")},
			},
			t2: treeNode{
				fieldOp:       "contains",
				fieldName:     "service",
				caseSensitive: false,
				values:        [][]byte{[]byte("test-1")},
			},
			wantErr: true,
		},
		{
			name: "not_equal_field_op_mismatch_2",
			t1: treeNode{
				fieldOp:       "prefix",
				fieldName:     "service",
				caseSensitive: false,
				values:        [][]byte{[]byte("test-1")},
			},
			t2: treeNode{
				fieldOp:       "suffix",
				fieldName:     "service",
				caseSensitive: false,
				values:        [][]byte{[]byte("test-1")},
			},
			wantErr: true,
		},
		{
			name: "not_equal_field_op_mismatch_3",
			t1: treeNode{
				fieldOp:       "regex",
				fieldName:     "service",
				caseSensitive: false,
				values:        [][]byte{[]byte("test-1")},
			},
			t2: treeNode{
				fieldOp:       "contains",
				fieldName:     "service",
				caseSensitive: false,
				values:        [][]byte{[]byte("test-1")},
			},
			wantErr: true,
		},
		{
			name: "not_equal_field_case_sensitive_mismatch",
			t1: treeNode{
				fieldOp:       "equal",
				fieldName:     "service",
				caseSensitive: false,
				values:        [][]byte{[]byte("test-1")},
			},
			t2: treeNode{
				fieldOp:       "equal",
				fieldName:     "service",
				caseSensitive: true,
				values:        [][]byte{[]byte("test-1")},
			},
			wantErr: true,
		},
		{
			name: "not_equal_field_field_path_mismatch",
			t1: treeNode{
				fieldOp:       "equal",
				fieldName:     "log.msg",
				caseSensitive: true,
				values:        [][]byte{[]byte("test-1")},
			},
			t2: treeNode{
				fieldOp:       "equal",
				fieldName:     "log.svc",
				caseSensitive: true,
				values:        [][]byte{[]byte("test-1")},
			},
			wantErr: true,
		},
		{
			name: "not_equal_field_values_slice_len_mismatch",
			t1: treeNode{
				fieldOp:       "contains",
				fieldName:     "service",
				caseSensitive: true,
				values:        [][]byte{[]byte("test-1"), []byte("test-2")},
			},
			t2: treeNode{
				fieldOp:       "contains",
				fieldName:     "service",
				caseSensitive: true,
				values:        [][]byte{[]byte("test-1")},
			},
			wantErr: true,
		},
		{
			name: "not_equal_field_values_slice_vals_mismatch",
			t1: treeNode{
				fieldOp:       "contains",
				fieldName:     "service",
				caseSensitive: true,
				values:        [][]byte{[]byte("test-2")},
			},
			t2: treeNode{
				fieldOp:       "contains",
				fieldName:     "service",
				caseSensitive: true,
				values:        [][]byte{[]byte("test-1")},
			},
			wantErr: true,
		},
		{
			name: "not_equal_field_values_by_size_len_mismatch",
			t1: treeNode{
				fieldOp:       "equal",
				fieldName:     "service",
				caseSensitive: true,
				values:        [][]byte{[]byte("test-1"), []byte("test-22")},
			},
			t2: treeNode{
				fieldOp:       "equal",
				fieldName:     "service",
				caseSensitive: true,
				values:        [][]byte{[]byte("test-1")},
			},
			wantErr: true,
		},
		{
			name: "not_equal_field_values_by_size_vals_key_mismatch",
			t1: treeNode{
				fieldOp:       "equal",
				fieldName:     "service",
				caseSensitive: true,
				values:        [][]byte{[]byte("test-11")},
			},
			t2: treeNode{
				fieldOp:       "equal",
				fieldName:     "service",
				caseSensitive: true,
				values:        [][]byte{[]byte("test-1")},
			},
			wantErr: true,
		},
		{
			name: "not_equal_field_values_by_size_vals_len_mismatch",
			t1: treeNode{
				fieldOp:       "equal",
				fieldName:     "service",
				caseSensitive: true,
				values:        [][]byte{[]byte("test-1"), []byte("test-2")},
			},
			t2: treeNode{
				fieldOp:       "equal",
				fieldName:     "service",
				caseSensitive: true,
				values:        [][]byte{[]byte("test-1")},
			},
			wantErr: true,
		},
		{
			name: "not_equal_field_values_by_size_vals_mismatch",
			t1: treeNode{
				fieldOp:       "equal",
				fieldName:     "service",
				caseSensitive: true,
				values:        [][]byte{[]byte("test-2")},
			},
			t2: treeNode{
				fieldOp:       "equal",
				fieldName:     "service",
				caseSensitive: true,
				values:        [][]byte{[]byte("test-1")},
			},
			wantErr: true,
		},
		{
			name: "not_equal_field_reValues_len_mismatch",
			t1: treeNode{
				fieldOp:       "regex",
				fieldName:     "service",
				caseSensitive: true,
				values:        [][]byte{[]byte("test-1"), []byte("test-2")},
			},
			t2: treeNode{
				fieldOp:       "regex",
				fieldName:     "service",
				caseSensitive: true,
				values:        [][]byte{[]byte("test-1")},
			},
			wantErr: true,
		},
		{
			name: "not_equal_field_reValues_vals_mismatch",
			t1: treeNode{
				fieldOp:       "regex",
				fieldName:     "service",
				caseSensitive: true,
				values:        [][]byte{[]byte("test-2")},
			},
			t2: treeNode{
				fieldOp:       "regex",
				fieldName:     "service",
				caseSensitive: true,
				values:        [][]byte{[]byte("test-1")},
			},
			wantErr: true,
		},
		{
			name: "not_equal_byte_len_cmp_op_mismatch",
			t1: treeNode{
				byteLenCmpOp: "lt",
				fieldName:    "msg",
				cmpValue:     100,
			},
			t2: treeNode{
				byteLenCmpOp: "gt",
				fieldName:    "msg",
				cmpValue:     100,
			},
			wantErr: true,
		},
		{
			name: "not_equal_byte_len_cmp_op_field_mismatch",
			t1: treeNode{
				byteLenCmpOp: "lt",
				fieldName:    "msg",
				cmpValue:     100,
			},
			t2: treeNode{
				byteLenCmpOp: "lt",
				fieldName:    "pod",
				cmpValue:     100,
			},
			wantErr: true,
		},
		{
			name: "not_equal_byte_len_cmp_op_value_mismatch",
			t1: treeNode{
				byteLenCmpOp: "lt",
				fieldName:    "msg",
				cmpValue:     100,
			},
			t2: treeNode{
				byteLenCmpOp: "lt",
				fieldName:    "msg",
				cmpValue:     200,
			},
			wantErr: true,
		},
		{
			name: "not_equal_logical_op_mismatch",
			t1: treeNode{
				logicalOp: "not",
				operands: []treeNode{
					{
						fieldOp:       "equal",
						fieldName:     "service",
						caseSensitive: false,
						values:        [][]byte{nil},
					},
				},
			},
			t2: treeNode{
				logicalOp: "and",
				operands: []treeNode{
					{
						fieldOp:       "equal",
						fieldName:     "service",
						caseSensitive: false,
						values:        [][]byte{nil},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "not_equal_logical_operands_len_mismatch",
			t1: treeNode{
				logicalOp: "or",
				operands: []treeNode{
					{
						fieldOp:       "equal",
						fieldName:     "service",
						caseSensitive: false,
						values:        [][]byte{nil},
					},
				},
			},
			t2: treeNode{
				logicalOp: "or",
				operands: []treeNode{
					{
						fieldOp:       "equal",
						fieldName:     "service",
						caseSensitive: false,
						values:        [][]byte{nil},
					},
					{
						fieldOp:       "equal",
						fieldName:     "service",
						caseSensitive: false,
						values:        [][]byte{nil},
					},
				},
			},
			wantErr: true,
		},
		{
			name: "not_equal_logical_operands_mismatch_field_name",
			t1: treeNode{
				logicalOp: "or",
				operands: []treeNode{
					{
						fieldOp:       "equal",
						fieldName:     "service",
						caseSensitive: false,
						values:        [][]byte{nil},
					},
				},
			},
			t2: treeNode{
				logicalOp: "or",
				operands: []treeNode{
					{
						fieldOp:       "equal",
						fieldName:     "pod",
						caseSensitive: false,
						values:        [][]byte{nil},
					},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			root1, err := buildTree(tt.t1)
			require.NoError(t, err)
			root2, err := buildTree(tt.t2)
			require.NoError(t, err)
			c1 := NewDoIfChecker(root1)
			c2 := NewDoIfChecker(root2)
			err1 := c1.IsEqualTo(c2)
			err2 := c2.IsEqualTo(c1)
			if tt.wantErr {
				assert.Error(t, err1, "tree1 expected to be not equal to tree2")
				assert.Error(t, err2, "tree2 expected to be not equal to tree1")
			} else {
				assert.NoError(t, err1, "tree1 expected to be equal to tree2")
				assert.NoError(t, err2, "tree2 expected to be equal to tree1")
			}
		})
	}
}
