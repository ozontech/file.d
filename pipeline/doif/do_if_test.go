package doif

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

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

	lenCmpOp string
	cmpOp    string
	cmpValue int

	tsCmpOp    bool
	tsFormat   string
	tsCmpMode  string
	tsCmpValue time.Time
}

// nolint:gocritic
func buildTree(node treeNode) (Node, error) {
	switch {
	case node.fieldOp != "":
		return NewFieldOpNode(
			node.fieldOp,
			node.fieldName,
			node.caseSensitive,
			node.values,
		)
	case node.logicalOp != "":
		operands := make([]Node, 0)
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
	case node.lenCmpOp != "":
		return NewLenCmpOpNode(node.lenCmpOp, node.fieldName, node.cmpOp, node.cmpValue)
	case node.tsCmpOp:
		return NewTsCmpOpNode(node.fieldName, node.tsFormat, node.cmpOp, node.tsCmpMode, node.tsCmpValue)
	default:
		return nil, errors.New("unknown type of node")
	}
}

func checkNode(t *testing.T, want, got Node) {
	require.Equal(t, want.Type(), got.Type())
	switch want.Type() {
	case NodeFieldOp:
		wantNode := want.(*fieldOpNode)
		gotNode := got.(*fieldOpNode)
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
	case NodeLogicalOp:
		wantNode := want.(*logicalNode)
		gotNode := got.(*logicalNode)
		assert.Equal(t, wantNode.op, gotNode.op)
		require.Equal(t, len(wantNode.operands), len(gotNode.operands))
		for i := 0; i < len(wantNode.operands); i++ {
			checkNode(t, wantNode.operands[i], gotNode.operands[i])
		}
	case NodeLengthCmpOp:
		wantNode := want.(*lenCmpOpNode)
		gotNode := got.(*lenCmpOpNode)
		assert.Equal(t, wantNode.lenCmpOp, gotNode.lenCmpOp)
		assert.NoError(t, wantNode.comparator.isEqualTo(gotNode.comparator))
		assert.Equal(t, 0, slices.Compare[[]string](wantNode.fieldPath, gotNode.fieldPath))
	case NodeTimestampCmpOp:
		wantNode := want.(*tsCmpOpNode)
		gotNode := got.(*tsCmpOpNode)
		assert.Equal(t, wantNode.format, gotNode.format)
		assert.Equal(t, wantNode.cmpOp, gotNode.cmpOp)
		assert.Equal(t, wantNode.cmpMode, gotNode.cmpMode)
		assert.Equal(t, wantNode.cmpValue, gotNode.cmpValue)
		assert.Equal(t, 0, slices.Compare[[]string](wantNode.fieldPath, gotNode.fieldPath))
	default:
		t.Error("unknown node type")
	}
}

func TestBuildNodes(t *testing.T) {
	timestamp := time.Now()

	tests := []struct {
		name    string
		tree    treeNode
		want    Node
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
			want: &fieldOpNode{
				op:            fieldEqualOp,
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
			want: &fieldOpNode{
				op:            fieldEqualOp,
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
			want: &logicalNode{
				op: logicalOr,
				operands: []Node{
					&fieldOpNode{
						op:            fieldEqualOp,
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
					&fieldOpNode{
						op:            fieldContainsOp,
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
				lenCmpOp:  byteLenCmpOpTag,
				cmpOp:     "lt",
				fieldName: "pod",
				cmpValue:  100,
			},
			want: &lenCmpOpNode{
				lenCmpOp:  byteLenCmpOp,
				fieldPath: []string{"pod"},
				comparator: comparator{
					cmpOp:    cmpOpLess,
					cmpValue: 100,
				},
			},
		},
		{
			name: "ok_byte_len_cmp_op_node_empty_selector",
			tree: treeNode{
				lenCmpOp:  byteLenCmpOpTag,
				cmpOp:     "lt",
				fieldName: "",
				cmpValue:  100,
			},
			want: &lenCmpOpNode{
				lenCmpOp:  byteLenCmpOp,
				fieldPath: []string{},
				comparator: comparator{
					cmpOp:    cmpOpLess,
					cmpValue: 100,
				},
			},
		},
		{
			name: "ok_array_len_cmp_op_node",
			tree: treeNode{
				lenCmpOp:  arrayLenCmpOpTag,
				cmpOp:     "lt",
				fieldName: "items",
				cmpValue:  100,
			},
			want: &lenCmpOpNode{
				lenCmpOp:  arrayLenCmpOp,
				fieldPath: []string{"items"},
				comparator: comparator{
					cmpOp:    cmpOpLess,
					cmpValue: 100,
				},
			},
		},
		{
			name: "ok_array_len_cmp_op_node_empty_selector",
			tree: treeNode{
				lenCmpOp:  arrayLenCmpOpTag,
				cmpOp:     "lt",
				fieldName: "",
				cmpValue:  100,
			},
			want: &lenCmpOpNode{
				lenCmpOp:  arrayLenCmpOp,
				fieldPath: []string{},
				comparator: comparator{
					cmpOp:    cmpOpLess,
					cmpValue: 100,
				},
			},
		},
		{
			name: "ok_ts_cmp_op_node_explicit",
			tree: treeNode{
				tsCmpOp:    true,
				cmpOp:      "lt",
				fieldName:  "items",
				tsCmpMode:  "explicit",
				tsFormat:   time.RFC3339,
				tsCmpValue: timestamp,
			},
			want: &tsCmpOpNode{
				fieldPath: []string{"items"},
				format:    time.RFC3339,
				cmpOp:     cmpOpLess,
				cmpMode:   cmpModeExplicit,
				cmpValue:  timestamp,
			},
		},
		{
			name: "ok_ts_cmp_op_node_now",
			tree: treeNode{
				tsCmpOp:    true,
				cmpOp:      "lt",
				fieldName:  "items",
				tsCmpMode:  "now",
				tsFormat:   time.RFC3339,
				tsCmpValue: timestamp, // ignored because of cmp mode
			},
			want: &tsCmpOpNode{
				fieldPath: []string{"items"},
				format:    time.RFC3339,
				cmpOp:     cmpOpLess,
				cmpMode:   cmpModeNow,
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
				lenCmpOp:  byteLenCmpOpTag,
				cmpOp:     "no-op",
				fieldName: "pod",
				cmpValue:  100,
			},
			wantErr: true,
		},
		{
			name: "err_byte_len_op_node_negative_cmp_value",
			tree: treeNode{
				lenCmpOp:  byteLenCmpOpTag,
				cmpOp:     "lt",
				fieldName: "pod",
				cmpValue:  -1,
			},
			wantErr: true,
		},
		{
			name: "err_array_len_op_node_invalid_op_type",
			tree: treeNode{
				lenCmpOp:  arrayLenCmpOpTag,
				cmpOp:     "no-op",
				fieldName: "items",
				cmpValue:  100,
			},
			wantErr: true,
		},
		{
			name: "err_array_len_op_node_negative_cmp_value",
			tree: treeNode{
				lenCmpOp:  arrayLenCmpOpTag,
				cmpOp:     "lt",
				fieldName: "items",
				cmpValue:  -1,
			},
			wantErr: true,
		},
		{
			name: "err_ts_cmp_op_node_invalid_op_type",
			tree: treeNode{
				tsCmpOp:    true,
				cmpOp:      "no-op",
				fieldName:  "items",
				tsCmpMode:  "explicit",
				tsFormat:   time.RFC3339,
				tsCmpValue: timestamp,
			},
			wantErr: true,
		},
		{
			name: "err_ts_cmp_op_node_invalid_cmp_mode",
			tree: treeNode{
				tsCmpOp:    true,
				cmpOp:      "lt",
				fieldName:  "items",
				tsCmpMode:  "no-mode",
				tsFormat:   time.RFC3339,
				tsCmpValue: timestamp,
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
			checkNode(t, tt.want, got)
		})
	}
}

func getTsLog(t time.Time) string {
	return fmt.Sprintf(`{"ts":"%s"}`, t.Format(time.RFC3339))
}

func TestCheck(t *testing.T) {
	type argsResp struct {
		eventStr string
		want     bool
	}

	timestamp, err := time.Parse(time.RFC3339, time.Now().Format(time.RFC3339))
	require.NoError(t, err)

	tests := []struct {
		name string
		tree treeNode
		data []argsResp
	}{
		{
			name: "equal",
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
			name: "contains",
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
			name: "prefix",
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
			name: "suffix",
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
			name: "regex",
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
			name: "or",
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
			name: "and",
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
			name: "not",
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
			name: "equal_case_insensitive",
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
			name: "contains_case_insensitive",
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
			name: "prefix_case_insensitive",
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
			name: "suffix_case_insensitive",
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
			name: "equal_nil_or_empty_string",
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
			name: "byte_len_cmp_lt",
			tree: treeNode{
				lenCmpOp:  byteLenCmpOpTag,
				cmpOp:     "lt",
				fieldName: "msg",
				cmpValue:  4,
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
			name: "byte_len_cmp_ge",
			tree: treeNode{
				lenCmpOp:  byteLenCmpOpTag,
				cmpOp:     "ge",
				fieldName: "msg",
				cmpValue:  4,
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
			name: "byte_len_cmp_lt_empty_selector",
			tree: treeNode{
				lenCmpOp:  byteLenCmpOpTag,
				cmpOp:     "lt",
				fieldName: "",
				cmpValue:  4,
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
			name: "byte_len_cmp_eq",
			tree: treeNode{
				lenCmpOp:  byteLenCmpOpTag,
				cmpOp:     "eq",
				fieldName: "msg",
				cmpValue:  2,
			},
			data: []argsResp{
				{`{"msg":1}`, false},
				{`{"msg":12}`, true},
				{`{"msg":123}`, false},
			},
		},
		{
			name: "byte_len_cmp_ne",
			tree: treeNode{
				lenCmpOp:  byteLenCmpOpTag,
				cmpOp:     "ne",
				fieldName: "msg",
				cmpValue:  2,
			},
			data: []argsResp{
				{`{"msg":1}`, true},
				{`{"msg":12}`, false},
				{`{"msg":123}`, true},
			},
		},
		{
			name: "array_len_cmp_lt",
			tree: treeNode{
				lenCmpOp:  arrayLenCmpOpTag,
				cmpOp:     "lt",
				fieldName: "numbers",
				cmpValue:  2,
			},
			data: []argsResp{
				{`{"numbers":[]}`, true},
				{`{"numbers":[1]}`, true},
				{`{"numbers":[1, 2]}`, false},
				{`{"numbers":[1, 2, 3]}`, false},
			},
		},
		{
			name: "array_len_cmp_ge",
			tree: treeNode{
				lenCmpOp:  arrayLenCmpOpTag,
				cmpOp:     "ge",
				fieldName: "numbers",
				cmpValue:  2,
			},
			data: []argsResp{
				{`{"numbers":[]}`, false},
				{`{"numbers":[1]}`, false},
				{`{"numbers":[1, 2]}`, true},
				{`{"numbers":[1, 2, 3]}`, true},
			},
		},
		{
			name: "array_len_cmp_lt_empty_selector",
			tree: treeNode{
				lenCmpOp:  arrayLenCmpOpTag,
				cmpOp:     "lt",
				fieldName: "",
				cmpValue:  2,
			},
			data: []argsResp{
				{`[]`, true},
				{`[1]`, true},
				{`[1, 2]`, false},
				{`[1, 2, 3]`, false},
			},
		},
		{
			name: "array_len_cmp_eq",
			tree: treeNode{
				lenCmpOp:  arrayLenCmpOpTag,
				cmpOp:     "eq",
				fieldName: "numbers",
				cmpValue:  2,
			},
			data: []argsResp{
				{`{"numbers":[1]}`, false},
				{`{"numbers":[1, 2]}`, true},
				{`{"numbers":[1, 2, 3]}`, false},
			},
		},
		{
			name: "array_len_cmp_ne",
			tree: treeNode{
				lenCmpOp:  arrayLenCmpOpTag,
				cmpOp:     "ne",
				fieldName: "numbers",
				cmpValue:  2,
			},
			data: []argsResp{
				{`{"numbers":[1]}`, true},
				{`{"numbers":[1, 2]}`, false},
				{`{"numbers":[1, 2, 3]}`, true},
			},
		},
		{
			name: "array_len_cmp_field_not_found",
			tree: treeNode{
				lenCmpOp:  arrayLenCmpOpTag,
				cmpOp:     "lt",
				fieldName: "some",
				cmpValue:  100,
			},
			data: []argsResp{
				{`{"msg":"qwerty"}`, false},
				{`[1, 2, 3]`, false},
			},
		},
		{
			name: "array_len_cmp_field_is_not_array",
			tree: treeNode{
				lenCmpOp:  arrayLenCmpOpTag,
				cmpOp:     "lt",
				fieldName: "items",
				cmpValue:  100,
			},
			data: []argsResp{
				{`{"items":123}`, false},
				{`{"items":"abc"}`, false},
				{`{"items":null}`, false},
				{`{"items":{}}`, false},
				{`{"items":[]}`, true},
			},
		},
		{
			name: "ts_cmp_lt",
			tree: treeNode{
				tsCmpOp:    true,
				cmpOp:      "lt",
				fieldName:  "ts",
				tsFormat:   time.RFC3339,
				tsCmpMode:  "explicit",
				tsCmpValue: timestamp,
			},
			data: []argsResp{
				{getTsLog(timestamp.Add(-2 * time.Second)), true},
				{getTsLog(timestamp.Add(-1 * time.Second)), true},
				{getTsLog(timestamp), false},
				{getTsLog(timestamp.Add(1 * time.Second)), false},
				{getTsLog(timestamp.Add(2 * time.Second)), false},
			},
		},
		{
			name: "ts_cmp_ge",
			tree: treeNode{
				tsCmpOp:    true,
				cmpOp:      "ge",
				fieldName:  "ts",
				tsFormat:   time.RFC3339,
				tsCmpMode:  "explicit",
				tsCmpValue: timestamp,
			},
			data: []argsResp{
				{getTsLog(timestamp.Add(-2 * time.Second)), false},
				{getTsLog(timestamp.Add(-1 * time.Second)), false},
				{getTsLog(timestamp), true},
				{getTsLog(timestamp.Add(1 * time.Second)), true},
				{getTsLog(timestamp.Add(2 * time.Second)), true},
			},
		},
		{
			name: "ts_cmp_eq",
			tree: treeNode{
				tsCmpOp:    true,
				cmpOp:      "eq",
				fieldName:  "ts",
				tsFormat:   time.RFC3339,
				tsCmpMode:  "explicit",
				tsCmpValue: timestamp,
			},
			data: []argsResp{
				{getTsLog(timestamp.Add(-1 * time.Second)), false},
				{getTsLog(timestamp), true},
				{getTsLog(timestamp.Add(1 * time.Second)), false},
			},
		},
		{
			name: "ts_cmp_ne",
			tree: treeNode{
				tsCmpOp:    true,
				cmpOp:      "ne",
				fieldName:  "ts",
				tsFormat:   time.RFC3339,
				tsCmpMode:  "explicit",
				tsCmpValue: timestamp,
			},
			data: []argsResp{
				{getTsLog(timestamp.Add(-1 * time.Second)), true},
				{getTsLog(timestamp), false},
				{getTsLog(timestamp.Add(1 * time.Second)), true},
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			var root Node
			var eventRoot *insaneJSON.Root
			var err error
			t.Parallel()
			root, err = buildTree(tt.tree)
			require.NoError(t, err)
			checker := NewChecker(root)
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

	rawJSON := fmt.Sprintf(`{"user_info": %s}`, userInfoRawJSON)
	eventRoot, err := insaneJSON.DecodeString(rawJSON)
	require.NoError(t, err)

	for index, test := range tests {
		root, err := buildTree(treeNode{
			fieldName: "user_info",
			lenCmpOp:  byteLenCmpOpTag,
			cmpOp:     "lt",
			cmpValue:  test.cmpValue,
		})
		require.NoError(t, err)

		checker := NewChecker(root)
		result := checker.Check(eventRoot)
		require.Equal(t, test.result, result, "invalid result; test id: %d", index)
	}

	eventRoot, err = insaneJSON.DecodeString(userInfoRawJSON)
	require.NoError(t, err)

	for index, test := range tests {
		root, err := buildTree(treeNode{
			fieldName: "",
			lenCmpOp:  byteLenCmpOpTag,
			cmpOp:     "lt",
			cmpValue:  test.cmpValue,
		})
		require.NoError(t, err)

		checker := NewChecker(root)
		result := checker.Check(eventRoot)
		require.Equal(t, test.result, result, "invalid result (empty selector); test id: %d", index)
	}
}

func TestCheckTsCmpModeNow(t *testing.T) {
	const (
		format = "2006-01-02T15:04:05.999Z07:00"
		dt     = 20 * time.Millisecond
	)

	ts := time.Now().Add(dt)

	root, err := buildTree(treeNode{
		tsCmpOp:   true,
		fieldName: "ts",
		cmpOp:     "lt",
		tsFormat:  format,
		tsCmpMode: "now",
	})
	require.NoError(t, err)

	checker := NewChecker(root)

	eventRoot, err := insaneJSON.DecodeString(fmt.Sprintf(`{"ts":"%s"}`, ts.Format(format)))
	require.NoError(t, err)
	defer insaneJSON.Release(eventRoot)

	require.False(t, checker.Check(eventRoot))
	time.Sleep(2 * dt)
	require.True(t, checker.Check(eventRoot))
}

func TestNodeIsEqual(t *testing.T) {
	ts := time.Now()

	fieldNode := treeNode{
		fieldOp:       "equal",
		fieldName:     "service",
		caseSensitive: true,
		values:        [][]byte{[]byte("test-1"), []byte("test-2")},
	}
	byteLenCmpOpNode := treeNode{
		lenCmpOp:  byteLenCmpOpTag,
		cmpOp:     "lt",
		fieldName: "msg",
		cmpValue:  100,
	}
	arrayLenCmpOpNode := treeNode{
		lenCmpOp:  arrayLenCmpOpTag,
		cmpOp:     "lt",
		fieldName: "items",
		cmpValue:  100,
	}
	timestampCmpOpNode := treeNode{
		tsCmpOp:    true,
		cmpOp:      "ge",
		fieldName:  "ts",
		tsFormat:   time.RFC3339,
		tsCmpMode:  "explicit",
		tsCmpValue: ts,
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
						lenCmpOp:  byteLenCmpOpTag,
						cmpOp:     "lt",
						fieldName: "msg",
						cmpValue:  100,
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
			name:    "equal_field_node",
			t1:      fieldNode,
			t2:      fieldNode,
			wantErr: false,
		},
		{
			name:    "equal_byte_len_cmp_node",
			t1:      byteLenCmpOpNode,
			t2:      byteLenCmpOpNode,
			wantErr: false,
		},
		{
			name:    "equal_array_len_cmp_node",
			t1:      arrayLenCmpOpNode,
			t2:      arrayLenCmpOpNode,
			wantErr: false,
		},
		{
			name:    "equal_ts_cmp_node",
			t1:      timestampCmpOpNode,
			t2:      timestampCmpOpNode,
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
			t1:      fieldNode,
			t2:      byteLenCmpOpNode,
			wantErr: true,
		},
		{
			name:    "not_equal_type_mismatch_2",
			t1:      arrayLenCmpOpNode,
			t2:      timestampCmpOpNode,
			wantErr: true,
		},
		{
			name:    "not_equal_type_mismatch_3",
			t1:      timestampCmpOpNode,
			t2:      twoNodes,
			wantErr: true,
		},
		{
			name:    "not_equal_len_cmp_op_mismatch",
			t1:      byteLenCmpOpNode,
			t2:      arrayLenCmpOpNode,
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
				lenCmpOp:  byteLenCmpOpTag,
				cmpOp:     "lt",
				fieldName: "msg",
				cmpValue:  100,
			},
			t2: treeNode{
				lenCmpOp:  byteLenCmpOpTag,
				cmpOp:     "gt",
				fieldName: "msg",
				cmpValue:  100,
			},
			wantErr: true,
		},
		{
			name: "not_equal_byte_len_cmp_op_field_mismatch",
			t1: treeNode{
				lenCmpOp:  byteLenCmpOpTag,
				cmpOp:     "lt",
				fieldName: "msg",
				cmpValue:  100,
			},
			t2: treeNode{
				lenCmpOp:  byteLenCmpOpTag,
				cmpOp:     "lt",
				fieldName: "pod",
				cmpValue:  100,
			},
			wantErr: true,
		},
		{
			name: "not_equal_byte_len_cmp_op_value_mismatch",
			t1: treeNode{
				lenCmpOp:  byteLenCmpOpTag,
				cmpOp:     "lt",
				fieldName: "msg",
				cmpValue:  100,
			},
			t2: treeNode{
				lenCmpOp:  byteLenCmpOpTag,
				cmpOp:     "lt",
				fieldName: "msg",
				cmpValue:  200,
			},
			wantErr: true,
		},
		{
			name: "not_equal_array_len_cmp_op_mismatch",
			t1: treeNode{
				lenCmpOp:  arrayLenCmpOpTag,
				cmpOp:     "lt",
				fieldName: "items",
				cmpValue:  100,
			},
			t2: treeNode{
				lenCmpOp:  arrayLenCmpOpTag,
				cmpOp:     "gt",
				fieldName: "items",
				cmpValue:  100,
			},
			wantErr: true,
		},
		{
			name: "not_equal_array_len_cmp_op_field_mismatch",
			t1: treeNode{
				lenCmpOp:  arrayLenCmpOpTag,
				cmpOp:     "lt",
				fieldName: "items",
				cmpValue:  100,
			},
			t2: treeNode{
				lenCmpOp:  arrayLenCmpOpTag,
				cmpOp:     "lt",
				fieldName: "numbers",
				cmpValue:  100,
			},
			wantErr: true,
		},
		{
			name: "not_equal_array_len_cmp_op_value_mismatch",
			t1: treeNode{
				lenCmpOp:  arrayLenCmpOpTag,
				cmpOp:     "lt",
				fieldName: "items",
				cmpValue:  100,
			},
			t2: treeNode{
				lenCmpOp:  arrayLenCmpOpTag,
				cmpOp:     "lt",
				fieldName: "items",
				cmpValue:  200,
			},
			wantErr: true,
		},
		{
			name: "not_equal_ts_cmp_op_mismatch_format",
			t1: treeNode{
				tsCmpOp:   true,
				tsFormat:  time.RFC3339,
				cmpOp:     "lt",
				tsCmpMode: "now",
			},
			t2: treeNode{
				tsCmpOp:   true,
				tsFormat:  time.RFC3339Nano,
				cmpOp:     "lt",
				tsCmpMode: "now",
			},
			wantErr: true,
		},
		{
			name: "not_equal_ts_cmp_op_mismatch_op",
			t1: treeNode{
				tsCmpOp:   true,
				tsFormat:  time.RFC3339,
				cmpOp:     "lt",
				tsCmpMode: "now",
			},
			t2: treeNode{
				tsCmpOp:   true,
				tsFormat:  time.RFC3339,
				cmpOp:     "gt",
				tsCmpMode: "now",
			},
			wantErr: true,
		},
		{
			name: "not_equal_ts_cmp_op_mismatch_mode",
			t1: treeNode{
				tsCmpOp:   true,
				tsFormat:  time.RFC3339,
				cmpOp:     "lt",
				tsCmpMode: "now",
			},
			t2: treeNode{
				tsCmpOp:   true,
				tsFormat:  time.RFC3339,
				cmpOp:     "lt",
				tsCmpMode: "explicit",
			},
			wantErr: true,
		},
		{
			name: "not_equal_ts_cmp_op_mismatch_value",
			t1: treeNode{
				tsCmpOp:    true,
				tsFormat:   time.RFC3339,
				cmpOp:      "lt",
				tsCmpMode:  "explicit",
				tsCmpValue: ts,
			},
			t2: treeNode{
				tsCmpOp:    true,
				tsFormat:   time.RFC3339,
				cmpOp:      "lt",
				tsCmpMode:  "explicit",
				tsCmpValue: ts.Add(1 * time.Second),
			},
			wantErr: true,
		},
		{
			name: "not_equal_ts_cmp_op_mismatch_field",
			t1: treeNode{
				tsCmpOp:    true,
				tsFormat:   time.RFC3339,
				cmpOp:      "lt",
				tsCmpMode:  "explicit",
				tsCmpValue: ts,
				fieldName:  "a.ts",
			},
			t2: treeNode{
				tsCmpOp:    true,
				tsFormat:   time.RFC3339,
				cmpOp:      "lt",
				tsCmpMode:  "explicit",
				tsCmpValue: ts,
				fieldName:  "ts",
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
			c1 := NewChecker(root1)
			c2 := NewChecker(root2)
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
