package fd

import (
	"bytes"
	"errors"
	"fmt"
	"testing"

	"github.com/bitly/go-simplejson"
	"github.com/ozontech/file.d/pipeline"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_extractConditions(t *testing.T) {
	j, err := simplejson.NewJson([]byte(`{"service": ["address-api", "tarifficator-api", "card-api", "teams-api"]}`))
	require.NoError(t, err)
	got, err := extractConditions(j)
	require.NoError(t, err)
	expected := pipeline.MatchConditions{
		pipeline.MatchCondition{
			Field:  []string{"service"},
			Values: []string{"address-api", "tarifficator-api", "card-api", "teams-api"},
		},
	}
	require.Equal(t, expected, got)

	j, err = simplejson.NewJson([]byte(`{"service": "address-api"}`))
	require.NoError(t, err)
	got, err = extractConditions(j)
	require.NoError(t, err)
	expected = pipeline.MatchConditions{
		pipeline.MatchCondition{
			Field:  []string{"service"},
			Values: []string{"address-api"},
		},
	}
	require.Equal(t, expected, got)
}

type doIfTreeNode struct {
	fieldOp       string
	fieldName     string
	caseSensitive bool
	cmpOp         string
	values        [][]byte

	logicalOp string
	operands  []*doIfTreeNode
}

// nolint:gocritic
func buildDoIfTree(node *doIfTreeNode) (pipeline.DoIfNode, error) {
	if node.fieldOp != "" {
		return pipeline.NewFieldOpNode(
			node.fieldOp,
			node.fieldName,
			node.caseSensitive,
			node.cmpOp,
			node.values,
		)
	} else if node.logicalOp != "" {
		operands := make([]pipeline.DoIfNode, 0)
		for _, operandNode := range node.operands {
			operand, err := buildDoIfTree(operandNode)
			if err != nil {
				return nil, fmt.Errorf("failed to build tree: %w", err)
			}
			operands = append(operands, operand)
		}
		return pipeline.NewLogicalNode(
			node.logicalOp,
			operands,
		)
	}
	return nil, errors.New("unknown type of node")
}

func Test_extractDoIfChecker(t *testing.T) {
	type args struct {
		cfgStr string
	}

	tests := []struct {
		name    string
		args    args
		want    *doIfTreeNode
		wantErr bool
	}{
		{
			name: "ok",
			args: args{
				cfgStr: `
		{
			"op": "not",
			"operands": [
				{
					"op": "and",
					"operands": [
						{
							"op": "equal",
							"field": "service",
							"values": [null, ""],
							"case_sensitive": false
						},
						{
							"op": "prefix",
							"field": "log.msg",
							"values": ["test-1", "test-2"],
							"case_sensitive": false
						},
						{
							"op": "or",
							"operands": [
								{
									"op": "suffix",
									"field": "service",
									"values": ["test-svc-1", "test-svc-2"],
									"case_sensitive": true
								},
								{
									"op": "contains",
									"field": "pod",
									"values": ["test"]
								},
								{
									"op": "regex",
									"field": "message",
									"values": ["test-\\d+", "test-msg-\\d+"]
								},
								{
									"op": "bytes_len_cmp",
									"field": "message",
									"cmp_op": "lt",
									"values": ["100"]
								}
							]
						}
					]
				}
			]
		}
						`,
			},
			want: &doIfTreeNode{
				logicalOp: "not",
				operands: []*doIfTreeNode{
					{
						logicalOp: "and",
						operands: []*doIfTreeNode{
							{
								fieldOp:       "equal",
								fieldName:     "service",
								values:        [][]byte{nil, []byte("")},
								caseSensitive: false,
							},
							{
								fieldOp:       "prefix",
								fieldName:     "log.msg",
								values:        [][]byte{[]byte("test-1"), []byte("test-2")},
								caseSensitive: false,
							},
							{
								logicalOp: "or",
								operands: []*doIfTreeNode{
									{
										fieldOp:       "suffix",
										fieldName:     "service",
										values:        [][]byte{[]byte("test-svc-1"), []byte("test-svc-2")},
										caseSensitive: true,
									},
									{
										fieldOp:       "contains",
										fieldName:     "pod",
										values:        [][]byte{[]byte("test")},
										caseSensitive: true,
									},
									{
										fieldOp:       "regex",
										fieldName:     "message",
										values:        [][]byte{[]byte(`test-\d+`), []byte(`test-msg-\d+`)},
										caseSensitive: true,
									},
									{
										fieldOp:       "bytes_len_cmp",
										fieldName:     "message",
										cmpOp:         "lt",
										values:        [][]byte{[]byte("100")},
										caseSensitive: true,
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "ok_not_map",
			args: args{
				cfgStr: `[{"field":"val"}]`,
			},
			wantErr: false,
		},
		{
			name: "ok_single_val",
			args: args{
				cfgStr: `{
					"op":"or",
					"operands":[
						{"op":"equal","field":"service","values":null},
						{"op":"equal","field":"service","values":""},
						{"op":"equal","field":"service","values":"test"}
					]
				}`,
			},
			want: &doIfTreeNode{
				logicalOp: "or",
				operands: []*doIfTreeNode{
					{
						fieldOp:       "equal",
						fieldName:     "service",
						values:        [][]byte{nil},
						caseSensitive: true,
					},
					{
						fieldOp:       "equal",
						fieldName:     "service",
						values:        [][]byte{[]byte("")},
						caseSensitive: true,
					},
					{
						fieldOp:       "equal",
						fieldName:     "service",
						values:        [][]byte{[]byte("test")},
						caseSensitive: true,
					},
				},
			},
			wantErr: false,
		},
		{
			name: "error_no_op_field",
			args: args{
				cfgStr: `{"field": "val"}`,
			},
			wantErr: true,
		},
		{
			name: "error_invalid_op_name",
			args: args{
				cfgStr: `{"op": "invalid"}`,
			},
			wantErr: true,
		},
		{
			name: "error_invalid_field_op",
			args: args{
				cfgStr: `{"op": "equal"}`,
			},
			wantErr: true,
		},
		{
			name: "error_invalid_logical_op",
			args: args{
				cfgStr: `{"op": "or"}`,
			},
			wantErr: true,
		},
		{
			name: "error_invalid_logical_op_operand",
			args: args{
				cfgStr: `{"op": "or", "operands": [{"op": "equal"}]}`,
			},
			wantErr: true,
		},
		{
			name: "error_no_cmp_value",
			args: args{
				cfgStr: `{"op": "bytes_len_cmp", "field": "message", "values": []}`,
			},
			wantErr: true,
		},
		{
			name: "error_too_many_cmp_values",
			args: args{
				cfgStr: `{"op": "bytes_len_cmp", "field": "message", "values": ["1", "2", "3"]}`,
			},
			wantErr: true,
		},
		{
			name: "error_invalid_cmp_value",
			args: args{
				cfgStr: `{"op": "bytes_len_cmp", "field": "message", "values": ["can't parse it"]}`,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			reader := bytes.NewBufferString(tt.args.cfgStr)
			actionJSON, err := simplejson.NewFromReader(reader)
			require.NoError(t, err)
			got, err := extractDoIfChecker(actionJSON)
			if (err != nil) != tt.wantErr {
				t.Errorf("extractDoIfChecker() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if tt.want == nil {
				assert.Nil(t, got)
				return
			}
			wantTree, err := buildDoIfTree(tt.want)
			require.NoError(t, err)
			wantDoIfChecker := pipeline.NewDoIfChecker(wantTree)
			assert.NoError(t, wantDoIfChecker.IsEqualTo(got))
		})
	}
}
