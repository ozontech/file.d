package do_if

import (
	"bytes"
	"testing"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/ozontech/file.d/pipeline/do_if/data_checker"
	"github.com/ozontech/file.d/pipeline/do_if/logic"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractNode(t *testing.T) {
	tests := []struct {
		name     string
		raw      string
		expected Node
		wantErr  bool
	}{
		{
			name: "ok",
			raw: `
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
								"op": "byte_len_cmp",
								"field": "msg",
								"cmp_op": "gt",
								"value": 100
							},
							{
								"op": "array_len_cmp",
								"field": "items",
								"cmp_op": "lt",
								"value": 100
							},
							{
								"op": "ts_cmp",
								"field": "timestamp",
								"cmp_op": "lt",
								"value": "2009-11-10T23:00:00Z",
								"format": "2006-01-02T15:04:05.999999999Z07:00",
								"update_interval": "15s"
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
									}
								]
							}
						]
					}
				]
			}`,
			expected: &logicalNode{
				op: logic.Not,
				operands: []Node{
					&logicalNode{
						op: logic.And,
						operands: []Node{
							&fieldOpNode{
								fieldPath:    []string{"service"},
								fieldPathStr: "service",
								checker:      data_checker.MustNew("equal", false, [][]byte{nil, []byte("")}),
							},
							&fieldOpNode{
								fieldPath:    []string{"log", "msg"},
								fieldPathStr: "log.msg",
								checker: data_checker.MustNew(
									"prefix", false, [][]byte{[]byte("test-1"), []byte("test-2")}),
							},
							&lenCmpOpNode{
								lenCmpOp:  byteLenCmpOp,
								fieldPath: []string{"msg"},
								cmpOp:     cmpOpGreater,
								cmpValue:  100,
							},
							&lenCmpOpNode{
								lenCmpOp:  arrayLenCmpOp,
								fieldPath: []string{"items"},
								cmpOp:     cmpOpLess,
								cmpValue:  100,
							},
							&tsCmpOpNode{
								fieldPath:        []string{"timestamp"},
								format:           time.RFC3339Nano,
								cmpOp:            cmpOpLess,
								cmpValChangeMode: cmpValChangeModeConst,
								constCmpValue: time.Date(
									2009, time.November, 10, 23, 0, 0, 0, time.UTC,
								).UnixNano(),
								updateInterval: 15 * time.Second,
							},
							&logicalNode{
								op: logic.Or,
								operands: []Node{
									&fieldOpNode{
										fieldPath:    []string{"service"},
										fieldPathStr: "service",
										checker: data_checker.MustNew(
											"suffix",
											true,
											[][]byte{[]byte("test-svc-1"), []byte("test-svc-2")},
										),
									},
									&fieldOpNode{
										fieldPath:    []string{"pod"},
										fieldPathStr: "pod",
										checker: data_checker.MustNew(
											"contains",
											true,
											[][]byte{[]byte("test")},
										),
									},
									&fieldOpNode{
										fieldPath:    []string{"message"},
										fieldPathStr: "message",
										checker: data_checker.MustNew(
											"regex",
											true,
											[][]byte{[]byte(`test-\d+`), []byte(`test-msg-\d+`)},
										),
									},
								},
							},
						},
					},
				},
			},
		},
	}
	/*
			{
				name: "ok_not_map",
				args: args{
					cfgStr: `[{"field":"val"}]`,
				},
				wantErr: false,
			},
			{
				name: "ok_byte_len_cmp_op",
				args: args{
					cfgStr: `{"op":"byte_len_cmp","field":"data","cmp_op":"lt","value":10}`,
				},
				want: &doIfTreeNode{
					lenCmpOp:  "byte_len_cmp",
					cmpOp:     "lt",
					fieldName: "data",
					cmpValue:  10,
				},
			},
			{
				name: "ok_array_len_cmp_op",
				args: args{
					cfgStr: `{"op":"array_len_cmp","field":"items","cmp_op":"lt","value":10}`,
				},
				want: &doIfTreeNode{
					lenCmpOp:  "array_len_cmp",
					cmpOp:     "lt",
					fieldName: "items",
					cmpValue:  10,
				},
			},
			{
				name: "ok_ts_cmp_op",
				args: args{
					cfgStr: `{
						"op": "ts_cmp",
						"field": "timestamp",
						"cmp_op": "lt",
						"value": "2009-11-10T23:00:00Z",
						"value_shift": "-24h",
						"format": "2006-01-02T15:04:05Z07:00",
						"update_interval": "15s"}`,
				},
				want: &doIfTreeNode{
					tsCmpOp:            true,
					cmpOp:              "lt",
					fieldName:          "timestamp",
					tsFormat:           time.RFC3339,
					tsCmpValue:         time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
					tsCmpValueShift:    -24 * time.Hour,
					tsCmpValChangeMode: tsCmpModeConstTag,
					tsUpdateInterval:   15 * time.Second,
				},
			},
			{
				name: "ok_ts_cmp_op_default_settings",
				args: args{
					cfgStr: `{
						"op": "ts_cmp",
						"field": "timestamp",
						"cmp_op": "lt",
						"value": "now"}`,
				},
				want: &doIfTreeNode{
					tsCmpOp:            true,
					cmpOp:              "lt",
					fieldName:          "timestamp",
					tsCmpValChangeMode: tsCmpModeNowTag,
					tsFormat:           defaultTsFormat,
					tsCmpValueShift:    0,
					tsUpdateInterval:   defaultTsCmpValUpdateInterval,
				},
			},
			{
				name: "ok_ts_cmp_op_format_alias",
				args: args{
					cfgStr: `{
						"op": "ts_cmp",
						"field": "timestamp",
						"cmp_op": "lt",
						"format": "rfc3339",
						"value": "now"}`,
				},
				want: &doIfTreeNode{
					tsCmpOp:            true,
					cmpOp:              "lt",
					fieldName:          "timestamp",
					tsCmpValChangeMode: tsCmpModeNowTag,
					tsFormat:           time.RFC3339,
					tsCmpValueShift:    0,
					tsUpdateInterval:   defaultTsCmpValUpdateInterval,
				},
			},

			{
				name: "ok_check_type",
				args: args{
					cfgStr: `{
						"op": "check_type",
						"field": "log",
						"values": ["obj","arr"]
					}`,
				},
				want: &doIfTreeNode{
					checkTypeOp: true,
					fieldName:   "log",
					values: [][]byte{
						[]byte("obj"),
						[]byte("arr"),
					},
				},
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
				name: "error_invalid_case_sensitive_type",
				args: args{
					cfgStr: `{
						"op": "equal",
						"field": "a",
						"values": ["abc"],
						"case_sensitive": "not bool"}`,
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
				name: "error_byte_len_cmp_op_no_field",
				args: args{
					cfgStr: `{"op":"byte_len_cmp","cmp_op":"lt","value":10}`,
				},
				wantErr: true,
			},
			{
				name: "error_array_len_cmp_op_no_field",
				args: args{
					cfgStr: `{"op":"array_len_cmp","cmp_op":"lt","value":10}`,
				},
				wantErr: true,
			},
			{
				name: "error_byte_len_cmp_op_field_is_not_string",
				args: args{
					cfgStr: `{"op":"byte_len_cmp","field":123,"cmp_op":"lt","value":10}`,
				},
				wantErr: true,
			},
			{
				name: "error_byte_len_cmp_op_no_cmp_op",
				args: args{
					cfgStr: `{"op":"byte_len_cmp","field":"data","value":10}`,
				},
				wantErr: true,
			},
			{
				name: "error_byte_len_cmp_op_cmp_op_is_not_string",
				args: args{
					cfgStr: `{"op":"byte_len_cmp","field":"data","cmp_op":123,"value":10}`,
				},
				wantErr: true,
			},
			{
				name: "error_byte_len_cmp_op_no_cmp_value",
				args: args{
					cfgStr: `{"op":"byte_len_cmp","field":"data","cmp_op":"lt"}`,
				},
				wantErr: true,
			},
			{
				name: "error_byte_len_cmp_op_cmp_value_is_not_integer",
				args: args{
					cfgStr: `{"op":"byte_len_cmp","field":"data","cmp_op":"lt","value":"abc"}`,
				},
				wantErr: true,
			},
			{
				name:    "error_byte_len_cmp_op_invalid_cmp_op",
				args:    args{cfgStr: `{"op":"byte_len_cmp","field":"data","cmp_op":"ABC","value":10}`},
				wantErr: true,
			},
			{
				name:    "error_byte_len_cmp_op_negative_cmp_value",
				args:    args{cfgStr: `{"op":"byte_len_cmp","field":"data","cmp_op":"lt","value":-1}`},
				wantErr: true,
			},
			{
				name: "error_ts_cmp_op_no_field",
				args: args{
					cfgStr: `{"op": "ts_cmp","cmp_op": "lt"}`,
				},
				wantErr: true,
			},
			{
				name: "error_ts_cmp_op_field_is_not_string",
				args: args{
					cfgStr: `{"op":"ts_cmp","field":123}`,
				},
				wantErr: true,
			},
			{
				name: "error_ts_cmp_op_no_cmp_op",
				args: args{
					cfgStr: `{"op":"ts_cmp","field":"timestamp"}`,
				},
				wantErr: true,
			},
			{
				name: "error_ts_cmp_op_invalid_format_type",
				args: args{
					cfgStr: `{
						"op": "ts_cmp",
						"field": "timestamp",
						"cmp_op": "lt",
						"format": 1000,
						"value": "now"}`,
				},
				wantErr: true,
			},
			{
				name: "error_ts_cmp_op_invalid_value_shift_type",
				args: args{
					cfgStr: `{
						"op": "ts_cmp",
						"field": "timestamp",
						"cmp_op": "lt",
						"value": "2009-11-10T23:00:00Z",
						"value_shift": 1000,
						"format": "2006-01-02T15:04:05Z07:00",
						"update_interval": "15s"}`,
				},
				wantErr: true,
			},
			{
				name: "error_ts_cmp_op_invalid_update_interval_type",
				args: args{
					cfgStr: `{
						"op": "ts_cmp",
						"field": "timestamp",
						"cmp_op": "lt",
						"value": "2009-11-10T23:00:00Z",
						"format": "2006-01-02T15:04:05Z07:00",
						"update_interval": false}`,
				},
				wantErr: true,
			},
			{
				name: "error_ts_cmp_op_cmp_op_is_not_string",
				args: args{
					cfgStr: `{"op":"ts_cmp","field":"timestamp","cmp_op":123}`,
				},
				wantErr: true,
			},
			{
				name: "error_ts_cmp_op_no_cmp_value",
				args: args{
					cfgStr: `{"op":"ts_cmp","field":"timestamp","cmp_op":"lt"}`,
				},
				wantErr: true,
			},
			{
				name: "error_ts_cmp_op_cmp_value_is_not_string",
				args: args{
					cfgStr: `{"op":"ts_cmp","field":"timestamp","cmp_op":"lt","value":123}`,
				},
				wantErr: true,
			},
			{
				name: "error_ts_cmp_op_invalid_cmp_value",
				args: args{
					cfgStr: `{"op":"ts_cmp","field":"timestamp","cmp_op":"lt","value":"qwe"}`,
				},
				wantErr: true,
			},
			{
				name: "error_ts_cmp_op_invalid_cmp_op",
				args: args{
					cfgStr: `{"op":"ts_cmp","field":"timestamp","cmp_op":"qwe","value":"2009-11-10T23:00:00Z"}`,
				},
				wantErr: true,
			},
			{
				name: "error_ts_cmp_op_invalid_update_interval",
				args: args{
					cfgStr: `{
						"op": "ts_cmp",
						"field": "timestamp",
						"cmp_op": "lt",
						"value": "2009-11-10T23:00:00Z",
						"update_interval": "qwe"}`,
				},
				wantErr: true,
			},
			{
				name: "error_check_type_op_empty_values",
				args: args{
					cfgStr: `{
						"op": "check_type",
						"field": "log",
						"values": []
					}`,
				},
				wantErr: true,
			},
			{
				name: "error_check_type_op_invalid_value",
				args: args{
					cfgStr: `{
						"op": "check_type",
						"field": "log",
						"values": ["unknown_type"]
					}`,
				},
				wantErr: true,
			},
			{
				name: "error_check_type_op_no_field",
				args: args{
					cfgStr: `{
						"op": "check_type",
						"values": ["obj"]
					}`,
				},
				wantErr: true,
			},
		}
	*/
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			reader := bytes.NewBufferString(tt.raw)
			actionJSON, err := simplejson.NewFromReader(reader)
			require.NoError(t, err)

			got, err := ExtractNode(actionJSON.MustMap())
			require.Equal(t, err != nil, tt.wantErr)

			if tt.wantErr {
				return
			}

			assert.NoError(t, got.isEqualTo(tt.expected, 1))
		})
	}
}
