package do_if

import (
	"bytes"
	"testing"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/ozontech/file.d/pipeline/do_if/logic"
	"github.com/ozontech/file.d/pipeline/do_if/str_checker"
	insaneJSON "github.com/ozontech/insane-json"
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
							&stringOpNode{
								fieldPath:    []string{"service"},
								fieldPathStr: "service",
								checker:      str_checker.MustNew("equal", false, [][]byte{nil, []byte("")}),
							},
							&stringOpNode{
								fieldPath:    []string{"log", "msg"},
								fieldPathStr: "log.msg",
								checker: str_checker.MustNew(
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
									&stringOpNode{
										fieldPath:    []string{"service"},
										fieldPathStr: "service",
										checker: str_checker.MustNew(
											"suffix",
											true,
											[][]byte{[]byte("test-svc-1"), []byte("test-svc-2")},
										),
									},
									&stringOpNode{
										fieldPath:    []string{"pod"},
										fieldPathStr: "pod",
										checker: str_checker.MustNew(
											"contains",
											true,
											[][]byte{[]byte("test")},
										),
									},
									&stringOpNode{
										fieldPath:    []string{"message"},
										fieldPathStr: "message",
										checker: str_checker.MustNew(
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
		{
			name: "ok_field_op_node_data_type_event",
			raw:  `{"op":"equal", "values":["a"], "data":"event"}`,
			expected: &stringOpNode{
				dataType: dataTypeEvent,
				checker:  str_checker.MustNew("equal", true, [][]byte{[]byte("a")}),
			},
		},
		{
			name: "ok_field_op_node_data_type_source_name",
			raw:  `{"op":"equal", "values":["a"], "data":"source_name"}`,
			expected: &stringOpNode{
				dataType: dataTypeSourceName,
				checker:  str_checker.MustNew("equal", true, [][]byte{[]byte("a")}),
			},
		},
		{
			name: "ok_field_op_node_data_type_meta",
			raw:  `{"op":"equal", "values":["a"], "data":"meta.name"}`,
			expected: &stringOpNode{
				dataType: dataTypeMeta,
				metaKey:  "name",
				checker:  str_checker.MustNew("equal", true, [][]byte{[]byte("a")}),
			},
		},
		{
			name: "ok_byte_len_cmp_op",
			raw:  `{"op":"byte_len_cmp","field":"data","cmp_op":"lt","value":10}`,
			expected: &lenCmpOpNode{
				lenCmpOp:  byteLenCmpOp,
				fieldPath: []string{"data"},
				cmpOp:     cmpOpLess,
				cmpValue:  10,
			},
		},
		{
			name: "ok_array_len_cmp_op",
			raw:  `{"op":"array_len_cmp","field":"items","cmp_op":"lt","value":10}`,
			expected: &lenCmpOpNode{
				lenCmpOp:  arrayLenCmpOp,
				fieldPath: []string{"items"},
				cmpOp:     cmpOpLess,
				cmpValue:  10,
			},
		},
		{
			name: "ok_ts_cmp_op",
			raw: `{
				"op": "ts_cmp",
				"field": "timestamp",
				"cmp_op": "lt",
				"value": "2009-11-10T23:00:00Z",
				"value_shift": "-24h",
				"format": "2006-01-02T15:04:05Z07:00",
				"update_interval": "15s"
			}`,
			expected: &tsCmpOpNode{
				fieldPath:        []string{"timestamp"},
				format:           time.RFC3339,
				cmpOp:            cmpOpLess,
				cmpValChangeMode: cmpValChangeModeConst,
				constCmpValue: time.Date(
					2009, time.November, 10, 23, 0, 0, 0, time.UTC,
				).UnixNano(),
				cmpValueShift:  (-24 * time.Hour).Nanoseconds(),
				updateInterval: 15 * time.Second,
			},
		},
		{
			name: "ok_ts_cmp_op_default_settings",
			raw: `{
				"op": "ts_cmp",
				"field": "timestamp",
				"cmp_op": "lt",
				"value": "now"
			}`,
			expected: &tsCmpOpNode{
				fieldPath:        []string{"timestamp"},
				format:           time.RFC3339Nano,
				cmpOp:            cmpOpLess,
				cmpValChangeMode: cmpValChangeModeNow,
				constCmpValue:    time.Time{}.UnixNano(),
				updateInterval:   defaultTsCmpValUpdateInterval,
				cmpValueShift:    0,
			},
		},
		{
			name: "ok_ts_cmp_op_format_alias",
			raw: `{
				"op": "ts_cmp",
				"field": "timestamp",
				"cmp_op": "lt",
				"format": "rfc3339",
				"value": "now"
			}`,
			expected: &tsCmpOpNode{
				fieldPath:        []string{"timestamp"},
				format:           time.RFC3339,
				cmpOp:            cmpOpLess,
				cmpValChangeMode: cmpValChangeModeNow,
				constCmpValue:    time.Time{}.UnixNano(),
				cmpValueShift:    0,
				updateInterval:   defaultTsCmpValUpdateInterval,
			},
		},
		{
			name: "ok_check_type",
			raw: `{
				"op": "check_type",
				"field": "log",
				"values": ["obj","arr"]
			}`,
			expected: &checkTypeOpNode{
				fieldPath:    []string{"log"},
				fieldPathStr: "log",
				checkTypeFns: []checkTypeFn{
					func(n *insaneJSON.Node) bool {
						return n.IsObject()
					},
					func(n *insaneJSON.Node) bool {
						return n.IsArray()
					},
				},
			},
		},
		{
			name: "ok_single_val",
			raw: `{
				"op":"or",
				"operands":[
					{"op":"equal","field":"service","values":null},
					{"op":"equal","field":"service","values":""},
					{"op":"equal","field":"service","values":"test"}
				]
			}`,
			expected: &logicalNode{
				op: logic.Or,
				operands: []Node{
					&stringOpNode{
						fieldPath:    []string{"service"},
						fieldPathStr: "service",
						checker:      str_checker.MustNew("equal", true, [][]byte{nil}),
					},
					&stringOpNode{
						fieldPath:    []string{"service"},
						fieldPathStr: "service",
						checker:      str_checker.MustNew("equal", true, [][]byte{[]byte("")}),
					},
					&stringOpNode{
						fieldPath:    []string{"service"},
						fieldPathStr: "service",
						checker:      str_checker.MustNew("equal", true, [][]byte{[]byte("test")}),
					},
				},
			},
			wantErr: false,
		},
		{
			name:    "error_no_op_field",
			raw:     `{"field": "val"}`,
			wantErr: true,
		},
		{
			name:    "error_invalid_op_name",
			raw:     `{"op": "invalid"}`,
			wantErr: true,
		},
		{
			name:    "error_invalid_field_op",
			raw:     `{"op": "equal"}`,
			wantErr: true,
		},
		{
			name: "error_invalid_case_sensitive_type",
			raw: `{
				"op": "equal",
				"field": "a",
				"values": ["abc"],
				"case_sensitive": "not bool"
			}`,
			wantErr: true,
		},
		{
			name:    "error_invalid_logical_op",
			raw:     `{"op": "or"}`,
			wantErr: true,
		},
		{
			name:    "error_invalid_logical_op_operand",
			raw:     `{"op": "or", "operands": [{"op": "equal"}]}`,
			wantErr: true,
		},
		{
			name:    "error_byte_len_cmp_op_no_field",
			raw:     `{"op":"byte_len_cmp","cmp_op":"lt","value":10}`,
			wantErr: true,
		},
		{
			name:    "error_array_len_cmp_op_no_field",
			raw:     `{"op":"array_len_cmp","cmp_op":"lt","value":10}`,
			wantErr: true,
		},
		{
			name:    "error_byte_len_cmp_op_field_is_not_string",
			raw:     `{"op":"byte_len_cmp","field":123,"cmp_op":"lt","value":10}`,
			wantErr: true,
		},
		{
			name:    "error_byte_len_cmp_op_no_cmp_op",
			raw:     `{"op":"byte_len_cmp","field":"data","value":10}`,
			wantErr: true,
		},
		{
			name:    "error_byte_len_cmp_op_cmp_op_is_not_string",
			raw:     `{"op":"byte_len_cmp","field":"data","cmp_op":123,"value":10}`,
			wantErr: true,
		},
		{
			name:    "error_byte_len_cmp_op_no_cmp_value",
			raw:     `{"op":"byte_len_cmp","field":"data","cmp_op":"lt"}`,
			wantErr: true,
		},
		{
			name:    "error_byte_len_cmp_op_cmp_value_is_not_integer",
			raw:     `{"op":"byte_len_cmp","field":"data","cmp_op":"lt","value":"abc"}`,
			wantErr: true,
		},
		{
			name:    "error_byte_len_cmp_op_invalid_cmp_op",
			raw:     `{"op":"byte_len_cmp","field":"data","cmp_op":"ABC","value":10}`,
			wantErr: true,
		},
		{
			name:    "error_byte_len_cmp_op_negative_cmp_value",
			raw:     `{"op":"byte_len_cmp","field":"data","cmp_op":"lt","value":-1}`,
			wantErr: true,
		},
		{
			name:    "error_ts_cmp_op_no_field",
			raw:     `{"op": "ts_cmp","cmp_op": "lt"}`,
			wantErr: true,
		},
		{
			name:    "error_ts_cmp_op_field_is_not_string",
			raw:     `{"op":"ts_cmp","field":123}`,
			wantErr: true,
		},
		{
			name:    "error_ts_cmp_op_no_cmp_op",
			raw:     `{"op":"ts_cmp","field":"timestamp"}`,
			wantErr: true,
		},
		{
			name: "error_ts_cmp_op_invalid_format_type",
			raw: `{
				"op": "ts_cmp",
				"field": "timestamp",
				"cmp_op": "lt",
				"format": 1000,
				"value": "now"
			}`,
			wantErr: true,
		},
		{
			name: "error_ts_cmp_op_invalid_value_shift_type",
			raw: `{
				"op": "ts_cmp",
				"field": "timestamp",
				"cmp_op": "lt",
				"value": "2009-11-10T23:00:00Z",
				"value_shift": 1000,
				"format": "2006-01-02T15:04:05Z07:00",
				"update_interval": "15s"
			}`,
			wantErr: true,
		},
		{
			name: "error_ts_cmp_op_invalid_update_interval_type",
			raw: `{
				"op": "ts_cmp",
				"field": "timestamp",
				"cmp_op": "lt",
				"value": "2009-11-10T23:00:00Z",
				"format": "2006-01-02T15:04:05Z07:00",
				"update_interval": false
			}`,
			wantErr: true,
		},
		{
			name:    "error_ts_cmp_op_cmp_op_is_not_string",
			raw:     `{"op":"ts_cmp","field":"timestamp","cmp_op":123}`,
			wantErr: true,
		},
		{
			name:    "error_ts_cmp_op_no_cmp_value",
			raw:     `{"op":"ts_cmp","field":"timestamp","cmp_op":"lt"}`,
			wantErr: true,
		},
		{
			name:    "error_ts_cmp_op_cmp_value_is_not_string",
			raw:     `{"op":"ts_cmp","field":"timestamp","cmp_op":"lt","value":123}`,
			wantErr: true,
		},
		{
			name:    "error_ts_cmp_op_invalid_cmp_value",
			raw:     `{"op":"ts_cmp","field":"timestamp","cmp_op":"lt","value":"qwe"}`,
			wantErr: true,
		},
		{
			name:    "error_ts_cmp_op_invalid_cmp_op",
			raw:     `{"op":"ts_cmp","field":"timestamp","cmp_op":"qwe","value":"2009-11-10T23:00:00Z"}`,
			wantErr: true,
		},
		{
			name: "error_ts_cmp_op_invalid_update_interval",
			raw: `{
				"op": "ts_cmp",
				"field": "timestamp",
				"cmp_op": "lt",
				"value": "2009-11-10T23:00:00Z",
				"update_interval": "qwe"
			}`,
			wantErr: true,
		},
		{
			name:    "error_field_op_node_data_type_type_mismatch",
			raw:     `{"op":"equal", "values":["a"], "data":123}`,
			wantErr: true,
		},
		{
			name:    "error_field_op_node_data_type_unparsable",
			raw:     `{"op":"equal", "values":["a"], "data":"some"}`,
			wantErr: true,
		},
		{
			name: "error_check_type_op_empty_values",
			raw: `{
				"op": "check_type",
				"field": "log",
				"values": []
			}`,
			wantErr: true,
		},
		{
			name: "error_check_type_op_invalid_value",
			raw: `{
				"op": "check_type",
				"field": "log",
				"values": ["unknown_type"]
			}`,
			wantErr: true,
		},
		{
			name: "error_check_type_op_no_field",
			raw: `{
				"op": "check_type",
				"values": ["obj"]
			}`,
			wantErr: true,
		},
	}

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
