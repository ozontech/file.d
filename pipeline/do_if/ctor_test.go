package do_if

import (
	"bytes"
	"testing"
	"time"

	"github.com/bitly/go-simplejson"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractNode(t *testing.T) {
	tests := []struct {
		name    string
		raw     string
		want    *treeNode
		wantErr bool
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
			want: &treeNode{
				logicalOp: "not",
				operands: []treeNode{
					{
						logicalOp: "and",
						operands: []treeNode{
							{
								stringOp:      "equal",
								fieldName:     "service",
								values:        [][]byte{nil, []byte("")},
								caseSensitive: false,
							},
							{
								stringOp:      "prefix",
								fieldName:     "log.msg",
								values:        [][]byte{[]byte("test-1"), []byte("test-2")},
								caseSensitive: false,
							},
							{
								lenCmpOp:  "byte_len_cmp",
								cmpOp:     "gt",
								fieldName: "msg",
								cmpValue:  100,
							},
							{
								lenCmpOp:  "array_len_cmp",
								cmpOp:     "lt",
								fieldName: "items",
								cmpValue:  100,
							},
							{
								tsCmpOp:            true,
								cmpOp:              "lt",
								fieldName:          "timestamp",
								tsFormat:           time.RFC3339Nano,
								tsCmpValue:         time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC),
								tsCmpValChangeMode: tsCmpModeConstTag,
								tsUpdateInterval:   15 * time.Second,
							},
							{
								logicalOp: "or",
								operands: []treeNode{
									{
										stringOp:      "suffix",
										fieldName:     "service",
										values:        [][]byte{[]byte("test-svc-1"), []byte("test-svc-2")},
										caseSensitive: true,
									},
									{
										stringOp:      "contains",
										fieldName:     "pod",
										values:        [][]byte{[]byte("test")},
										caseSensitive: true,
									},
									{
										stringOp:      "regex",
										fieldName:     "message",
										values:        [][]byte{[]byte(`test-\d+`), []byte(`test-msg-\d+`)},
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
			name: "ok_byte_len_cmp_op",
			raw:  `{"op":"byte_len_cmp","field":"data","cmp_op":"lt","value":10}`,
			want: &treeNode{
				lenCmpOp:  "byte_len_cmp",
				cmpOp:     "lt",
				fieldName: "data",
				cmpValue:  10,
			},
		},
		{
			name: "ok_array_len_cmp_op",
			raw:  `{"op":"array_len_cmp","field":"items","cmp_op":"lt","value":10}`,
			want: &treeNode{
				lenCmpOp:  "array_len_cmp",
				cmpOp:     "lt",
				fieldName: "items",
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
			want: &treeNode{
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
			raw: `{
				"op": "ts_cmp",
				"field": "timestamp",
				"cmp_op": "lt",
				"value": "now"
			}`,
			want: &treeNode{
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
			raw: `{
				"op": "ts_cmp",
				"field": "timestamp",
				"cmp_op": "lt",
				"format": "rfc3339",
				"value": "now"
			}`,
			want: &treeNode{
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
			raw: `{
				"op": "check_type",
				"field": "log",
				"values": ["obj","arr"]
			}`,
			want: &treeNode{
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
			raw: `{
				"op":"or",
				"operands":[
					{"op":"equal","field":"service","values":null},
					{"op":"equal","field":"service","values":""},
					{"op":"equal","field":"service","values":"test"}
				]
			}`,
			want: &treeNode{
				logicalOp: "or",
				operands: []treeNode{
					{
						stringOp:      "equal",
						fieldName:     "service",
						values:        [][]byte{nil},
						caseSensitive: true,
					},
					{
						stringOp:      "equal",
						fieldName:     "service",
						values:        [][]byte{[]byte("")},
						caseSensitive: true,
					},
					{
						stringOp:      "equal",
						fieldName:     "service",
						values:        [][]byte{[]byte("test")},
						caseSensitive: true,
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

			want, err := buildTree(tt.want)
			require.NoError(t, err)
			assert.NoError(t, got.isEqualTo(want, 1))
		})
	}
}
