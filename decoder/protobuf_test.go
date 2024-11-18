package decoder

import (
	"encoding/json"
	"testing"

	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const protoContent = `syntax = "proto3";

package test;
option go_package = "test.v1";

message Data {
  string stringData = 1 [json_name="string_data"];
  int32 intData = 2 [json_name="int_data"];
}

message MyMessage {
  message InternalData {
    repeated string myStrings = 1 [json_name="my_strings"];
    bool isValid = 2 [json_name="is_valid"];
  }

  Data data = 1;
  InternalData internalData = 2 [json_name="internal_data"];
  uint64 version = 3;
}
`

func TestProtobuf(t *testing.T) {
	const protoMessage = "MyMessage"

	type (
		testData struct {
			StringData string `json:"string_data"`
			IntData    int    `json:"int_data"`
		}

		testInternalData struct {
			MyStrings []string `json:"my_strings"`
			IsValid   bool     `json:"is_valid"`
		}

		testMyMessage struct {
			Data         testData         `json:"data"`
			InternalData testInternalData `json:"internal_data"`
			Version      string           `json:"version"`
		}
	)

	inputData := []byte{10, 13, 10, 9, 109, 121, 95, 115, 116, 114, 105, 110, 103, 16, 123, 18, 14, 10, 4, 115, 116, 114, 49, 10, 4, 115, 116, 114, 50, 16, 1, 24, 10}
	myMessage := testMyMessage{
		Data: testData{
			StringData: "my_string",
			IntData:    123,
		},
		InternalData: testInternalData{
			MyStrings: []string{"str1", "str2"},
			IsValid:   true,
		},
		Version: "10",
	}

	tests := []struct {
		name string

		data   []byte
		params map[string]any

		want          testMyMessage
		wantCreateErr bool
		wantDecodeErr bool
	}{
		{
			name: "proto_file_path",
			data: inputData,
			params: map[string]any{
				protoFileParam:    "../testdata/proto/valid.proto",
				protoMessageParam: protoMessage,
			},
			want: myMessage,
		},
		{
			name: "proto_file_content",
			data: inputData,
			params: map[string]any{
				protoFileParam:    protoContent,
				protoMessageParam: protoMessage,
			},
			want: myMessage,
		},
		{
			name: "proto_file_with_imports",
			data: inputData,
			params: map[string]any{
				protoFileParam:    "with_imports.proto",
				protoMessageParam: protoMessage,
				protoImportPathsParam: []any{
					"../testdata/proto",
				},
			},
			want: myMessage,
		},
		{
			name: "proto_file_param_not_exists",
			params: map[string]any{
				protoMessageParam: "test",
			},
			wantCreateErr: true,
		},
		{
			name: "proto_file_param_invalid",
			params: map[string]any{
				protoFileParam:    123,
				protoMessageParam: "test",
			},
			wantCreateErr: true,
		},
		{
			name: "proto_message_param_not_exists",
			params: map[string]any{
				protoFileParam: "test",
			},
			wantCreateErr: true,
		},
		{
			name: "proto_message_param_invalid",
			params: map[string]any{
				protoFileParam:    "test",
				protoMessageParam: 123,
			},
			wantCreateErr: true,
		},
		{
			name: "compile_error",
			params: map[string]any{
				protoFileParam:    "../testdata/proto/invalid.proto",
				protoMessageParam: protoMessage,
			},
			wantCreateErr: true,
		},
		{
			name: "message_not_found",
			params: map[string]any{
				protoFileParam:    "../testdata/proto/valid.proto",
				protoMessageParam: "test",
			},
			wantCreateErr: true,
		},
		{
			name: "invalid_data",
			data: []byte{10, 13},
			params: map[string]any{
				protoFileParam:    "../testdata/proto/valid.proto",
				protoMessageParam: protoMessage,
			},
			wantDecodeErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dec, err := NewProtobufDecoder(tt.params)
			require.Equal(t, tt.wantCreateErr, err != nil, err)
			if tt.wantCreateErr {
				return
			}

			root := insaneJSON.Spawn()
			defer insaneJSON.Release(root)

			err = dec.DecodeToJson(root, tt.data)
			require.Equal(t, tt.wantDecodeErr, err != nil, err)
			if tt.wantDecodeErr {
				return
			}

			// for correct comparison
			var gotMsg testMyMessage
			assert.NoError(t, json.Unmarshal(root.EncodeToByte(), &gotMsg))
			require.Equal(t, tt.want, gotMsg)
		})
	}
}
