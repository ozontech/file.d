package decode

import (
	"sync"
	"testing"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	"github.com/stretchr/testify/assert"
)

const protoFileContent = `syntax = "proto3";

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

func TestDecode(t *testing.T) {
	tests := []struct {
		name string

		config *Config
		input  []byte
		want   map[string]string
	}{
		{
			name: "json",
			config: &Config{
				Field: "log",
			},
			input: []byte(`{"level":"error","log":"{\"field1\":\"value1\",\"field2\":\"value2\"}"}`),
			want: map[string]string{
				"level":  "error",
				"field1": "value1",
				"field2": "value2",
			},
		},
		{
			name: "json_prefix",
			config: &Config{
				Field:   "log",
				Decoder: "json",
				Prefix:  "p_",
			},
			input: []byte(`{"level":"error","log":"{\"field1\":\"value1\",\"field2\":\"value2\"}"}`),
			want: map[string]string{
				"level":    "error",
				"p_field1": "value1",
				"p_field2": "value2",
			},
		},
		{
			name: "json_max_fields_size",
			config: &Config{
				Field:   "log",
				Decoder: "json",
				Params: map[string]any{
					"json_max_fields_size": map[string]any{
						"field1": 5,
						"field2": 10,
						"field3": 1,
					},
				},
			},
			input: []byte(`{"level":"error","log":"{\"field1\":\"value long long long\",\"field2\":\"value2\",\"field3\":true}"}`),
			want: map[string]string{
				"level":  "error",
				"field1": "value",
				"field2": "value2",
				"field3": "true",
			},
		},
		{
			name: "json_not_obj",
			config: &Config{
				Field: "log",
			},
			input: []byte(`{"level":"error","log":"not obj"}`),
			want: map[string]string{
				"level": "error",
				"log":   "not obj",
			},
		},
		{
			name: "postgres",
			config: &Config{
				Field:   "log",
				Decoder: "postgres",
			},
			input: []byte(`{"level":"error","log":"2021-06-22 16:24:27 GMT [7291] => [3-1] client=test_client,db=test_db,user=test_user LOG:  listening on Unix socket \"/var/run/postgresql/.s.PGSQL.5432\""}`),
			want: map[string]string{
				"level":              "error",
				"time":               "2021-06-22 16:24:27 GMT",
				"pid":                "7291",
				"pid_message_number": "3-1",
				"client":             "test_client",
				"db":                 "test_db",
				"user":               "test_user",
				"log":                "listening on Unix socket \"/var/run/postgresql/.s.PGSQL.5432\"",
			},
		},
		{
			name: "postgres_prefix",
			config: &Config{
				Field:   "log",
				Decoder: "postgres",
				Prefix:  "p_",
			},
			input: []byte(`{"level":"error","log":"2021-06-22 16:24:27 GMT [7291] => [3-1] client=test_client,db=test_db,user=test_user LOG:  listening on Unix socket \"/var/run/postgresql/.s.PGSQL.5432\""}`),
			want: map[string]string{
				"level":                "error",
				"p_time":               "2021-06-22 16:24:27 GMT",
				"p_pid":                "7291",
				"p_pid_message_number": "3-1",
				"p_client":             "test_client",
				"p_db":                 "test_db",
				"p_user":               "test_user",
				"p_log":                "listening on Unix socket \"/var/run/postgresql/.s.PGSQL.5432\"",
			},
		},
		{
			name: "nginx_error",
			config: &Config{
				Field:   "log",
				Decoder: "nginx_error",
			},
			input: []byte(`{"level":"warn","log":"2022/08/17 10:49:27 [error] 2725122#2725122: *792412315 lua udp socket read timed out, context: ngx.timer\n"}`),
			want: map[string]string{
				"level":   "error",
				"time":    "2022/08/17 10:49:27",
				"pid":     "2725122",
				"tid":     "2725122",
				"cid":     "792412315",
				"message": "lua udp socket read timed out, context: ngx.timer",
			},
		},
		{
			name: "nginx_error_prefix",
			config: &Config{
				Field:   "log",
				Decoder: "nginx_error",
				Prefix:  "p_",
			},
			input: []byte(`{"level":"warn","log":"2022/08/17 10:49:27 [error] 2725122#2725122: *792412315 lua udp socket read timed out, context: ngx.timer"}`),
			want: map[string]string{
				"level":     "warn",
				"p_level":   "error",
				"p_time":    "2022/08/17 10:49:27",
				"p_pid":     "2725122",
				"p_tid":     "2725122",
				"p_cid":     "792412315",
				"p_message": "lua udp socket read timed out, context: ngx.timer",
			},
		},
		{
			name: "nginx_error_with_custom_fields",
			config: &Config{
				Field:   "log",
				Decoder: "nginx_error",
				Params: map[string]any{
					"nginx_with_custom_fields": true,
				},
			},
			input: []byte(`{"level":"warn","log":"2022/08/18 09:29:37 [error] 844935#844935: *44934601 upstream timed out (110: Operation timed out), while connecting to upstream, client: 10.125.172.251, server: , request: \"POST /download HTTP/1.1\", upstream: \"http://10.117.246.15:84/download\", host: \"mpm-youtube-downloader-38.name.tldn:84\"\n"}`),
			want: map[string]string{
				"level":    "error",
				"time":     "2022/08/18 09:29:37",
				"pid":      "844935",
				"tid":      "844935",
				"cid":      "44934601",
				"message":  "upstream timed out (110: Operation timed out), while connecting to upstream",
				"client":   "10.125.172.251",
				"server":   "",
				"request":  "POST /download HTTP/1.1",
				"upstream": "http://10.117.246.15:84/download",
				"host":     "mpm-youtube-downloader-38.name.tldn:84",
			},
		},
		{
			name: "protobuf",
			config: &Config{
				Field:   "log",
				Decoder: "protobuf",
				Params: map[string]any{
					"proto_message": "MyMessage",
					"proto_file":    protoFileContent,
				},
			},
			input: []byte{123, 34, 108, 101, 118, 101, 108, 34, 58, 34, 101, 114, 114, 111, 114, 34, 44, 34, 108, 111, 103, 34, 58, 34, 10, 13, 10, 9, 109, 121, 95, 115, 116, 114, 105, 110, 103, 16, 123, 18, 14, 10, 4, 115, 116, 114, 49, 10, 4, 115, 116, 114, 50, 16, 1, 24, 10, 34, 125},
			want: map[string]string{
				"level":                    "error",
				"data.string_data":         "my_string",
				"data.int_data":            "123",
				"internal_data.my_strings": `["str1","str2"]`,
				"internal_data.is_valid":   "true",
				"version":                  "10",
			},
		},
		{
			name: "protobuf_prefix",
			config: &Config{
				Field:   "log",
				Decoder: "protobuf",
				Params: map[string]any{
					"proto_message": "MyMessage",
					"proto_file":    protoFileContent,
				},
				Prefix: "p_",
			},
			input: []byte{123, 34, 108, 101, 118, 101, 108, 34, 58, 34, 101, 114, 114, 111, 114, 34, 44, 34, 108, 111, 103, 34, 58, 34, 10, 13, 10, 9, 109, 121, 95, 115, 116, 114, 105, 110, 103, 16, 123, 18, 14, 10, 4, 115, 116, 114, 49, 10, 4, 115, 116, 114, 50, 16, 1, 24, 10, 34, 125},
			want: map[string]string{
				"level":                      "error",
				"p_data.string_data":         "my_string",
				"p_data.int_data":            "123",
				"p_internal_data.my_strings": `["str1","str2"]`,
				"p_internal_data.is_valid":   "true",
				"p_version":                  "10",
			},
		},
		{
			name: "syslog_rfc3164",
			config: &Config{
				Field:   "log",
				Decoder: "syslog_rfc3164",
			},
			input: []byte(`{"service":"test","log":"<34>Oct  5 22:14:15 mymachine.example.com myproc[10]: 'myproc' failed on /dev/pts/8"}`),
			want: map[string]string{
				"service":    "test",
				"priority":   "34",
				"facility":   "4",
				"severity":   "2",
				"timestamp":  "Oct  5 22:14:15",
				"hostname":   "mymachine.example.com",
				"app_name":   "myproc",
				"process_id": "10",
				"message":    "'myproc' failed on /dev/pts/8",
			},
		},
		{
			name: "syslog_rfc3164_prefix",
			config: &Config{
				Field:   "log",
				Decoder: "syslog_rfc3164",
				Prefix:  "p_",
			},
			input: []byte(`{"service":"test","log":"<34>Oct  5 22:14:15 mymachine.example.com myproc[10]: 'myproc' failed on /dev/pts/8"}`),
			want: map[string]string{
				"service":      "test",
				"p_priority":   "34",
				"p_facility":   "4",
				"p_severity":   "2",
				"p_timestamp":  "Oct  5 22:14:15",
				"p_hostname":   "mymachine.example.com",
				"p_app_name":   "myproc",
				"p_process_id": "10",
				"p_message":    "'myproc' failed on /dev/pts/8",
			},
		},
		{
			name: "syslog_rfc3164_priority_format",
			config: &Config{
				Field:   "log",
				Decoder: "syslog_rfc3164",
				Params: map[string]any{
					"syslog_facility_format": "string",
					"syslog_severity_format": "string",
				},
			},
			input: []byte(`{"service":"test","log":"<34>Oct  5 22:14:15 mymachine.example.com myproc[10]: 'myproc' failed on /dev/pts/8"}`),
			want: map[string]string{
				"service":    "test",
				"priority":   "34",
				"facility":   "AUTH",
				"severity":   "CRIT",
				"timestamp":  "Oct  5 22:14:15",
				"hostname":   "mymachine.example.com",
				"app_name":   "myproc",
				"process_id": "10",
				"message":    "'myproc' failed on /dev/pts/8",
			},
		},
		{
			name: "syslog_rfc5424",
			config: &Config{
				Field:   "log",
				Decoder: "syslog_rfc5424",
			},
			input: []byte(`{"service":"test","log":"<165>1 2003-10-11T22:14:15.003Z mymachine.example.com myproc 10 ID47 [exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"] An application event log"}`),
			want: map[string]string{
				"service":                       "test",
				"priority":                      "165",
				"facility":                      "20",
				"severity":                      "5",
				"proto_version":                 "1",
				"timestamp":                     "2003-10-11T22:14:15.003Z",
				"hostname":                      "mymachine.example.com",
				"app_name":                      "myproc",
				"process_id":                    "10",
				"message_id":                    "ID47",
				"message":                       "An application event log",
				"exampleSDID@32473.iut":         "3",
				"exampleSDID@32473.eventSource": "Application",
				"exampleSDID@32473.eventID":     "1011",
			},
		},
		{
			name: "syslog_rfc5424_prefix",
			config: &Config{
				Field:   "log",
				Decoder: "syslog_rfc5424",
				Prefix:  "p_",
			},
			input: []byte(`{"service":"test","log":"<165>1 2003-10-11T22:14:15.003Z mymachine.example.com myproc 10 ID47 [exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"] An application event log"}`),
			want: map[string]string{
				"service":                       "test",
				"p_priority":                    "165",
				"p_facility":                    "20",
				"p_severity":                    "5",
				"p_proto_version":               "1",
				"p_timestamp":                   "2003-10-11T22:14:15.003Z",
				"p_hostname":                    "mymachine.example.com",
				"p_app_name":                    "myproc",
				"p_process_id":                  "10",
				"p_message_id":                  "ID47",
				"p_message":                     "An application event log",
				"exampleSDID@32473.iut":         "3",
				"exampleSDID@32473.eventSource": "Application",
				"exampleSDID@32473.eventID":     "1011",
			},
		},
		{
			name: "syslog_rfc5424_priority_format",
			config: &Config{
				Field:   "log",
				Decoder: "syslog_rfc5424",
				Params: map[string]any{
					"syslog_facility_format": "string",
					"syslog_severity_format": "string",
				},
			},
			input: []byte(`{"service":"test","log":"<165>1 2003-10-11T22:14:15.003Z mymachine.example.com myproc - ID47 [exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"]"}`),
			want: map[string]string{
				"service":                       "test",
				"priority":                      "165",
				"facility":                      "LOCAL4",
				"severity":                      "NOTICE",
				"proto_version":                 "1",
				"timestamp":                     "2003-10-11T22:14:15.003Z",
				"hostname":                      "mymachine.example.com",
				"app_name":                      "myproc",
				"message_id":                    "ID47",
				"exampleSDID@32473.iut":         "3",
				"exampleSDID@32473.eventSource": "Application",
				"exampleSDID@32473.eventID":     "1011",
			},
		},
		{
			name: "keep_origin",
			config: &Config{
				Field:      "log",
				Decoder:    "json",
				KeepOrigin: true,
			},
			input: []byte(`{"level":"error","log":"{\"field1\":\"value1\",\"field2\":\"value2\"}"}`),
			want: map[string]string{
				"level":  "error",
				"log":    "{\"field1\":\"value1\",\"field2\":\"value2\"}",
				"field1": "value1",
				"field2": "value2",
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			config := test.NewConfig(tt.config, nil)
			p, input, output := test.NewPipelineMock(test.NewActionPluginStaticInfo(factory, config, pipeline.MatchModeAnd, nil, false))

			wg := &sync.WaitGroup{}
			wg.Add(1)

			output.SetOutFn(func(e *pipeline.Event) {
				for k, v := range tt.want {
					node := e.Root.Dig(cfg.ParseFieldSelector(k)...)
					val := ""
					if v != "" && v[0] == '[' {
						val = node.EncodeToString()
					} else {
						val = node.AsString()
					}
					assert.Equal(t, v, val, "wrong out event value with key %q", k)
				}
				wg.Done()
			})

			input.In(0, "test.log", test.NewOffset(0), tt.input)

			wg.Wait()
			p.Stop()
		})
	}
}
