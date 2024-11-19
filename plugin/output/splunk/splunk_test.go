package splunk

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/pipeline"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestSplunk(t *testing.T) {
	suites := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "basic",
			input:    `{"msg":"AAAA","some_field":"BBBB"}`,
			expected: `{"event":{"msg":"AAAA","some_field":"BBBB"}}`,
		},
	}

	for _, tt := range suites {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			input, err := insaneJSON.DecodeBytes([]byte(tt.input))
			if err != nil {
				t.Fatal(err)
			}
			defer insaneJSON.Release(input)

			var response []byte
			testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
				response, err = io.ReadAll(req.Body)
				if err != nil {
					t.Fatal(err)
				}
				res.WriteHeader(http.StatusOK)
				_, _ = res.Write([]byte(`{"code":0}`))
			}))
			defer testServer.Close()

			plugin := Plugin{
				config: &Config{
					Endpoint: testServer.URL,
				},
				logger: zap.NewExample().Sugar(),
			}
			plugin.prepareClient()

			batch := pipeline.NewPreparedBatch([]*pipeline.Event{
				{Root: input},
				{Root: input},
			})

			data := pipeline.WorkerData(nil)
			_ = plugin.out(&data, batch)

			assert.Equal(t, tt.expected+tt.expected, string(response))
		})
	}
}

func TestParseSplunkError(t *testing.T) {
	cases := []struct {
		name    string
		data    string
		wantErr bool
	}{
		{
			name: "ok",
			data: `{"code":0}`,
		},
		{
			name:    "err_parse_json",
			data:    "invalid json",
			wantErr: true,
		},
		{
			name:    "err_no_code",
			data:    `{"not_code":10}`,
			wantErr: true,
		},
		{
			name:    "err_bad_code",
			data:    `{"code":5}`,
			wantErr: true,
		},
	}
	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := parseSplunkError([]byte(tt.data))
			assert.Equal(t, tt.wantErr, err != nil)
		})
	}
}

func TestCopyFields(t *testing.T) {
	suites := []struct {
		name       string
		copyFields []copyFieldPaths
		input      string
		expected   string
	}{
		{
			`no_copy_fields`,
			nil,
			`{"msg":"AAAA","some_field":"BBBB"}`,
			`{"event":{"msg":"AAAA","some_field":"BBBB"}}`,
		},
		{
			`copy_absent_field`,
			[]copyFieldPaths{{cfg.ParseFieldSelector("ts"), cfg.ParseFieldSelector("time")}},
			`{"msg":"AAAA","some_field":"BBBB"}`,
			`{"event":{"msg":"AAAA","some_field":"BBBB"}}`,
		},
		{
			`copy_single_non_nested_field`,
			[]copyFieldPaths{{cfg.ParseFieldSelector("ts"), cfg.ParseFieldSelector("time")}},
			`{"msg":"AAAA","some_field":"BBBB","ts":"1723651045"}`,
			`{"event":{"msg":"AAAA","some_field":"BBBB","ts":"1723651045"},"time":"1723651045"}`,
		},
		{
			`copy_single_field_to_nested_field`,
			[]copyFieldPaths{{cfg.ParseFieldSelector("service"), cfg.ParseFieldSelector("fields.service_name")}},
			`{"msg":"AAAA","some_field":"BBBB","service":"test-svc"}`,
			`{"event":{"msg":"AAAA","some_field":"BBBB","service":"test-svc"},"fields":{"service_name":"test-svc"}}`,
		},
		{
			`copy_two_fields`,
			[]copyFieldPaths{
				{cfg.ParseFieldSelector("ts"), cfg.ParseFieldSelector("time")},
				{cfg.ParseFieldSelector("service"), cfg.ParseFieldSelector("fields.service_name")},
			},
			`{"msg":"AAAA","some_field":"BBBB","ts":"1723651045","service":"test-svc"}`,
			`{"event":{"msg":"AAAA","some_field":"BBBB","ts":"1723651045","service":"test-svc"},"time":"1723651045","fields":{"service_name":"test-svc"}}`,
		},
		{
			`copy_root`,
			[]copyFieldPaths{
				{cfg.ParseFieldSelector(""), cfg.ParseFieldSelector("copy")},
			},
			`{"msg":"AAAA","some_field":"BBBB"}`,
			`{"event":{"msg":"AAAA","some_field":"BBBB"},"copy":{"msg":"AAAA","some_field":"BBBB"}}`,
		},
	}

	for _, tt := range suites {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			input, err := insaneJSON.DecodeBytes([]byte(tt.input))
			if err != nil {
				t.Fatal(err)
			}
			defer insaneJSON.Release(input)

			var response []byte
			testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
				response, err = io.ReadAll(req.Body)
				if err != nil {
					t.Fatal(err)
				}
				res.WriteHeader(http.StatusOK)
				_, _ = res.Write([]byte(`{"code":0}`))
			}))
			defer testServer.Close()

			plugin := Plugin{
				config: &Config{
					Endpoint: testServer.URL,
				},
				copyFieldsPaths: tt.copyFields,
				logger:          zap.NewExample().Sugar(),
			}
			plugin.prepareClient()

			batch := pipeline.NewPreparedBatch([]*pipeline.Event{
				{Root: input},
				{Root: input},
			})

			data := pipeline.WorkerData(nil)
			_ = plugin.out(&data, batch)

			assert.Equal(t, tt.expected+tt.expected, string(response))
		})
	}
}
