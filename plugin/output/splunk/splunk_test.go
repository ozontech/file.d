package splunk

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ozontech/file.d/pipeline"
	"github.com/stretchr/testify/assert"
	insaneJSON "github.com/vitkovskii/insane-json"
	"go.uber.org/zap"
)

func TestEventKey(t *testing.T) {
	suites := []struct {
		name     string
		input    string
		expected string
	}{
		{
			`basic case`,
			`{"msg":"AAAA","some_field":"BBBB"}`,
			`{"event":{"msg":"AAAA"},"some_field":"BBBB"}`,
		},
		{
			`composite field`,
			`{"msg":"AAAA","some_field":{"boo": "bar"}}`,
			`{"event":{"msg":"AAAA"},"some_field":{"boo":"bar"}}`,
		},
	}

	for _, testCase := range suites {
		t.Run(testCase.name, func(t *testing.T) {
			input, err := insaneJSON.DecodeBytes([]byte(testCase.input))
			if err != nil {
				t.Fatal(err)
			}
			defer insaneJSON.Release(input)

			var response []byte
			testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
				response, err = ioutil.ReadAll(req.Body)
				if err != nil {
					t.Fatal(err)
				}
				res.WriteHeader(http.StatusOK)
				_, _ = res.Write([]byte(`{"code":0}`))
			}))
			defer testServer.Close()

			plugin := Plugin{
				config: &Config{
					EventKey: "msg",
					Endpoint: testServer.URL,
				},
				logger: zap.NewExample().Sugar(),
			}

			batch := pipeline.Batch{
				Events: []*pipeline.Event{{
					Root: input,
				}},
			}

			data := pipeline.WorkerData(nil)
			plugin.out(&data, &batch)

			assert.Equal(t, testCase.expected, string(response))
		})
	}
}
