package splunk

import (
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/ozontech/file.d/pipeline"
	"github.com/stretchr/testify/assert"
	insaneJSON "github.com/vitkovskii/insane-json"
	"go.uber.org/zap"
)

func TestSplunk(t *testing.T) {
	suites := []struct {
		name     string
		input    string
		expected string
	}{
		{
			`basic case`,
			`{"msg":"AAAA","some_field":"BBBB"}`,
			`{"event":{"msg":"AAAA","some_field":"BBBB"}}`,
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

			ctx := context.TODO()
			stdBackoff := backoff.NewExponentialBackOff()
			stdBackoff.Multiplier = 1.2
			stdBackoff.RandomizationFactor = 0.25
			stdBackoff.InitialInterval = time.Second
			stdBackoff.MaxInterval = stdBackoff.InitialInterval * 2

			plugin := Plugin{
				config: &Config{
					Endpoint: testServer.URL,
				},
				logger:  zap.NewExample().Sugar(),
				backoff: backoff.WithContext(stdBackoff, ctx),
			}

			batch := pipeline.Batch{
				Events: []*pipeline.Event{
					{
						Root: input,
					},
					{
						Root: input,
					}},
			}

			data := pipeline.WorkerData(nil)
			plugin.out(&data, &batch)

			assert.Equal(t, testCase.expected+testCase.expected, string(response))
		})
	}
}
