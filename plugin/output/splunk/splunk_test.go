package splunk

import (
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ozontech/file.d/consts"
	"github.com/ozontech/file.d/expbackoff"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/stats"
	"github.com/stretchr/testify/assert"
	insaneJSON "github.com/vitkovskii/insane-json"
	"go.uber.org/zap"
)

func TestSplunk(t *testing.T) {
	stats.InitStats()
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
			plugin := Plugin{
				ctx: ctx,
				config: &Config{
					Endpoint: testServer.URL,
				},
				logger: zap.NewExample().Sugar(),
				backoff: expbackoff.New(
					ctx,
					stats.GetCounter(subsystemName, sendErrorCounter),
					time.Minute,
					expbackoff.RetriesCfg{Limited: false},
					expbackoff.Multiplier(consts.ExpBackoffDefaultMultiplier),
					expbackoff.RandomizationFactor(consts.ExpBackoffDefaultRndFactor),
					expbackoff.InitialIntervalOpt(time.Second),
					expbackoff.MaxInterval(time.Second*2),
				),
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
