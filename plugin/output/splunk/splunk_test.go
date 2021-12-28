package splunk

import (
	"github.com/ozonru/file.d/pipeline"
	insaneJSON "github.com/vitkovskii/insane-json"
	"go.uber.org/zap"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestEventKey(t *testing.T) {
	testsCases := map[string]string{
		`{"msg":"AAAA","some_field":"BBBB"}`:         `{"event":{"msg":"AAAA"},"some_field":"BBBB"}`,
		`{"msg":"AAAA","some_field":{"boo": "bar"}}`: `{"event":{"msg":"AAAA"},"some_field":{"boo":"bar"}}`,
	}

	for k, v := range testsCases {
		if resp := testOne(k, t); resp != v {
			println(k)
			println(v)
			t.Fatal(resp)
		}
	}
}

func testOne(in string, t *testing.T) string {
	input, err := insaneJSON.DecodeBytes([]byte(in))
	if err != nil {
		t.Fatal(err)
	}

	var response []byte
	testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		var err error
		response, err = ioutil.ReadAll(req.Body)
		if err != nil {
			t.Fatal(err)
		}
		res.WriteHeader(http.StatusOK)
		res.Write([]byte(`{"code":0}`))
	}))

	defer func() { testServer.Close() }()

	logger := zap.NewExample().Sugar()
	plugin := Plugin{
		config: &Config{
			EventKey: "msg",
			Endpoint: testServer.URL,
		},
		logger: logger,
	}

	batch := pipeline.Batch{
		Events: []*pipeline.Event{&pipeline.Event{
			Root: input,
		}},
	}

	data := pipeline.WorkerData(nil)
	plugin.out(&data, &batch)

	return string(response)
}
