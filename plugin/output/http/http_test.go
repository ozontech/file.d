package http

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ozontech/file.d/encoder"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
	insaneJSON "github.com/ozontech/insane-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestEncoding(t *testing.T) {
	p := &Plugin{}
	config := &Config{
		Endpoints: []string{"test"},
		BatchSize: "1",
	}
	test.NewConfig(config, map[string]int{"gomaxprocs": 1})

	p.Start(config, test.NewEmptyOutputPluginParams())

	root, _ := insaneJSON.DecodeBytes([]byte(`{"message":"[INFO] some event","field_a":"AAAA","field_b":"BBBB"}`))
	defer insaneJSON.Release(root)

	data := data{}
	event := &pipeline.Event{Root: root}

	jsonEncoder, _ := encoder.NewEncoder(encoder.EncodingConfig{
		Type: encoder.EncoderTypeJSON,
	})
	data.outBuf, _ = jsonEncoder.Encode(event, data.outBuf)
	data.outBuf = append(data.outBuf, '\n')

	expected := fmt.Sprintf("%s\n", `{"message":"[INFO] some event","field_a":"AAAA","field_b":"BBBB"}`)
	assert.Equal(t, expected, string(data.outBuf), "wrong request content")

	rawEncoder, _ := encoder.NewEncoder(encoder.EncodingConfig{
		Type: encoder.EncoderTypeRaw,
	})
	data.outBuf = data.outBuf[:0]
	data.outBuf, _ = rawEncoder.Encode(event, data.outBuf)
	data.outBuf = append(data.outBuf, '\n')

	expected = fmt.Sprintf("%s\n", `[INFO] some event`)
	assert.Equal(t, expected, string(data.outBuf), "wrong request content")

	root2, _ := insaneJSON.DecodeBytes([]byte(`{"message":"{\"log\":\"[INFO] some event\"}","field_a":"AAAA","field_b":"BBBB"}`))
	defer insaneJSON.Release(root2)
	event.Root = root2

	data.outBuf = data.outBuf[:0]
	data.outBuf, _ = rawEncoder.Encode(event, data.outBuf)
	data.outBuf = append(data.outBuf, '\n')

	expected = fmt.Sprintf("%s\n", `{"log":"[INFO] some event"}`)
	assert.Equal(t, expected, string(data.outBuf), "wrong request content")
}

func TestHeaders(t *testing.T) {
	var gotHeaders http.Header
	testServer := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		gotHeaders = req.Header.Clone()
		res.WriteHeader(http.StatusOK)
	}))
	defer testServer.Close()

	p := &Plugin{
		config: &Config{
			Endpoints:          []string{testServer.URL},
			Headers:            map[string]string{"X-Header-1": "value1", "X-Header-2": "value2"},
			BatchSize_:         1,
			ConnectionTimeout_: 10 * time.Second,
		},
		logger:       zap.NewExample(),
		avgEventSize: 1024,
	}
	var err error
	p.encoder, err = encoder.NewEncoder(encoder.EncodingConfig{Type: encoder.EncoderTypeJSON})
	require.NoError(t, err)
	p.prepareClient(context.Background())

	root, err := insaneJSON.DecodeBytes([]byte(`{"message":"test"}`))
	require.NoError(t, err)
	defer insaneJSON.Release(root)

	batch := pipeline.NewPreparedBatch([]*pipeline.Event{{Root: root}})
	workerData := pipeline.WorkerData(nil)
	require.NoError(t, p.out(&workerData, batch))

	assert.Equal(t, "value1", gotHeaders.Get("X-Header-1"))
	assert.Equal(t, "value2", gotHeaders.Get("X-Header-2"))
}
