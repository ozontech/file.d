package elasticsearch

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
	insaneJSON "github.com/vitkovskii/insane-json"

	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/test"
)

func TestAppendEvent(t *testing.T) {
	p := &Plugin{}
	config := &Config{
		Endpoints:   []string{"test"},
		IndexFormat: "test-%-index-%-%",
		IndexValues: []string{"@time", "field_a", "field_b"},
		BatchSize:   "1",
	}
	test.NewConfig(config, map[string]int{"gomaxprocs": 1})

	p.Start(config, test.NewEmptyOutputPluginParams())

	p.time = "6666-66-66"
	root, _ := insaneJSON.DecodeBytes([]byte(`{"field_a":"AAAA","field_b":"BBBB"}`))
	defer insaneJSON.Release(root)

	result := p.appendEvent(nil, &pipeline.Event{Root: root})

	expected := fmt.Sprintf("%s\n%s\n", `{"index":{"_index":"test-6666-66-66-index-AAAA-BBBB"}}`, `{"field_a":"AAAA","field_b":"BBBB"}`)
	assert.Equal(t, expected, string(result), "wrong request content")
}

func TestAppendEventWithIndexOpType(t *testing.T) {
	p := &Plugin{}
	config := &Config{
		Endpoints:   []string{"test"},
		IndexFormat: "test-%-index-%-%",
		IndexValues: []string{"@time", "field_a", "field_b"},
		BatchSize:   "1",
		BatchOpType: "index",
	}
	test.NewConfig(config, map[string]int{"gomaxprocs": 1})

	p.Start(config, test.NewEmptyOutputPluginParams())

	p.time = "6666-66-66"
	root, _ := insaneJSON.DecodeBytes([]byte(`{"field_a":"AAAA","field_b":"BBBB"}`))
	defer insaneJSON.Release(root)

	result := p.appendEvent(nil, &pipeline.Event{Root: root})

	expected := fmt.Sprintf("%s\n%s\n", `{"index":{"_index":"test-6666-66-66-index-AAAA-BBBB"}}`, `{"field_a":"AAAA","field_b":"BBBB"}`)
	assert.Equal(t, expected, string(result), "wrong request content")
}

func TestAppendEventWithCreateOpType(t *testing.T) {
	p := &Plugin{}
	config := &Config{
		Endpoints:   []string{"test"},
		IndexFormat: "test-%-index-%-%",
		IndexValues: []string{"@time", "field_a", "field_b"},
		BatchSize:   "1",
		BatchOpType: "create",
	}
	test.NewConfig(config, map[string]int{"gomaxprocs": 1})

	p.Start(config, test.NewEmptyOutputPluginParams())

	p.time = "6666-66-66"
	root, _ := insaneJSON.DecodeBytes([]byte(`{"field_a":"AAAA","field_b":"BBBB"}`))
	defer insaneJSON.Release(root)

	result := p.appendEvent(nil, &pipeline.Event{Root: root})

	expected := fmt.Sprintf("%s\n%s\n", `{"create":{"_index":"test-6666-66-66-index-AAAA-BBBB"}}`, `{"field_a":"AAAA","field_b":"BBBB"}`)
	assert.Equal(t, expected, string(result), "wrong request content")
}

func TestConfig(t *testing.T) {
	p := &Plugin{}
	config := &Config{
		IndexFormat: "test-%",
		Endpoints: []string{
			"http://endpoint_1:9000",
			"http://endpoint_2:9000/",
			"https://endpoint_3:9000",
			"https://endpoint_4:9000/",
		},
		BatchSize: "1",
	}
	test.NewConfig(config, map[string]int{"gomaxprocs": 1})

	p.Start(config, test.NewEmptyOutputPluginParams())

	results := []string{
		"http://endpoint_1:9000/_bulk?_source=false",
		"http://endpoint_2:9000/_bulk?_source=false",
		"https://endpoint_3:9000/_bulk?_source=false",
		"https://endpoint_4:9000/_bulk?_source=false",
	}

	require.Len(t, p.endpoints, len(results))
	for i := range results {
		assert.Equal(t, results[i], p.endpoints[i].String())
	}
}

func TestPrepareRequest(t *testing.T) {
	type wantData struct {
		uri             string
		method          []byte
		contentType     []byte
		contentEncoding []byte
		auth            []byte
		body            []byte
	}

	cases := []struct {
		name   string
		config *Config

		body    string
		want    wantData
		wantErr bool
	}{
		{
			name: "raw",
			config: &Config{
				Endpoints: []string{"http://endpoint:9000"},
				APIKey:    "test",
			},
			body: "test",
			want: wantData{
				uri:         "http://endpoint:9000/",
				method:      []byte(fasthttp.MethodPost),
				contentType: []byte(NDJSONContentType),
				auth:        []byte("ApiKey test"),
				body:        []byte("test"),
			},
		},
		{
			name: "gzip",
			config: &Config{
				Endpoints: []string{"http://endpoint:9000"},
				UseGzip:   true,
			},
			body: "test",
			want: wantData{
				uri:             "http://endpoint:9000/",
				method:          []byte(fasthttp.MethodPost),
				contentType:     []byte(NDJSONContentType),
				contentEncoding: []byte(gzipContentEncoding),
				body:            []byte("test"),
			},
		},
	}
	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := Plugin{
				config: tt.config,
			}
			p.authHeader = p.getAuthHeader()

			endpoint := &fasthttp.URI{}
			if err := endpoint.Parse(nil, []byte(tt.config.Endpoints[0])); err != nil {
				t.Fatal(err)
			}

			req := fasthttp.AcquireRequest()
			defer fasthttp.ReleaseRequest(req)

			err := p.prepareRequest(req, endpoint, []byte(tt.body))
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.Equal(t, tt.want.uri, req.URI().String(), "wrong uri")
			require.Equal(t, tt.want.method, req.Header.Method(), "wrong method")
			require.Equal(t, tt.want.contentType, req.Header.ContentType(), "wrong content type")
			require.Equal(t, tt.want.contentEncoding, req.Header.ContentEncoding(), "wrong content encoding")
			require.Equal(t, tt.want.auth, req.Header.PeekBytes(strAuthorization), "wrong auth")

			var body []byte
			if tt.config.UseGzip {
				body, _ = req.BodyUncompressed()
			} else {
				body = req.Body()
			}
			require.Equal(t, tt.want.body, body, "wrong body")
		})
	}
}
