package xhttp

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

func TestPrepareRequest(t *testing.T) {
	type inputData struct {
		endpoint             string
		method               string
		contentType          string
		body                 string
		authHeader           string
		gzipCompressionLevel int
	}

	type wantData struct {
		uri             string
		method          []byte
		contentType     []byte
		contentEncoding []byte
		body            []byte
		auth            []byte
	}

	cases := []struct {
		name string
		in   inputData
		want wantData
	}{
		{
			name: "simple",
			in: inputData{
				endpoint:             "http://endpoint:1",
				method:               fasthttp.MethodPost,
				contentType:          "application/json",
				body:                 "test simple",
				gzipCompressionLevel: -1,
			},
			want: wantData{
				uri:         "http://endpoint:1/",
				method:      []byte(fasthttp.MethodPost),
				contentType: []byte("application/json"),
				body:        []byte("test simple"),
			},
		},
		{
			name: "auth",
			in: inputData{
				endpoint:             "http://endpoint:3",
				method:               fasthttp.MethodPost,
				contentType:          "application/json",
				body:                 "test auth",
				authHeader:           "Auth Header",
				gzipCompressionLevel: -1,
			},
			want: wantData{
				uri:         "http://endpoint:3/",
				method:      []byte(fasthttp.MethodPost),
				contentType: []byte("application/json"),
				body:        []byte("test auth"),
				auth:        []byte("Auth Header"),
			},
		},
		{
			name: "gzip",
			in: inputData{
				endpoint:             "http://endpoint:4",
				method:               fasthttp.MethodPost,
				contentType:          "application/json",
				body:                 "test gzip",
				gzipCompressionLevel: 1,
			},
			want: wantData{
				uri:             "http://endpoint:4/",
				method:          []byte(fasthttp.MethodPost),
				contentType:     []byte("application/json"),
				contentEncoding: []byte(gzipContentEncoding),
				body:            []byte("test gzip"),
			},
		},
	}
	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := &Client{
				authHeader:           tt.in.authHeader,
				gzipCompressionLevel: tt.in.gzipCompressionLevel,
			}

			req := fasthttp.AcquireRequest()
			defer fasthttp.ReleaseRequest(req)

			endpoints, _ := parseEndpoints([]string{tt.in.endpoint})

			c.prepareRequest(req, endpoints[0], tt.in.method, tt.in.contentType, []byte(tt.in.body))

			require.Equal(t, tt.want.uri, req.URI().String(), "wrong uri")
			require.Equal(t, tt.want.method, req.Header.Method(), "wrong method")
			require.Equal(t, tt.want.contentType, req.Header.ContentType(), "wrong content type")
			require.Equal(t, tt.want.contentEncoding, req.Header.ContentEncoding(), "wrong content encoding")
			require.Equal(t, tt.want.auth, req.Header.Peek(fasthttp.HeaderAuthorization), "wrong auth")

			var body []byte
			if tt.in.gzipCompressionLevel != -1 {
				body, _ = req.BodyUncompressed()
			} else {
				body = req.Body()
			}
			require.Equal(t, tt.want.body, body, "wrong body")
		})
	}
}
