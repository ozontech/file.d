package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestElasticsearchResponse(t *testing.T) {
	t.Parallel()
	r := require.New(t)

	p := Plugin{}

	tcs := []struct {
		Name         string
		Handler      http.HandlerFunc
		Request      *http.Request
		ExpectedBody []byte
		ExpectedCode int
	}{
		{
			Name:         "info",
			Handler:      p.serveElasticsearchInfo,
			Request:      httptest.NewRequest(http.MethodGet, "/", http.NoBody),
			ExpectedBody: info,
			ExpectedCode: http.StatusOK,
		},
		{
			Name:         "info head",
			Handler:      p.serveElasticsearchInfo,
			Request:      httptest.NewRequest(http.MethodHead, "/", http.NoBody),
			ExpectedBody: nil,
			ExpectedCode: http.StatusOK,
		},
		{
			Name:         "xpack",
			Handler:      p.serveElasticsearchXPack,
			Request:      httptest.NewRequest(http.MethodGet, "/", http.NoBody),
			ExpectedBody: xpack,
			ExpectedCode: http.StatusOK,
		},
		{
			Name:         "license",
			Handler:      p.serveElasticsearchLicense,
			Request:      httptest.NewRequest(http.MethodGet, "/", http.NoBody),
			ExpectedBody: license,
			ExpectedCode: http.StatusOK,
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			rec := httptest.NewRecorder()

			tc.Handler.ServeHTTP(rec, tc.Request)

			r.Equal(http.StatusOK, rec.Code)

			body := rec.Body.Bytes()
			r.Equal(tc.ExpectedBody, body)

			// check body is valid json
			if len(body) > 0 {
				m := map[string]any{}
				r.NoError(json.Unmarshal(rec.Body.Bytes(), &m))
			}
		})
	}
}
