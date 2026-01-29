package xoauth

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

func TestParseError(t *testing.T) {
	cases := []struct {
		name string
		in   []error
		want *errorAuth
	}{
		{
			name: "retrieve_err_json",
			in: []error{
				&oauth2.RetrieveError{
					Response: &http.Response{
						Header: http.Header{
							"Content-Type": []string{"application/json"},
						},
					},
					Body: []byte(`{"error":"invalid_client","error_description":"some err description"}`),
				},
			},
			want: &errorAuth{
				code: ecInvalidClient,
			},
		},
		{
			name: "retrieve_err_json_unmarshal_err",
			in: []error{
				&oauth2.RetrieveError{
					Response: &http.Response{
						Header: http.Header{
							"Content-Type": []string{"application/json"},
						},
					},
					Body: []byte(`invalid json`),
				},
				&oauth2.RetrieveError{
					Response: &http.Response{
						StatusCode: http.StatusBadRequest,
					},
				},
			},
			want: &errorAuth{
				code: ecUnknownServer,
			},
		},
		{
			name: "retrieve_err_rate_limit",
			in: []error{
				&oauth2.RetrieveError{
					Response: &http.Response{
						StatusCode: http.StatusTooManyRequests,
					},
				},
			},
			want: &errorAuth{
				code: ecRateLimit,
			},
		},
		{
			name: "timeout",
			in:   []error{context.DeadlineExceeded, context.Canceled},
			want: &errorAuth{
				code: ecTimeout,
			},
		},
		{
			name: "network",
			in:   []error{&net.AddrError{}},
			want: &errorAuth{
				code: ecNetwork,
			},
		},
		{
			name: "unknown",
			in:   []error{errors.New("some err")},
			want: &errorAuth{
				code: ecUnknown,
			},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			for _, err := range tt.in {
				got := parseError(err)

				require.Equal(t, tt.want.Code(), got.Code())
			}
		})
	}
}
