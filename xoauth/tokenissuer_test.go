package xoauth

import (
	"context"
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewTokenRequest(t *testing.T) {
	cases := []struct {
		name string
		cfg  *Config

		wantBodyForm map[string]string
		wantHeader   map[string]string
	}{
		{
			name: "auth_in_params",
			cfg: &Config{
				ClientID:     "test-client",
				ClientSecret: "test-secret",
				TokenURL:     "http://example.com",
				AuthStyle:    AuthStyleInParams,
			},
			wantBodyForm: map[string]string{
				"grant_type":    "client_credentials",
				"client_id":     "test-client",
				"client_secret": "test-secret",
			},
			wantHeader: map[string]string{
				"Content-Type": "application/x-www-form-urlencoded",
			},
		},
		{
			name: "auth_in_params_no_secret",
			cfg: &Config{
				ClientID:  "test-client",
				TokenURL:  "http://example.com",
				AuthStyle: AuthStyleInParams,
			},
			wantBodyForm: map[string]string{
				"grant_type": "client_credentials",
				"client_id":  "test-client",
			},
			wantHeader: map[string]string{
				"Content-Type": "application/x-www-form-urlencoded",
			},
		},
		{
			name: "auth_in_header",
			cfg: &Config{
				ClientID:     "test-client",
				ClientSecret: "test-secret",
				TokenURL:     "http://example.com",
				AuthStyle:    AuthStyleInHeader,
			},
			wantBodyForm: map[string]string{
				"grant_type": "client_credentials",
			},
			wantHeader: map[string]string{
				"Content-Type":  "application/x-www-form-urlencoded",
				"Authorization": basicAuth("test-client", "test-secret"),
			},
		},
		{
			name: "auth_in_header_no_secret",
			cfg: &Config{
				ClientID:  "test-client",
				TokenURL:  "http://example.com",
				AuthStyle: AuthStyleInHeader,
			},
			wantBodyForm: map[string]string{
				"grant_type": "client_credentials",
			},
			wantHeader: map[string]string{
				"Content-Type":  "application/x-www-form-urlencoded",
				"Authorization": basicAuth("test-client", ""),
			},
		},
		{
			name: "scopes",
			cfg: &Config{
				ClientID:  "test-client",
				TokenURL:  "http://example.com",
				Scopes:    []string{"scp1", "scp2"},
				AuthStyle: AuthStyleInParams,
			},
			wantBodyForm: map[string]string{
				"grant_type": "client_credentials",
				"client_id":  "test-client",
				"scope":      "scp1 scp2",
			},
			wantHeader: map[string]string{
				"Content-Type": "application/x-www-form-urlencoded",
			},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := newTokenRequest(context.Background(), tt.cfg)
			require.NoError(t, err)

			for k, v := range tt.wantBodyForm {
				require.Equal(t, v, got.PostFormValue(k))
			}

			for k, v := range tt.wantHeader {
				require.Equal(t, v, got.Header.Get(k))
			}
		})
	}
}

func basicAuth(user, pass string) string {
	auth := user + ":" + pass
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(auth))
}
