package xoauth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ozontech/file.d/xtime"
	"golang.org/x/oauth2"
)

const (
	defaultHttpTimeout = 1 * time.Minute

	grantTypeClientCreds = "client_credentials"
)

type httpTokenIssuer struct {
	client *http.Client
	cfg    *Config
}

func newHTTPTokenIssuer(cfg *Config) *httpTokenIssuer {
	return &httpTokenIssuer{
		client: &http.Client{
			Timeout: defaultHttpTimeout,
		},
		cfg: cfg,
	}
}

type tokenJSON struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int64  `json:"expires_in"`
	Scope       string `json:"scope"`
}

func (ti *httpTokenIssuer) issueToken(ctx context.Context) (*Token, error) {
	req, err := newTokenRequest(ctx, ti.cfg)
	if err != nil {
		return nil, err
	}

	resp, err := ti.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("cannot fetch token: %w", err)
	}

	if c := resp.StatusCode; c < http.StatusOK || c >= http.StatusMultipleChoices {
		return nil, &oauth2.RetrieveError{
			Response: resp,
			Body:     body,
		}
	}

	var data tokenJSON
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, err
	}

	var expiry time.Time
	if secs := data.ExpiresIn; secs > 0 {
		expiry = xtime.GetInaccurateTime().Add(time.Duration(secs) * time.Second)
	}
	return &Token{
		AccessToken: data.AccessToken,
		TokenType:   data.TokenType,
		Expiry:      expiry,
		Scope:       data.Scope,
	}, nil
}

func newTokenRequest(ctx context.Context, cfg *Config) (*http.Request, error) {
	v := url.Values{}
	v.Set("grant_type", grantTypeClientCreds)
	if len(cfg.Scopes) > 0 {
		v.Set("scope", strings.Join(cfg.Scopes, " "))
	}

	if cfg.AuthStyle == AuthStyleInParams {
		v.Set("client_id", cfg.ClientID)
		if cfg.ClientSecret != "" {
			v.Set("client_secret", cfg.ClientSecret)
		}
	}

	reqBody := io.NopCloser(strings.NewReader(v.Encode()))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.TokenURL, reqBody)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if cfg.AuthStyle == AuthStyleInHeader {
		req.SetBasicAuth(url.QueryEscape(cfg.ClientID), url.QueryEscape(cfg.ClientSecret))
	}

	return req, nil
}
