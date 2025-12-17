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

type tokenJSON struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int64  `json:"expires_in"`
	Scope       string `json:"scope"`
}

type tokenIssuer struct {
	client *http.Client
	cfg    *Config
}

func newTokenIssuer(cfg *Config) *tokenIssuer {
	return &tokenIssuer{
		client: &http.Client{
			Timeout: defaultHttpTimeout,
		},
		cfg: cfg,
	}
}

func (ti *tokenIssuer) issueToken(ctx context.Context) (Token, error) {
	req, err := ti.newTokenRequest(ctx)
	if err != nil {
		return Token{}, err
	}

	resp, err := ti.client.Do(req)
	if err != nil {
		return Token{}, err
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return Token{}, fmt.Errorf("cannot fetch token: %w", err)
	}

	if c := resp.StatusCode; c < 200 || c > 299 {
		return Token{}, &oauth2.RetrieveError{
			Response: resp,
			Body:     body,
		}
	}

	var data tokenJSON
	if err := json.Unmarshal(body, &data); err != nil {
		return Token{}, err
	}

	var expiry time.Time
	if secs := data.ExpiresIn; secs > 0 {
		expiry = xtime.GetInaccurateTime().Add(time.Duration(secs) * time.Second)
	}
	return Token{
		AccessToken: data.AccessToken,
		TokenType:   data.TokenType,
		Expiry:      expiry,
		Scope:       data.Scope,
	}, nil
}

func (ti *tokenIssuer) newTokenRequest(ctx context.Context) (*http.Request, error) {
	v := url.Values{}
	v.Set("grant_type", grantTypeClientCreds)
	if len(ti.cfg.Scopes) > 0 {
		v.Set("scope", strings.Join(ti.cfg.Scopes, " "))
	}

	if ti.cfg.AuthStyle == AuthStyleInParams {
		v.Set("client_id", ti.cfg.ClientID)
		if ti.cfg.ClientSecret != "" {
			v.Set("client_secret", ti.cfg.ClientSecret)
		}
	}

	reqBody := io.NopCloser(strings.NewReader(v.Encode()))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ti.cfg.TokenURL, reqBody)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	if ti.cfg.AuthStyle == AuthStyleInHeader {
		req.SetBasicAuth(url.QueryEscape(ti.cfg.ClientID), url.QueryEscape(ti.cfg.ClientSecret))
	}

	return req, nil
}
