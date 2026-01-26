package xoauth

import (
	"context"
	"fmt"
	"time"

	"github.com/ozontech/file.d/logger"
	"go.uber.org/atomic"
)

type AuthStyle int

const (
	AuthStyleUnknown AuthStyle = iota
	AuthStyleInParams
	AuthStyleInHeader
)

type Config struct {
	ClientID     string
	ClientSecret string
	TokenURL     string
	Scopes       []string
	AuthStyle    AuthStyle
}

func (c *Config) validate() error {
	if c.ClientID == "" {
		return fmt.Errorf("client id must be non-empty")
	}
	if c.TokenURL == "" {
		return fmt.Errorf("token url must be non-empty")
	}
	if c.AuthStyle == AuthStyleUnknown {
		return fmt.Errorf("auth style must be specified")
	}
	return nil
}

// tokenIssuer issues token for specific token grant type flow
type tokenIssuer interface {
	issueToken(ctx context.Context) (*Token, error)
}

type TokenSource interface {
	Token(ctx context.Context) *Token // read-only
	Stop()
}

type staticTokenSource struct {
	t *Token
}

func NewStaticTokenSource(t *Token) TokenSource {
	return &staticTokenSource{
		t: t,
	}
}

func (ts *staticTokenSource) Token(_ context.Context) *Token {
	return ts.t
}

func (ts *staticTokenSource) Stop() {}

// reuseTokenSource implements lifecycle of auth token refreshing.
//   - Once reuseTokenSource is created, first token issuance happens
//   - Further Token() calls must be non-blocking
//   - Token is updated in the background depending on the expidation date
//   - After Close() reuseTokenSource is irreversibly stops all background work
type reuseTokenSource struct {
	tokenHolder atomic.Pointer[Token]
	tokenIssuer tokenIssuer

	stopCh chan struct{}
}

func NewReuseTokenSource(ctx context.Context, cfg *Config) (TokenSource, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	ti := newHTTPTokenIssuer(cfg)
	return newReuseTokenSource(ctx, ti)
}

func newReuseTokenSource(ctx context.Context, ti tokenIssuer) (*reuseTokenSource, error) {
	ts := &reuseTokenSource{
		tokenHolder: atomic.Pointer[Token]{},
		tokenIssuer: ti,

		stopCh: make(chan struct{}),
	}

	// get first token during initialization to verify provided data
	t, err := ti.issueToken(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to init token source: %w", err)
	}

	ts.tokenHolder.Store(t)
	go ts.maintenance(ctx, time.Until(t.Expiry)/2)

	return ts, nil
}

func (ts *reuseTokenSource) Token(ctx context.Context) *Token {
	select {
	case <-ctx.Done():
		return nil
	default:
	}

	return ts.tokenHolder.Load()
}

func (ts *reuseTokenSource) Stop() {
	close(ts.stopCh)
}

// maintenance runs a token update loop
func (ts *reuseTokenSource) maintenance(ctx context.Context, firstDelay time.Duration) {
	scheduler := time.NewTimer(firstDelay)
	defer scheduler.Stop()

	// paths to calculate next scheduler delay
	success, fail := ts.newDelayer()

	updateToken := func() time.Duration {
		t, err := ts.tokenIssuer.issueToken(ctx)
		if err != nil {
			return fail(parseError(err))
		}

		ts.tokenHolder.Store(t)
		return success(time.Until(t.Expiry))
	}

	for {
		select {
		case <-ts.stopCh:
			return
		case <-ctx.Done():
			return
		case <-scheduler.C:
		}

		delay := updateToken()
		resetTimer(scheduler, delay)
	}
}

func resetTimer(t *time.Timer, d time.Duration) {
	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}
	t.Reset(d)
}

// newDelayer returns success and failure paths that will be applied to refresh scheduler
func (ts *reuseTokenSource) newDelayer() (
	func(ttl time.Duration) time.Duration, // success
	func(err *errorAuth) time.Duration, // failure
) {
	expBackoff := exponentialJitterBackoff()
	linBackoff := linearJitterBackoff()
	attempt := 0

	success := func(ttl time.Duration) time.Duration {
		attempt = 0
		return linBackoff(ttl/3, ttl/2, 0)
	}

	failure := func(err *errorAuth) time.Duration {
		attempt++
		code := err.Code()
		logger.Errorf("error occurred while updating oauth token: attempt=%d, code=%s, error=%s",
			attempt, code, err.Error())

		switch code {
		case ecInvalidRequest, ecInvalidClient, ecInvalidGrant, ecInvalidScope,
			ecUnauthorizedClient, ecUnsupportedGrantType:
			return linBackoff(time.Minute, 10*time.Minute, attempt)
		default:
			return expBackoff(time.Second, time.Minute, attempt)
		}
	}

	return success, failure
}
