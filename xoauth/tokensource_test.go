package xoauth

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReuseTokenSource(t *testing.T) {
	ctx := context.Background()
	now := time.Now()

	issuer := &mockTokenIssuer{}

	// first token fail
	issuer.expect(nil, errors.New("some error"))
	_, err := newReuseTokenSource(ctx, issuer)
	require.Error(t, err)

	// first token success, save and start maintenance
	tok := &Token{AccessToken: "test-token", Expiry: now.Add(time.Second)}
	issuer.expect(tok, nil)
	source, err := newReuseTokenSource(ctx, issuer)
	require.NoError(t, err)

	// check token
	require.Equal(t, tok.AccessToken, source.Token(ctx).AccessToken)

	// first update will be after first token expire / 2,
	// set next token and wait
	tok = &Token{AccessToken: "test-token-1", Expiry: now.Add(2 * time.Second)}
	issuer.expect(tok, nil)
	time.Sleep(time.Second)

	// check token
	require.Equal(t, tok.AccessToken, source.Token(ctx).AccessToken)

	// stop token source and wait next potential update,
	// set issuer token which we won't have to get
	issuer.expect(&Token{AccessToken: "test-token-2", Expiry: now.Add(3 * time.Second)}, nil)
	source.Stop()
	time.Sleep(time.Second)

	// check that token is one that was saved before the stop
	require.Equal(t, tok.AccessToken, source.Token(ctx).AccessToken)

	// check that Token() with canceled context returns nil
	ctx2, cancel := context.WithCancel(ctx)
	cancel()
	require.Nil(t, source.Token(ctx2))
}

type mockTokenIssuer struct {
	token *Token
	err   error

	m sync.RWMutex
}

func (ti *mockTokenIssuer) issueToken(ctx context.Context) (*Token, error) {
	ti.m.RLock()
	defer ti.m.RUnlock()
	return ti.token, ti.err
}

func (ti *mockTokenIssuer) expect(t *Token, err error) {
	ti.m.Lock()
	defer ti.m.Unlock()
	ti.token, ti.err = t, err
}
