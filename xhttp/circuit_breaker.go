package xhttp

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/ozontech/file.d/xtime"
	"github.com/valyala/fasthttp"
)

type endpoint struct {
	uri      *fasthttp.URI
	banUntil time.Time
}

type circuitBreaker struct {
	endpoints []endpoint
	idxByURI  map[string]int
	banPeriod time.Duration
	mu        sync.RWMutex
}

func NewCircuitBreaker(uris []*fasthttp.URI, banPeriod time.Duration) *circuitBreaker {
	cb := &circuitBreaker{
		endpoints: make([]endpoint, 0, len(uris)),
		idxByURI:  make(map[string]int, len(uris)),
		banPeriod: banPeriod,
	}

	for i, uri := range uris {
		cb.endpoints = append(cb.endpoints, endpoint{uri: uri})
		cb.idxByURI[uri.String()] = i
	}

	return cb
}

func (cb *circuitBreaker) getEndpoint() *fasthttp.URI {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	now := xtime.GetInaccurateTime()
	activeEndpoints := make([]*fasthttp.URI, 0, len(cb.endpoints))
	for i := range cb.endpoints {
		e := cb.endpoints[i]
		if e.banUntil.IsZero() || now.After(e.banUntil) {
			activeEndpoints = append(activeEndpoints, e.uri)
		}
	}
	switch len(activeEndpoints) {
	case 0:
		return nil
	case 1:
		return activeEndpoints[0]
	default:
		return activeEndpoints[rand.Int()%len(activeEndpoints)]
	}
}

func (cb *circuitBreaker) banEndpoint(uri *fasthttp.URI) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	idx := cb.idxByURI[uri.String()]
	cb.endpoints[idx].banUntil = xtime.GetInaccurateTime().Add(cb.banPeriod)
}

func (cb *circuitBreaker) restoreBannedEndpoints() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := xtime.GetInaccurateTime()
	for i := range cb.endpoints {
		e := &cb.endpoints[i]
		if !e.banUntil.IsZero() && now.After(e.banUntil) {
			e.banUntil = time.Time{}
		}
	}
}

func (cb *circuitBreaker) checkBannedEndpoints(ctx context.Context, reconnectInterval time.Duration) {
	ticker := time.NewTicker(reconnectInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cb.restoreBannedEndpoints()
		}
	}
}
