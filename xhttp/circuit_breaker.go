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
	endpoints       []endpoint
	activeEndpoints []int
	idxByURI        map[string]int
	banPeriod       time.Duration
	mu              sync.RWMutex
}

func newCircuitBreaker(ctx context.Context, uris []*fasthttp.URI, banPeriod, reconnectInterval time.Duration) *circuitBreaker {
	if banPeriod <= 0 {
		return nil
	}

	cb := &circuitBreaker{
		endpoints:       make([]endpoint, 0, len(uris)),
		activeEndpoints: make([]int, 0, len(uris)),
		idxByURI:        make(map[string]int, len(uris)),
		banPeriod:       banPeriod,
	}

	for i, uri := range uris {
		cb.endpoints = append(cb.endpoints, endpoint{uri: uri})
		cb.idxByURI[uri.String()] = i
		cb.activeEndpoints = append(cb.activeEndpoints, i)
	}

	go cb.checkBannedEndpoints(ctx, reconnectInterval)

	return cb
}

func (cb *circuitBreaker) getEndpoint() *fasthttp.URI {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	if len(cb.activeEndpoints) == 0 {
		return nil
	}

	idx := rand.Intn(len(cb.activeEndpoints))
	return cb.endpoints[cb.activeEndpoints[idx]].uri
}

func (cb *circuitBreaker) banEndpoint(uri *fasthttp.URI) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	idx := cb.idxByURI[uri.String()]
	cb.endpoints[idx].banUntil = xtime.GetInaccurateTime().Add(cb.banPeriod)

	for i, activeIdx := range cb.activeEndpoints {
		if activeIdx == idx {
			cb.activeEndpoints[i] = cb.activeEndpoints[len(cb.activeEndpoints)-1]
			cb.activeEndpoints = cb.activeEndpoints[:len(cb.activeEndpoints)-1]
			break
		}
	}
}

func (cb *circuitBreaker) restoreBannedEndpoints() {
	cb.mu.RLock()
	if len(cb.endpoints) == len(cb.activeEndpoints) {
		cb.mu.RUnlock()
		return
	}
	cb.mu.RUnlock()

	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := xtime.GetInaccurateTime()
	for i := range cb.endpoints {
		e := &cb.endpoints[i]
		if !e.banUntil.IsZero() && now.After(e.banUntil) {
			e.banUntil = time.Time{}
			cb.activeEndpoints = append(cb.activeEndpoints, i)
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
