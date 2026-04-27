package xhttp

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/ozontech/file.d/xtime"
	"github.com/valyala/fasthttp"
)

type CircuitBreaker struct {
	activeEndpoints []*fasthttp.URI
	endpointsByID   map[string]*fasthttp.URI
	bannedUntil     map[string]time.Time
	banPeriod       time.Duration
	mu              sync.RWMutex
}

func NewCircuitBreaker(endpoints []*fasthttp.URI, banPeriod time.Duration) *CircuitBreaker {
	cb := &CircuitBreaker{
		activeEndpoints: make([]*fasthttp.URI, 0, len(endpoints)),
		endpointsByID:   make(map[string]*fasthttp.URI, len(endpoints)),
		bannedUntil:     make(map[string]time.Time, len(endpoints)),
		banPeriod:       banPeriod,
	}

	for _, endpoint := range endpoints {
		id := endpoint.String()
		cb.endpointsByID[id] = endpoint
		cb.activeEndpoints = append(cb.activeEndpoints, endpoint)
	}

	return cb
}

func (cb *CircuitBreaker) GetEndpoint() *fasthttp.URI {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	switch len(cb.activeEndpoints) {
	case 0:
		return nil
	case 1:
		return cb.activeEndpoints[0]
	default:
		return cb.activeEndpoints[rand.Int()%len(cb.activeEndpoints)]
	}
}

func (cb *CircuitBreaker) BanEndpoint(endpoint *fasthttp.URI) {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	id := endpoint.String()
	filtered := cb.activeEndpoints[:0]
	for _, endpoint := range cb.activeEndpoints {
		if endpoint.String() != id {
			filtered = append(filtered, endpoint)
		}
	}

	cb.activeEndpoints = filtered
	cb.bannedUntil[id] = xtime.GetInaccurateTime().Add(cb.banPeriod)
}

func (cb *CircuitBreaker) RestoreBannedEndpoints() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	for id, until := range cb.bannedUntil {
		if xtime.GetInaccurateTime().Before(until) {
			continue
		}
		endpoint := cb.endpointsByID[id]
		cb.activeEndpoints = append(cb.activeEndpoints, endpoint)
		delete(cb.bannedUntil, id)
	}
}

func (cb *CircuitBreaker) CheckBannedEndpoints(ctx context.Context, reconnectInterval time.Duration) {
	ticker := time.NewTicker(reconnectInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cb.RestoreBannedEndpoints()
		}
	}
}
