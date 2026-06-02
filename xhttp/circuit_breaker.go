package xhttp

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/xtime"
	"github.com/valyala/fasthttp"
	"go.uber.org/zap"
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

	logger                *zap.Logger
	bannedEndpointsMetric *metric.Gauge

	mu    sync.RWMutex
	nowFn func() time.Time
}

func newCircuitBreaker(
	ctx context.Context,
	logger *zap.Logger,
	uris []*fasthttp.URI,
	banPeriod, reconnectInterval time.Duration,
	bannedEndpointsMetric *metric.Gauge,
) *circuitBreaker {
	if banPeriod <= 0 || len(uris) == 1 {
		return nil
	}

	cb := &circuitBreaker{
		endpoints:             make([]endpoint, 0, len(uris)),
		activeEndpoints:       make([]int, 0, len(uris)),
		idxByURI:              make(map[string]int, len(uris)),
		banPeriod:             banPeriod,
		logger:                logger,
		bannedEndpointsMetric: bannedEndpointsMetric,
		nowFn:                 xtime.GetInaccurateTime,
	}

	for i, uri := range uris {
		cb.endpoints = append(cb.endpoints, endpoint{uri: uri})
		cb.idxByURI[uri.String()] = i
		cb.activeEndpoints = append(cb.activeEndpoints, i)
	}

	go cb.checkBannedEndpoints(ctx, reconnectInterval)

	return cb
}

func (cb *circuitBreaker) updateBannedEndpointsMetric() {
	if cb.bannedEndpointsMetric == nil {
		return
	}

	cb.bannedEndpointsMetric.Set(float64(len(cb.endpoints) - len(cb.activeEndpoints)))
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
	cb.endpoints[idx].banUntil = cb.nowFn().Add(cb.banPeriod)

	for i, activeIdx := range cb.activeEndpoints {
		if activeIdx == idx {
			cb.activeEndpoints[i] = cb.activeEndpoints[len(cb.activeEndpoints)-1]
			cb.activeEndpoints = cb.activeEndpoints[:len(cb.activeEndpoints)-1]
			break
		}
	}

	cb.logger.Info(
		"endpoint banned",
		zap.String("endpoint", uri.String()),
		zap.Duration("ban_period", cb.banPeriod),
		zap.Int("active_endpoints_count", len(cb.activeEndpoints)),
		zap.Int("banned_endpoints_count", len(cb.endpoints)-len(cb.activeEndpoints)),
	)

	cb.updateBannedEndpointsMetric()
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

	hasRestoredEndpoints := false
	now := cb.nowFn()
	for i := range cb.endpoints {
		e := &cb.endpoints[i]
		if !e.banUntil.IsZero() && now.After(e.banUntil) {
			e.banUntil = time.Time{}
			cb.activeEndpoints = append(cb.activeEndpoints, i)
			hasRestoredEndpoints = true

			cb.logger.Info(
				"endpoint restored",
				zap.String("endpoint", e.uri.String()),
				zap.Int("active_endpoints_count", len(cb.activeEndpoints)),
				zap.Int("banned_endpoints_count", len(cb.endpoints)-len(cb.activeEndpoints)),
			)
		}
	}

	if hasRestoredEndpoints {
		cb.updateBannedEndpointsMetric()
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

func (cb *circuitBreaker) setNowFn(nowFn func() time.Time) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.nowFn = nowFn
}
