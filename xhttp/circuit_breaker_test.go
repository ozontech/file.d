package xhttp

import (
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	opBanEndpoint = iota + 1
	opSleep
)

var (
	defaultEndpoints   = []string{"http://localhost:19200", "http://localhost:19201", "http://localhost:19202"}
	defaultWorkerCount = 50
)

type cbStep struct {
	operation int
	idxEp     int
	duration  time.Duration
}

func TestNewCircuitBreaker(t *testing.T) {
	cases := []struct {
		name      string
		banPeriod time.Duration
		endpoints []string
		disabled  bool
	}{
		{
			name:      "ban_period_zero",
			banPeriod: 0,
			endpoints: defaultEndpoints,
			disabled:  true,
		},
		{
			name:      "single_endpoint",
			banPeriod: 2 * time.Second,
			endpoints: defaultEndpoints[0:1],
			disabled:  true,
		},
		{
			name:      "two_and_more_endpoints",
			banPeriod: 3 * time.Second,
			endpoints: defaultEndpoints,
			disabled:  false,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			uris, err := parseEndpoints(tt.endpoints)
			require.NoError(t, err)

			ctx := t.Context()
			cb := newCircuitBreaker(ctx, uris, tt.banPeriod, 5*time.Minute)

			if tt.disabled {
				require.Nil(t, cb, "circuit breaker must be disabled with these parameters")
				return
			}

			require.NotNil(t, cb)
			require.Len(t, cb.activeEndpoints, len(tt.endpoints))
			require.Equal(t, cb.banPeriod, tt.banPeriod)
			for i := range cb.endpoints {
				require.Zero(t, cb.endpoints[i].banUntil, "endpoint[%d]: banUntil should be zero after creation", i)
			}
		})
	}
}

func TestCircuitBreakerScenarios(t *testing.T) {
	cases := []struct {
		name         string
		endpoints    []string
		banPeriod    time.Duration
		steps        []cbStep
		wantActive   []int
		wantBanUntil map[int]time.Duration
	}{
		{
			name:       "ban_removes_endpoint_from_active",
			endpoints:  defaultEndpoints,
			banPeriod:  10 * time.Second,
			steps:      []cbStep{{operation: opBanEndpoint, idxEp: 1}},
			wantActive: []int{0, 2},
			wantBanUntil: map[int]time.Duration{
				1: 10 * time.Second,
			},
		},
		{
			name:      "ban_all_endpoint",
			endpoints: defaultEndpoints,
			banPeriod: 10 * time.Second,
			steps: []cbStep{
				{operation: opBanEndpoint, idxEp: 0},
				{operation: opBanEndpoint, idxEp: 1},
				{operation: opBanEndpoint, idxEp: 2},
			},
			wantActive: nil,
		},
		{
			name:      "ban_refreshes_ban_until",
			endpoints: defaultEndpoints,
			banPeriod: 10 * time.Second,
			steps: []cbStep{
				{operation: opBanEndpoint, idxEp: 0},
				{operation: opSleep, duration: 5 * time.Second},
				{operation: opBanEndpoint, idxEp: 0},
			},
			wantActive: []int{1, 2},
			wantBanUntil: map[int]time.Duration{
				0: 15 * time.Second,
			},
		},
		{
			name:      "does_not_restore_before_ban_period",
			endpoints: defaultEndpoints,
			banPeriod: 40 * time.Second,
			steps: []cbStep{
				{operation: opBanEndpoint, idxEp: 0},
				{operation: opSleep, duration: 31 * time.Second},
			},
			wantActive: []int{1, 2},
		},
		{
			name:      "restores_after_ban_period",
			endpoints: defaultEndpoints,
			banPeriod: 25 * time.Second,
			steps: []cbStep{
				{operation: opBanEndpoint, idxEp: 0},
				{operation: opSleep, duration: 31 * time.Second},
			},
			wantActive: []int{0, 1, 2},
		},
		{
			name:      "partially_restores_expired_endpoints",
			endpoints: defaultEndpoints,
			banPeriod: 10 * time.Second,
			steps: []cbStep{
				{operation: opBanEndpoint, idxEp: 0},
				{operation: opSleep, duration: 25 * time.Second},
				{operation: opBanEndpoint, idxEp: 1},
				{operation: opSleep, duration: 10 * time.Second},
			},
			wantActive: []int{0, 2},
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				ctx := t.Context()

				uris, err := parseEndpoints(tt.endpoints)
				require.NoError(t, err)

				cb := newCircuitBreaker(ctx, uris, tt.banPeriod, 30*time.Second)
				require.NotNil(t, cb)
				cb.setNowFn(time.Now)

				start := time.Now()
				for _, s := range tt.steps {
					switch s.operation {
					case opBanEndpoint:
						cb.banEndpoint(cb.endpoints[s.idxEp].uri)
					case opSleep:
						time.Sleep(s.duration)
					}
				}

				cb.mu.RLock()
				activeEp := append([]int{}, cb.activeEndpoints...)
				banUntil := make([]time.Time, len(cb.endpoints))
				for i := range cb.endpoints {
					banUntil[i] = cb.endpoints[i].banUntil
				}
				cb.mu.RUnlock()

				require.ElementsMatch(t, tt.wantActive, activeEp)

				for idx, dur := range tt.wantBanUntil {
					if dur == 0 {
						require.Zero(t, banUntil[idx], "endpoint[%d]: banUntil should be zero", idx)
					}
					require.Equal(t, start.Add(dur), banUntil[idx])
				}
			})
		})
	}
}

func TestCircuitBreakerFullCycle(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		uris, err := parseEndpoints(defaultEndpoints)
		require.NoError(t, err)

		ctx := t.Context()
		cb := newCircuitBreaker(ctx, uris, 10*time.Second, 3*time.Second)
		require.NotNil(t, cb)
		cb.setNowFn(time.Now)

		ep0, ep1, ep2 := cb.endpoints[0].uri.String(), cb.endpoints[1].uri.String(), cb.endpoints[2].uri.String()
		require.ElementsMatch(t, []string{ep0, ep1, ep2}, pickedURIs(cb, defaultWorkerCount))

		cb.banEndpoint(cb.endpoints[0].uri)
		require.ElementsMatch(t, []string{ep1, ep2}, pickedURIs(cb, defaultWorkerCount))
		time.Sleep(5 * time.Second)

		cb.banEndpoint(cb.endpoints[1].uri)
		require.ElementsMatch(t, []string{ep2}, pickedURIs(cb, defaultWorkerCount))
		time.Sleep(8 * time.Second)

		require.ElementsMatch(t, []string{ep0, ep2}, pickedURIs(cb, defaultWorkerCount))

		time.Sleep(7 * time.Second)
		require.ElementsMatch(t, []string{ep0, ep1, ep2}, pickedURIs(cb, defaultWorkerCount))
	})
}

func pickedURIs(cb *circuitBreaker, workers int) []string {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		seen = make(map[string]struct{})
	)

	for range workers {
		wg.Go(func() {
			uri := cb.getEndpoint()
			if uri == nil {
				return
			}

			mu.Lock()
			seen[uri.String()] = struct{}{}
			mu.Unlock()
		})
	}
	wg.Wait()

	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}

	return out
}
