package xhttp

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

// largeReconnectInterval is set to time.Hour, so background ticker doesn't fire during test.
const (
	largeReconnectInterval        = time.Hour
	opBanEndpoint          string = "banEndpoint"
	opAddTime              string = "addTime"
)

type cbStep struct {
	operation   string
	idxEndpoint int
	duration    time.Duration
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
			endpoints: []string{"http://localhost:19200", "http://localhost:19201"},
			disabled:  true,
		},
		{
			name:      "single_endpoint",
			banPeriod: 2 * time.Second,
			endpoints: []string{"http://localhost:19200"},
			disabled:  true,
		},
		{
			name:      "two_and_more_endpoints",
			banPeriod: 3 * time.Second,
			endpoints: []string{"http://localhost:19200", "http://localhost:19201"},
			disabled:  false,
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			uris, err := parseEndpoints(tt.endpoints)
			require.NoError(t, err)

			ctx := t.Context()
			cb := newCircuitBreaker(ctx, uris, tt.banPeriod, largeReconnectInterval)

			if tt.disabled {
				require.Nil(t, cb, "circuit breaker must be disabled with these parameters")
				return
			}

			require.NotNil(t, cb)
			require.Len(t, cb.endpoints, len(tt.endpoints))
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
			name:       "ban_and_removes_from_active",
			endpoints:  []string{"http://localhost:19200", "http://localhost:19201", "http://localhost:19202"},
			banPeriod:  10 * time.Second,
			steps:      []cbStep{{operation: opBanEndpoint, idxEndpoint: 1}},
			wantActive: []int{0, 2},
			wantBanUntil: map[int]time.Duration{
				1: 10 * time.Second,
			},
		},
		{
			name:      "all_endpoints_banned",
			endpoints: []string{"http://localhost:19200", "http://localhost:19201", "http://localhost:19202"},
			banPeriod: 10 * time.Second,
			steps: []cbStep{
				{operation: opBanEndpoint, idxEndpoint: 0},
				{operation: opBanEndpoint, idxEndpoint: 1},
				{operation: opBanEndpoint, idxEndpoint: 2},
			},
			wantActive: nil,
		},
		{
			name:      "ban_refreshes_banUntil",
			endpoints: []string{"http://localhost:19200", "http://localhost:19201", "http://localhost:19202"},
			banPeriod: 10 * time.Second,
			steps: []cbStep{
				{operation: opBanEndpoint, idxEndpoint: 0},
				{operation: opAddTime, duration: 5 * time.Second},
				{operation: opBanEndpoint, idxEndpoint: 0},
			},
			wantActive: []int{1, 2},
			wantBanUntil: map[int]time.Duration{
				0: 15 * time.Second,
			},
		},
		{
			name:      "restore_before_endpoint_ban",
			endpoints: []string{"http://localhost:19200", "http://localhost:19201", "http://localhost:19202"},
			banPeriod: 40 * time.Second,
			steps: []cbStep{
				{operation: opBanEndpoint, idxEndpoint: 0},
				{operation: opAddTime, duration: 31 * time.Second},
			},
			wantActive: []int{1, 2},
		},
		{
			name:      "restore_after_endpoint_ban",
			endpoints: []string{"http://localhost:19200", "http://localhost:19201", "http://localhost:19202"},
			banPeriod: 25 * time.Second,
			steps: []cbStep{
				{operation: opBanEndpoint, idxEndpoint: 0},
				{operation: opAddTime, duration: 31 * time.Second},
			},
			wantActive: []int{0, 1, 2},
		},
		{
			name:      "partional_after_endpoint_ban",
			endpoints: []string{"http://localhost:19200", "http://localhost:19201", "http://localhost:19202"},
			banPeriod: 10 * time.Second,
			steps: []cbStep{
				{operation: opBanEndpoint, idxEndpoint: 0},
				{operation: opAddTime, duration: 25 * time.Second},
				{operation: opBanEndpoint, idxEndpoint: 1},
				{operation: opAddTime, duration: 10 * time.Second},
			},
			wantActive: []int{0, 2},
		},
	}

	for _, tt := range cases {
		tt := tt
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
						cb.banEndpoint(cb.endpoints[s.idxEndpoint].uri)
					case opAddTime:
						time.Sleep(s.duration)
					}
				}

				require.ElementsMatch(t, tt.wantActive, cb.activeEndpoints)

				for idx, dur := range tt.wantBanUntil {
					if dur == 0 {
						require.Zero(t, cb.endpoints[idx].banUntil, "endpoint[%d]: banUntil should be zero", idx)
					}
					require.Equal(t, start.Add(dur), cb.endpoints[idx].banUntil)
				}
			})
		})
	}
}

func TestCircuitBreakerFullCycle(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		uris, err := parseEndpoints([]string{"http://localhost:19200", "http://localhost:19201", "http://localhost:19202"})
		require.NoError(t, err)

		ctx := t.Context()
		cb := newCircuitBreaker(ctx, uris, 10*time.Second, 3*time.Second)
		require.NotNil(t, cb)
		cb.setNowFn(time.Now)

		ep0 := cb.endpoints[0].uri.String()
		ep1 := cb.endpoints[1].uri.String()
		ep2 := cb.endpoints[2].uri.String()

		require.ElementsMatch(t, []string{ep0, ep1, ep2}, pickedURIs(cb, 30))

		cb.banEndpoint(cb.endpoints[0].uri)
		require.ElementsMatch(t, []string{ep1, ep2}, pickedURIs(cb, 30))
		time.Sleep(5 * time.Second)

		cb.banEndpoint(cb.endpoints[1].uri)
		require.ElementsMatch(t, []string{ep2}, pickedURIs(cb, 30))
		time.Sleep(8 * time.Second)

		require.ElementsMatch(t, []string{ep0, ep2}, pickedURIs(cb, 30))

		time.Sleep(5 * time.Second)
		require.ElementsMatch(t, []string{ep0, ep1, ep2}, pickedURIs(cb, 30))
	})
}
func pickedURIs(cb *circuitBreaker, n int) []string {
	seen := make(map[string]struct{})
	for range n {
		uri := cb.getEndpoint()
		if uri == nil {
			continue
		}
		seen[uri.String()] = struct{}{}
	}

	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}

	return out
}
