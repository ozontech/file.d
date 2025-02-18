package playground

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

const (
	pipelineCapacity = 2
)

type PlayResponse struct {
	// Result is slice of events after all action plugins.
	Result []json.RawMessage `json:"result"`
	// Stdout is pipeline stdout during actions execution.
	Stdout string `json:"stdout"`
	// Metrics is prometheus metrics in openmetrics format.
	Metrics string `json:"metrics"`
}

type Handler struct {
	logger *zap.Logger

	concurrencyLimiter chan struct{}

	playground *playground
}

var _ http.Handler = (*Handler)(nil)

func NewHandler(logger *zap.Logger) *Handler {
	return &Handler{
		logger:             logger,
		concurrencyLimiter: make(chan struct{}, runtime.GOMAXPROCS(0)),
		playground:         newPlayground(logger),
	}
}

var (
	concurrencyReached = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "file_d_playground",
		Subsystem:   "api",
		Name:        "concurrency_reached_total",
		Help:        "Total number of requests that were locked on the concurrency limiter",
		ConstLabels: nil,
	})
	concurrencyTimeouts = promauto.NewCounter(prometheus.CounterOpts{
		Namespace:   "file_d_playground",
		Subsystem:   "api",
		Name:        "concurrency_timeouts_total",
		Help:        "Total number of requests that where rejected due to concurrency limiter",
		ConstLabels: nil,
	})
)

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "", http.StatusMethodNotAllowed)
		return
	}

	select {
	case h.concurrencyLimiter <- struct{}{}:
		defer func() { <-h.concurrencyLimiter }()
	default:
		concurrencyReached.Inc()

		const maxWaitDuration = time.Second * 30
		ctx, cancel := context.WithTimeout(r.Context(), maxWaitDuration)
		defer cancel()

		select {
		case <-ctx.Done():
			concurrencyTimeouts.Inc()
			http.Error(w, "concurrency limiter timeout", http.StatusRequestTimeout)
		case h.concurrencyLimiter <- struct{}{}:
			defer func() { <-h.concurrencyLimiter }()
		}
	}

	limitedBody := io.LimitReader(r.Body, 1<<20)
	req, err := unmarshalRequest(limitedBody)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if len(req.Events) > 32 || len(req.Events) == 0 || len(req.Actions) > 64 {
		http.Error(w, "validate error: events count must be in range [1, 32] and actions count [0, 64]", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), time.Second*2)
	defer cancel()

	resp, err := h.playground.Play(ctx, req)
	if err != nil {
		http.Error(w, fmt.Sprintf("do actions: %s", err.Error()), http.StatusBadRequest)
		return
	}
	_ = json.NewEncoder(w).Encode(resp)
}
