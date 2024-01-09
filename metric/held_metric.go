package metric

import (
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cespare/xxhash/v2"
	"github.com/ozontech/file.d/xtime"
	"github.com/prometheus/client_golang/prometheus"
)

type heldMetric[T prometheus.Metric] struct {
	labels    []string
	lastUsage atomic.Int64 // unixnano timestamp
	metric    T
}

func newHeldMetric[T prometheus.Metric](lvs []string, metric T) *heldMetric[T] {
	labels := make([]string, len(lvs))
	// copy labels because they are unsafe strings
	copy(labels, lvs)
	hl := &heldMetric[T]{
		labels:    labels,
		lastUsage: atomic.Int64{},
		metric:    metric,
	}
	hl.updateUsage()
	return hl
}

var updateThreshold = (time.Second * 10).Nanoseconds()

func (h *heldMetric[T]) updateUsage() {
	now := xtime.GetInaccurateUnixNano()

	// optimize atomic writes,
	// because it is not important for us to have the newest state
	if lastUsage := h.lastUsage.Load(); now-lastUsage > updateThreshold {
		h.lastUsage.Store(now)
	}
}

type heldMetricsStore[T prometheus.Metric] struct {
	mu            sync.RWMutex
	metricsByHash map[uint64][]*heldMetric[T]

	// used in tests
	unixNanoFunc func() int64
}

func newHeldLabelsStore[T prometheus.Metric]() *heldMetricsStore[T] {
	return &heldMetricsStore[T]{
		mu:            sync.RWMutex{},
		metricsByHash: make(map[uint64][]*heldMetric[T]),
		unixNanoFunc:  xtime.GetInaccurateUnixNano,
	}
}

func (h *heldMetricsStore[T]) GetOrCreate(labels []string, createMetric func(...string) T) *heldMetric[T] {
	hash := computeStringsHash(labels)
	// fast path - metric exists
	h.mu.RLock()
	held, ok := h.getHeldLabelsByHash(labels, hash)
	h.mu.RUnlock()
	if ok {
		return held
	}
	// slow path - create new metric
	return h.tryCreate(labels, hash, createMetric)
}

func (h *heldMetricsStore[T]) getHeldLabelsByHash(lvs []string, hash uint64) (*heldMetric[T], bool) {
	hls, ok := h.metricsByHash[hash]
	if !ok {
		return nil, false
	}
	if len(hls) == 1 {
		return hls[0], true
	}

	if i := findHeldLabelsIndex(hls, lvs); i != -1 {
		return hls[i], true
	}
	return nil, false
}

type metricReleaser interface {
	DeleteLabelValues(...string) bool
}

func (h *heldMetricsStore[T]) ReleaseOldMetrics(holdDuration time.Duration, releaser metricReleaser) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for hash, hashedLabels := range h.metricsByHash {
		releasedMetrics := slices.DeleteFunc(hashedLabels, func(held *heldMetric[T]) bool {
			isObsolete := h.unixNanoFunc()-held.lastUsage.Load() > holdDuration.Nanoseconds()
			if isObsolete {
				releaser.DeleteLabelValues(held.labels...)
				*held = heldMetric[T]{} // release objects in the structure
				return true
			}
			return false
		})

		if len(releasedMetrics) == 0 {
			delete(h.metricsByHash, hash)
		}
	}
}

func (h *heldMetricsStore[T]) tryCreate(labels []string, hash uint64, createMetric func(...string) T) *heldMetric[T] {
	h.mu.Lock()
	defer h.mu.Unlock()
	held, ok := h.getHeldLabelsByHash(labels, hash)
	if ok {
		return held
	}

	held = newHeldMetric[T](labels, createMetric(labels...))
	h.metricsByHash[hash] = append(h.metricsByHash[hash], held)
	return held
}

func findHeldLabelsIndex[T prometheus.Metric](hLabels []*heldMetric[T], lvs []string) int {
	idx := -1
	for i := range hLabels {
		if slices.Equal(hLabels[i].labels, lvs) {
			idx = i
			break
		}
	}
	return idx
}

func computeStringsHash(s []string) uint64 {
	var hash uint64
	if len(s) == 1 {
		hash = xxhash.Sum64String(s[0])
	} else {
		digest := xxhash.New()
		for i := range s {
			_, _ = digest.WriteString(s[i])
		}
		hash = digest.Sum64()
	}
	return hash
}
