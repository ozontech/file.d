package metric

import (
	"slices"
	"strings"
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

func newHeldMetric[T prometheus.Metric](labels []string, metric T) *heldMetric[T] {
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
}

func newHeldMetricsStore[T prometheus.Metric]() *heldMetricsStore[T] {
	return &heldMetricsStore[T]{
		mu:            sync.RWMutex{},
		metricsByHash: make(map[uint64][]*heldMetric[T]),
	}
}

func (h *heldMetricsStore[T]) GetOrCreate(labels []string, newPromMetric func(...string) T) *heldMetric[T] {
	hash := computeStringsHash(labels)
	// fast path - metric exists
	h.mu.RLock()
	hMetric, ok := h.getHeldMetricByHash(labels, hash)
	h.mu.RUnlock()
	if ok {
		return hMetric
	}
	// slow path - create new metric
	return h.tryCreate(labels, hash, newPromMetric)
}

func (h *heldMetricsStore[T]) getHeldMetricByHash(labels []string, hash uint64) (*heldMetric[T], bool) {
	hMetrics, ok := h.metricsByHash[hash]
	if !ok {
		return nil, false
	}
	if len(hMetrics) == 1 {
		return hMetrics[0], true
	}

	if i := findHeldMetricIndex(hMetrics, labels); i != -1 {
		return hMetrics[i], true
	}
	return nil, false
}

func (h *heldMetricsStore[T]) tryCreate(labels []string, hash uint64, newPromMetric func(...string) T) *heldMetric[T] {
	// copy labels because they are unsafe converted bytes
	// TODO: replace with [][]byte to make it explicit
	labelsCopy := make([]string, len(labels))
	for i := range labels {
		labelsCopy[i] = strings.Clone(labels[i])
	}
	labels = labelsCopy

	metric := newPromMetric(labelsCopy...)

	h.mu.Lock()
	defer h.mu.Unlock()

	hMetric, ok := h.getHeldMetricByHash(labels, hash)
	if ok {
		return hMetric
	}

	hMetric = newHeldMetric[T](labels, metric)
	h.metricsByHash[hash] = append(h.metricsByHash[hash], hMetric)
	return hMetric
}

type metricDeleter interface {
	DeleteLabelValues(...string) bool
}

func (h *heldMetricsStore[T]) DeleteOldMetrics(holdDuration time.Duration, deleter metricDeleter) {
	now := xtime.GetInaccurateUnixNano()

	h.mu.Lock()
	defer h.mu.Unlock()

	for hash, hMetrics := range h.metricsByHash {
		releasedMetrics := slices.DeleteFunc(hMetrics, func(hMetric *heldMetric[T]) bool {
			lastUsage := hMetric.lastUsage.Load()
			diff := now - lastUsage
			isObsolete := diff > holdDuration.Nanoseconds()
			if isObsolete {
				deleter.DeleteLabelValues(hMetric.labels...)
				*hMetric = heldMetric[T]{} // release objects in the structure
			}
			return isObsolete
		})

		if len(releasedMetrics) == 0 {
			delete(h.metricsByHash, hash)
		}
	}
}

func findHeldMetricIndex[T prometheus.Metric](hMetrics []*heldMetric[T], labels []string) int {
	idx := -1
	for i := range hMetrics {
		if slices.Equal(hMetrics[i].labels, labels) {
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
