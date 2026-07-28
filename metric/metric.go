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

// heldMetricWrapper is implemented by all metric wrappers (Counter, Gauge, Histogram).
// It gives the store access to the common held data while letting it cache and return
// the concrete wrapper directly (without re-allocating on each WithLabelValues call).
type heldMetricWrapper[T prometheus.Metric] interface {
	getHeldMetric() *heldMetric[T]
}

type heldMetricsStore[T prometheus.Metric, W heldMetricWrapper[T]] struct {
	mu                        sync.RWMutex
	metricsByHash             map[uint64][]W
	metricMaxLabelValueLength int
	newWrapper                func(*heldMetric[T]) W
}

func newHeldMetricsStore[T prometheus.Metric, W heldMetricWrapper[T]](
	metricMaxLabelValueLength int,
	newWrapper func(*heldMetric[T]) W,
) *heldMetricsStore[T, W] {
	return &heldMetricsStore[T, W]{
		mu:                        sync.RWMutex{},
		metricsByHash:             make(map[uint64][]W),
		metricMaxLabelValueLength: metricMaxLabelValueLength,
		newWrapper:                newWrapper,
	}
}

func (h *heldMetricsStore[T, W]) GetOrCreate(labels []string, newPromMetric func(...string) T) W {
	h.truncateLabels(labels)
	hash := computeStringsHash(labels)
	// fast path - wrapper exists
	h.mu.RLock()
	w, ok := h.getByHash(labels, hash)
	h.mu.RUnlock()
	if ok {
		return w
	}
	// slow path - create new wrapper
	return h.tryCreate(labels, hash, newPromMetric)
}

func (h *heldMetricsStore[T, W]) Delete(labels []string, deleter metricDeleter) bool {
	h.truncateLabels(labels)
	hash := computeStringsHash(labels)

	h.mu.Lock()
	defer h.mu.Unlock()

	wrappers, ok := h.metricsByHash[hash]
	if !ok {
		return false
	}

	i := h.findIndex(wrappers, labels)
	if i == -1 {
		return false
	}

	deleter.DeleteLabelValues(labels...)
	*wrappers[i].getHeldMetric() = heldMetric[T]{}
	wrappers = append(wrappers[:i], wrappers[i+1:]...)

	if len(wrappers) == 0 {
		delete(h.metricsByHash, hash)
	} else {
		h.metricsByHash[hash] = wrappers
	}

	return ok
}

func (h *heldMetricsStore[T, W]) getByHash(labels []string, hash uint64) (W, bool) {
	wrappers, ok := h.metricsByHash[hash]
	if !ok {
		var zero W
		return zero, false
	}
	if len(wrappers) == 1 {
		return wrappers[0], true
	}

	if i := h.findIndex(wrappers, labels); i != -1 {
		return wrappers[i], true
	}

	var zero W
	return zero, false
}

func (h *heldMetricsStore[T, W]) tryCreate(labels []string, hash uint64, newPromMetric func(...string) T) W {
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

	if w, ok := h.getByHash(labels, hash); ok {
		return w
	}

	w := h.newWrapper(newHeldMetric(labels, metric))
	h.metricsByHash[hash] = append(h.metricsByHash[hash], w)
	return w
}

type metricDeleter interface {
	DeleteLabelValues(...string) bool
}

func (h *heldMetricsStore[T, W]) DeleteOldMetrics(holdDuration time.Duration, deleter metricDeleter) {
	now := xtime.GetInaccurateUnixNano()

	h.mu.Lock()
	defer h.mu.Unlock()

	for hash, wrappers := range h.metricsByHash {
		releasedMetrics := slices.DeleteFunc(wrappers, func(w W) bool {
			hm := w.getHeldMetric()
			lastUsage := hm.lastUsage.Load()
			diff := now - lastUsage
			isObsolete := diff > holdDuration.Nanoseconds()
			if isObsolete {
				deleter.DeleteLabelValues(hm.labels...)
				*hm = heldMetric[T]{} // release objects in the structure
			}
			return isObsolete
		})

		if len(releasedMetrics) == 0 {
			delete(h.metricsByHash, hash)
		} else {
			h.metricsByHash[hash] = releasedMetrics
		}
	}
}

func (h *heldMetricsStore[T, W]) findIndex(wrappers []W, labels []string) int {
	for i := range wrappers {
		if slices.Equal(wrappers[i].getHeldMetric().labels, labels) {
			return i
		}
	}
	return -1
}

func (h *heldMetricsStore[T, W]) truncateLabels(lvs []string) {
	if h.metricMaxLabelValueLength == 0 {
		return
	}

	for i, label := range lvs {
		if len(label) > h.metricMaxLabelValueLength {
			lvs[i] = label[:h.metricMaxLabelValueLength]
		}
	}
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
