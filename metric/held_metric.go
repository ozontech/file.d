package metric

import (
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ozontech/file.d/xtime"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/fasthash/fnv1a"
)

type heldLabels struct {
	labels    []string
	lastUsage *atomic.Int64 // unix timestamp

	// can be replaced for testing
	isObsoleteFunc func(int64, time.Duration) bool
}

func newHeldLabels(lvs []string) heldLabels {
	labels := make([]string, len(lvs))
	copy(labels, lvs)
	hl := heldLabels{
		labels:         labels,
		lastUsage:      new(atomic.Int64),
		isObsoleteFunc: isObsolete,
	}
	hl.updateUsage()
	return hl
}

func (h *heldLabels) updateUsage() {
	h.lastUsage.Store(xtime.GetInaccurateUnixNano())
}

func (h *heldLabels) isObsolete(holdDuration time.Duration) bool {
	if h.lastUsage != nil {
		return h.isObsoleteFunc(h.lastUsage.Load(), holdDuration)
	}
	return true
}

type HeldCounter struct {
	heldLabels
	counter prometheus.Counter
}

func (h *HeldCounter) Inc() {
	h.counter.Inc()
	h.updateUsage()
}

func (h *HeldCounter) Add(v float64) {
	h.counter.Add(v)
	h.updateUsage()
}

type HeldGauge struct {
	heldLabels
	gauge prometheus.Gauge
}

func (h *HeldGauge) Set(v float64) {
	h.gauge.Set(v)
	h.updateUsage()
}

func (h *HeldGauge) Inc() {
	h.gauge.Inc()
	h.updateUsage()
}

func (h *HeldGauge) Dec() {
	h.gauge.Dec()
	h.updateUsage()
}

func (h *HeldGauge) Add(v float64) {
	h.gauge.Add(v)
	h.updateUsage()
}

func (h *HeldGauge) Sub(v float64) {
	h.gauge.Sub(v)
	h.updateUsage()
}

type HeldHistogram struct {
	heldLabels
	histogram prometheus.Histogram
}

func (h *HeldHistogram) Observe(v float64) {
	h.histogram.Observe(v)
	h.updateUsage()
}

type heldLabelsVec struct {
	heldLabelsByHash map[uint64][]heldLabels
	mu               sync.RWMutex
}

func newHeldLabelsVec() heldLabelsVec {
	return heldLabelsVec{
		heldLabelsByHash: make(map[uint64][]heldLabels),
	}
}

func (h *heldLabelsVec) getOrCreateHeldLabels(lvs []string) heldLabels {
	lvsHash := hashLabels(lvs)

	h.mu.RLock()
	hl, ok := h.getHeldLabelsByHash(lvsHash, lvs)
	h.mu.RUnlock()
	if ok {
		return hl
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	hl, ok = h.getHeldLabelsByHash(lvsHash, lvs)
	if !ok {
		hl = newHeldLabels(lvs)
		h.heldLabelsByHash[lvsHash] = append(h.heldLabelsByHash[lvsHash], hl)
	}
	return hl
}

func (h *heldLabelsVec) getHeldLabelsByHash(hash uint64, lvs []string) (heldLabels, bool) {
	if hls, ok := h.heldLabelsByHash[hash]; ok {
		if i := findHeldLabelsIndex(hls, lvs); i != -1 {
			return hls[i], true
		}
	}
	return heldLabels{}, false
}

type HeldCounterVec struct {
	heldLabelsVec
	vec *prometheus.CounterVec
}

func newHeldCounterVec(cv *prometheus.CounterVec) *HeldCounterVec {
	return &HeldCounterVec{
		vec:           cv,
		heldLabelsVec: newHeldLabelsVec(),
	}
}

func (h *HeldCounterVec) WithLabelValues(lvs ...string) *HeldCounter {
	return &HeldCounter{
		counter:    h.vec.WithLabelValues(lvs...),
		heldLabels: h.getOrCreateHeldLabels(lvs),
	}
}

func (h *HeldCounterVec) update(holdDuration time.Duration) {
	deleteObsoleteMetrics(holdDuration, &h.heldLabelsVec, h.vec.MetricVec)
}

type HeldGaugeVec struct {
	heldLabelsVec
	vec *prometheus.GaugeVec
}

func newHeldGaugeVec(gv *prometheus.GaugeVec) *HeldGaugeVec {
	return &HeldGaugeVec{
		vec:           gv,
		heldLabelsVec: newHeldLabelsVec(),
	}
}

func (h *HeldGaugeVec) WithLabelValues(lvs ...string) *HeldGauge {
	return &HeldGauge{
		gauge:      h.vec.WithLabelValues(lvs...),
		heldLabels: h.getOrCreateHeldLabels(lvs),
	}
}

func (h *HeldGaugeVec) update(holdDuration time.Duration) {
	deleteObsoleteMetrics(holdDuration, &h.heldLabelsVec, h.vec.MetricVec)
}

type HeldHistogramVec struct {
	heldLabelsVec
	vec *prometheus.HistogramVec
}

func newHeldHistogramVec(hv *prometheus.HistogramVec) *HeldHistogramVec {
	return &HeldHistogramVec{
		vec:           hv,
		heldLabelsVec: newHeldLabelsVec(),
	}
}

func (h *HeldHistogramVec) WithLabelValues(lvs ...string) *HeldHistogram {
	return &HeldHistogram{
		histogram:  h.vec.WithLabelValues(lvs...).(prometheus.Histogram),
		heldLabels: h.getOrCreateHeldLabels(lvs),
	}
}

func (h *HeldHistogramVec) update(holdDuration time.Duration) {
	deleteObsoleteMetrics(holdDuration, &h.heldLabelsVec, h.vec.MetricVec)
}

func isObsolete(lastUsage int64, holdDuration time.Duration) bool {
	return xtime.GetInaccurateUnixNano()-lastUsage > holdDuration.Nanoseconds()
}

func hashLabels(lvs []string) uint64 {
	h := fnv1a.Init64
	for i := range lvs {
		h = fnv1a.AddString64(h, lvs[i])
	}
	return h
}

func findHeldLabelsIndex(hLabels []heldLabels, lvs []string) int {
	idx := -1
	for i := range hLabels {
		if slices.Equal(hLabels[i].labels, lvs) {
			idx = i
			break
		}
	}
	return idx
}

func deleteObsoleteMetrics(holdDuration time.Duration, hlVec *heldLabelsVec, mVec *prometheus.MetricVec) {
	hlVec.mu.Lock()
	defer hlVec.mu.Unlock()

	for hash, hls := range hlVec.heldLabelsByHash {
		i := 0
		for _, hl := range hls {
			if hl.isObsolete(holdDuration) {
				mVec.DeleteLabelValues(hl.labels...)
			} else {
				hls[i] = hl
				i++
			}
		}
		hlVec.heldLabelsByHash[hash] = hls[:i]
		if len(hlVec.heldLabelsByHash[hash]) == 0 {
			delete(hlVec.heldLabelsByHash, hash)
		}
	}
}
