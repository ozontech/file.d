package prometheus

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ozontech/file.d/xtime"

	"github.com/castai/promwrite"
	"go.uber.org/zap"
)

type metricCollector struct {
	sender       storageSender
	metrics      map[string]*metricCollectorValue
	mutex        sync.RWMutex
	flushTicker  *time.Ticker
	shutdownChan chan struct{}
	flushTimeout time.Duration

	logger *zap.Logger
}

type metricCollectorValue struct {
	value             float64
	timestamp         int64
	lastValueIsSended bool
	lastUpdateTime    time.Time
	sendedTimestamp   time.Time
	expiredAt         time.Time
}

type metricData struct {
	labels     []promwrite.Label
	value      float64
	timestamp  int64
	metricType string
	ttl        int64
}

type storageSender interface {
	sendToStorage(values []promwrite.TimeSeries) error
}

func newCollector(sender storageSender, flushTimeout time.Duration, logger *zap.Logger) *metricCollector {
	c := &metricCollector{
		sender:       sender,
		logger:       logger,
		metrics:      make(map[string]*metricCollectorValue),
		flushTicker:  time.NewTicker(flushTimeout),
		flushTimeout: flushTimeout,
		shutdownChan: make(chan struct{}),
	}
	go c.flushAndRepeatOldMetrics()
	return c
}

func (p *metricCollector) handleMetric(data metricData) {
	key := labelsToKey(data.labels)
	now := xtime.GetInaccurateTime()
	p.mutex.Lock()
	defer p.mutex.Unlock()

	value := data.value
	timestamp := data.timestamp

	if existing, exists := p.metrics[key]; exists {
		if data.metricType == metricTypeCounter {
			value += existing.value
		}
		timestamp = max(timestamp, existing.sendedTimestamp.UnixMilli())
	}

	nowUnixTime := now.UnixMilli()
	timestamp = min(timestamp, nowUnixTime)

	metric := &metricCollectorValue{
		value:             value,
		timestamp:         timestamp,
		lastUpdateTime:    now,
		lastValueIsSended: false,
		expiredAt:         now.Add(time.Duration(data.ttl) * time.Millisecond),
	}
	p.metrics[key] = metric
}

func (p *metricCollector) flushAndRepeatOldMetrics() {
	for {
		select {
		case <-p.flushTicker.C:
			p.flushMetrics()
		case <-p.shutdownChan:
			p.flushTicker.Stop()
			return
		}
	}
}

func (p *metricCollector) flushMetrics() {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	var toSend []promwrite.TimeSeries
	now := xtime.GetInaccurateTime()

	toDelete := []string{}

	for key, metric := range p.metrics {
		labels := keyToLabels(key)

		if metric.lastValueIsSended && now.Sub(metric.lastUpdateTime) >= p.flushTimeout && now.Before(metric.expiredAt) {
			// repeat value
			metric.timestamp = now.UnixMilli()
		}

		timeSeries := createTimeSeries(labels, metric, p.flushTimeout)
		if metric.sendedTimestamp != timeSeries.Sample.Time {
			toSend = append(toSend, timeSeries)
			metric.sendedTimestamp = timeSeries.Sample.Time
			metric.lastValueIsSended = true
			p.metrics[key] = metric
			if now.After(metric.expiredAt) {
				toDelete = append(toDelete, key)
			}
		}
	}

	for _, key := range toDelete {
		delete(p.metrics, key)
	}

	if len(toSend) > 0 {
		// Send these metrics to your storage
		err := p.sender.sendToStorage(toSend)
		if err != nil {
			p.logger.Error("can't send data", zap.Error(err))
		}
	}
}

func (p *metricCollector) shutdown() {
	close(p.shutdownChan)
	p.flushMetrics()
}

// Helper function
func createTimeSeries(labels []promwrite.Label, metric *metricCollectorValue, roundPeriod time.Duration) promwrite.TimeSeries {
	return promwrite.TimeSeries{
		Labels: labels,
		Sample: promwrite.Sample{
			Time:  time.Unix(0, metric.timestamp*int64(time.Millisecond)).Truncate(roundPeriod),
			Value: metric.value,
		},
	}
}

func keyToLabels(key string) []promwrite.Label {
	if key == "" {
		return nil
	}
	key = key[:len(key)-1] // Remove trailing comma
	labels := make([]promwrite.Label, 0, strings.Count(key, ",")+1)

	for key != "" {
		pair, rest, _ := strings.Cut(key, ",")
		name, value, _ := strings.Cut(pair, "=")
		labels = append(labels, promwrite.Label{Name: name, Value: value})
		key = rest
	}
	return labels
}

func labelsToKey(labels []promwrite.Label) string {
	// Fast path: if labels are already sorted or only 1-2 labels, skip sorting
	if len(labels) <= 1 {
		if len(labels) == 0 {
			return ""
		}
		return labels[0].Name + "=" + labels[0].Value + ","
	}

	sorted := make([]promwrite.Label, len(labels))
	copy(sorted, labels)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Name < sorted[j].Name
	})

	// Preallocate builder with estimated size
	var b strings.Builder
	b.Grow(len(labels) * 32) // ~32 bytes per label avg
	for _, l := range sorted {
		b.WriteString(l.Name)
		b.WriteByte('=')
		b.WriteString(l.Value)
		b.WriteByte(',')
	}
	return b.String()
}
