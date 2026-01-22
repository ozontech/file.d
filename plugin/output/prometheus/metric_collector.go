package prometheus

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/castai/promwrite"
	"go.uber.org/zap"
)

type metricCollector struct {
	sender       storageSender
	metrics      map[string]*metricValue
	mutex        sync.RWMutex
	flushTicker  *time.Ticker
	shutdownChan chan struct{}
	flushTimeout time.Duration

	logger *zap.Logger
}

type metricValue struct {
	value             float64
	timestamp         int64
	lastValueIsSended bool
	lastUpdateTime    time.Time
	sendedTimestamp   time.Time
	expiredAt         time.Time
}

type storageSender interface {
	sendToStorage(values []promwrite.TimeSeries) error
}

func newCollector(sender storageSender, flushTimeout time.Duration, logger *zap.Logger) *metricCollector {
	c := &metricCollector{
		sender:       sender,
		logger:       logger,
		metrics:      make(map[string]*metricValue),
		flushTicker:  time.NewTicker(flushTimeout),
		flushTimeout: flushTimeout,
		shutdownChan: make(chan struct{}),
	}
	go c.flushAndRepeatOldMetrics()
	return c
}

var i int

func (p *metricCollector) handleMetric(labels []promwrite.Label, value float64, timestamp int64, metricType string, ttl int64) {
	key := labelsToKey(labels)
	now := time.Now()
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if existing, exists := p.metrics[key]; exists {
		if metricType == "counter" {
			value += existing.value
		}
		timestamp = max(timestamp, existing.sendedTimestamp.UnixMilli())
	}

	nowUnixTime := now.UnixMilli()
	timestamp = min(timestamp, nowUnixTime)

	metric := &metricValue{
		value:             value,
		timestamp:         timestamp,
		lastUpdateTime:    now,
		lastValueIsSended: false,
		expiredAt:         now.Add(time.Duration(ttl) * time.Millisecond),
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
	now := time.Now()

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

	if len(toDelete) > 0 {
		for _, key := range toDelete {
			delete(p.metrics, key)
		}
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
func createTimeSeries(labels []promwrite.Label, metric *metricValue, roundPeriod time.Duration) promwrite.TimeSeries {
	return promwrite.TimeSeries{
		Labels: labels,
		Sample: promwrite.Sample{
			Time:  time.Unix(0, metric.timestamp*int64(time.Millisecond)).Truncate(roundPeriod),
			Value: metric.value,
		},
	}
}

func keyToLabels(key string) []promwrite.Label {
	if len(key) == 0 {
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
	sorted := make([]promwrite.Label, len(labels))
	copy(sorted, labels)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Name < sorted[j].Name
	})

	var b strings.Builder
	for _, l := range sorted {
		fmt.Fprintf(&b, "%s=%s,", l.Name, l.Value)
	}
	return b.String()
}
