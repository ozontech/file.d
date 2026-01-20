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
	mu      sync.RWMutex
	metrics map[string]metricValue

	timeout      time.Duration
	flushTicker  *time.Ticker
	repeatTicker *time.Ticker
	shutdownChan chan struct{}
	sender       storageSender
	logger       *zap.Logger
}

type metricValue struct {
	value           float64
	timestamp       int64
	sendedTimestamp time.Time
	lastUpdateTime  time.Time
	expiredAt       time.Time
}

type storageSender interface {
	sendToStorage(values []promwrite.TimeSeries) error
}

func newCollector(sender storageSender, logger *zap.Logger) *metricCollector {
	c := &metricCollector{
		mu:           sync.RWMutex{},
		metrics:      make(map[string]metricValue),
		flushTicker:  time.NewTicker(10 * time.Second),
		repeatTicker: time.NewTicker(1 * time.Minute),
		shutdownChan: make(chan struct{}),
		sender:       sender,
		logger:       logger,
	}
	go c.flushAndRepeatOldMetrics()
	return c
}

func (p *metricCollector) handleMetric(labels []promwrite.Label, value float64, timestamp int64, metricType string, ttl int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	labelsKey := labelsToKey(labels)
	now := time.Now()

	var currentValue float64

	if prevMetric, ok := p.metrics[labelsKey]; ok {
		currentValue = value
		if metricType == "counter" {
			currentValue += prevMetric.value
		}
	} else {
		currentValue = value
	}

	p.metrics[labelsKey] = metricValue{
		value:          currentValue,
		timestamp:      timestamp,
		lastUpdateTime: now,
		expiredAt:      now.Add(time.Duration(ttl) * time.Millisecond),
	}
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
	p.mu.Lock()
	defer p.mu.Unlock()

	var toSend []promwrite.TimeSeries
	now := time.Now()
	nowUnixtime := now.UnixMilli()

	sendEvery := 1 * time.Minute
	freshDuration := 3 * time.Minute

	for key, metric := range p.metrics {
		labels := keyToLabels(key)

		lastUpdateSeconds := now.Sub(metric.lastUpdateTime)
		isFresh := lastUpdateSeconds < freshDuration

		if isFresh {
			timeSeries := createTimeSeries(labels, metric)
			if metric.sendedTimestamp != timeSeries.Sample.Time {
				toSend = append(toSend, timeSeries)
				metric.sendedTimestamp = timeSeries.Sample.Time
				p.metrics[key] = metric
			}
		} else if lastUpdateSeconds > sendEvery {
			metric.timestamp = nowUnixtime
			if now.Before(metric.expiredAt) {
				p.metrics[key] = metric
				timeSeries := createTimeSeries(labels, metric)
				toSend = append(toSend, timeSeries)
				metric.sendedTimestamp = timeSeries.Sample.Time
			} else {
				delete(p.metrics, key)
			}
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
	p.mu.Lock()
	defer p.mu.Unlock()

	// Flush all remaining metrics
	var toSend []promwrite.TimeSeries

	for key, metric := range p.metrics {
		labels := keyToLabels(key)
		toSend = append(toSend, createTimeSeries(labels, metric))
	}
	p.sender.sendToStorage(toSend)
}

// Helper function
func createTimeSeries(labels []promwrite.Label, metric metricValue) promwrite.TimeSeries {
	return promwrite.TimeSeries{
		Labels: labels,
		Sample: promwrite.Sample{
			Time:  time.Unix(0, metric.timestamp*int64(time.Millisecond)).Round(30 * time.Second),
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
