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
	collector    *sync.Map
	timeout      time.Duration
	flushTicker  *time.Ticker
	shutdownChan chan struct{}
	sender       storageSender
	logger       *zap.Logger
}

type metricValue struct {
	value             float64
	timestamp         int64
	lastUpdateTime    time.Time
	lastValueIsSended bool
}

type storageSender interface {
	sendToStorage(values []promwrite.TimeSeries) error
}

func newCollector(timeout time.Duration, sender storageSender, logger *zap.Logger) *metricCollector {
	c := &metricCollector{
		collector:    new(sync.Map),
		timeout:      timeout,
		flushTicker:  time.NewTicker(1 * time.Second), // Check every second
		shutdownChan: make(chan struct{}),
		sender:       sender,
		logger:       logger,
	}
	go c.flushOldMetrics()
	return c
}

func (p *metricCollector) handleMetric(labels []promwrite.Label, value float64, timestamp int64, metricType string) []promwrite.TimeSeries {
	labelsKey := labelsToKey(labels)
	now := time.Now()
	currentTimestampSec := timestamp / 1_000_000_000

	var values []promwrite.TimeSeries
	var shouldSend bool
	var currentValue float64
	var prevMetric metricValue

	if prev, ok := p.collector.Load(labelsKey); ok {
		prevMetric = prev.(metricValue)
		prevTimestampSec := prevMetric.timestamp / 1_000_000_000

		// For counters, accumulate values
		currentValue = value
		if metricType == "counter" {
			currentValue += prevMetric.value
		}

		// Check if time window advanced
		shouldSend = prevTimestampSec < currentTimestampSec

		if shouldSend {
			values = append(values, createTimeSeries(labels, prevMetric))
		}
	} else {
		currentValue = value
		shouldSend = false // First value, don't send yet
	}

	// Always store the current value
	p.collector.Store(labelsKey, metricValue{
		value:             currentValue,
		timestamp:         timestamp,
		lastUpdateTime:    now,
		lastValueIsSended: shouldSend,
	})

	return values
}

func (p *metricCollector) flushOldMetrics() {
	for {
		select {
		case <-p.flushTicker.C:
			var toSend []promwrite.TimeSeries
			now := time.Now()

			p.collector.Range(func(key, value interface{}) bool {
				metric := value.(metricValue)
				if now.Sub(metric.lastUpdateTime) > p.timeout && !metric.lastValueIsSended {
					labels := keyToLabels(key.(string))
					toSend = append(toSend, createTimeSeries(labels, metric))

					metric.lastValueIsSended = true
					p.collector.Store(key, metric)
				}
				return true
			})

			if len(toSend) > 0 {
				// Send these metrics to your storage
				err := p.sender.sendToStorage(toSend)
				if err != nil {
					p.logger.Error("can't send data", zap.Error(err))
				}
			}

		case <-p.shutdownChan:
			p.flushTicker.Stop()
			return
		}
	}
}

func (p *metricCollector) shutdown() {
	close(p.shutdownChan)
	// Flush all remaining metrics
	var toSend []promwrite.TimeSeries
	p.collector.Range(func(key, value interface{}) bool {
		metric := value.(metricValue)
		labels := keyToLabels(key.(string))
		toSend = append(toSend, createTimeSeries(labels, metric))
		return true
	})
	p.sender.sendToStorage(toSend)
}

// Helper function
func createTimeSeries(labels []promwrite.Label, metric metricValue) promwrite.TimeSeries {
	return promwrite.TimeSeries{
		Labels: labels,
		Sample: promwrite.Sample{
			Time:  time.Unix(0, metric.timestamp),
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
