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
	collector *sync.Map

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
		collector:    new(sync.Map),
		flushTicker:  time.NewTicker(10 * time.Second),
		repeatTicker: time.NewTicker(1 * time.Minute),
		shutdownChan: make(chan struct{}),
		sender:       sender,
		logger:       logger,
	}
	go c.flushAndRepeatOldMetrics()
	return c
}

var i int

func (p *metricCollector) handleMetric(labels []promwrite.Label, value float64, timestamp int64, metricType string, ttl int64) {
	labelsKey := labelsToKey(labels)
	now := time.Now()

	newMetric := func() metricValue {
		if prev, ok := p.collector.Load(labelsKey); ok {
			prevMetric := prev.(metricValue)
			currentValue := value

			if metricType == "counter" {
				// For counters, incoming values should be increments
				currentValue += prevMetric.value
			}

			return metricValue{
				value:           currentValue,
				timestamp:       max(timestamp, prevMetric.timestamp),
				lastUpdateTime:  now,
				expiredAt:       now.Add(time.Duration(ttl) * time.Millisecond),
				sendedTimestamp: prevMetric.sendedTimestamp,
			}
		}

		return metricValue{
			value:           value,
			timestamp:       timestamp,
			lastUpdateTime:  now,
			expiredAt:       now.Add(time.Duration(ttl) * time.Millisecond),
			sendedTimestamp: time.Time{}, // Zero time for new metrics
		}
	}

	p.collector.Store(labelsKey, newMetric())
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
	var toSend []promwrite.TimeSeries
	now := time.Now()
	nowUnixtime := now.UnixMilli()

	sendEvery := 1 * time.Minute
	freshDuration := 3 * time.Minute

	toDelete := []string{}

	p.collector.Range(func(key, value interface{}) bool {
		metric := value.(metricValue)
		labels := keyToLabels(key.(string))

		lastUpdateSeconds := now.Sub(metric.lastUpdateTime)
		lastSendedSeconds := now.Sub(metric.sendedTimestamp)
		isFresh := lastUpdateSeconds < freshDuration

		if isFresh {
			timeSeries := createTimeSeries(labels, metric)
			if timeSeries.Sample.Time.After(metric.sendedTimestamp) {
				toSend = append(toSend, timeSeries)
				metric.sendedTimestamp = timeSeries.Sample.Time
				p.collector.Store(key, metric)
			}
		} else if lastSendedSeconds > sendEvery {
			if now.After(metric.expiredAt) {
				toDelete = append(toDelete, key.(string))
			}
			metric.timestamp = nowUnixtime
			timeSeries := createTimeSeries(labels, metric)
			if timeSeries.Sample.Time.After(metric.sendedTimestamp) {
				toSend = append(toSend, timeSeries)
				metric.sendedTimestamp = timeSeries.Sample.Time
				p.collector.Store(key, metric)
			}
		}
		return true
	})

	if len(toDelete) > 0 {
		for _, key := range toDelete {
			p.collector.Delete(key)
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
			Time:  time.Unix(0, metric.timestamp*int64(time.Millisecond)).Truncate(30 * time.Second),
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
