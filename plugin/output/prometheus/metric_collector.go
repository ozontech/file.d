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
	existing, exists := p.metrics[key]

	if exists {
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

	if exists {
		metric.sendedTimestamp = existing.sendedTimestamp
		metric.lastValueIsSended = existing.lastValueIsSended
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
		}
		if now.After(metric.expiredAt) {
			toDelete = append(toDelete, key)
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

func escapeCommas(s string) string {
	return strings.ReplaceAll(s, ",", `\,`)
}

func unescapeCommas(s string) string {
	return strings.ReplaceAll(s, `\,`, ",")
}

func keyToLabels(key string) []promwrite.Label {
	if key == "" {
		return nil
	}
	// Count how many labels to know which is the last
	numLabels := strings.Count(key, ",") + 1

	labels := make([]promwrite.Label, 0, numLabels)

	for {
		// Find the first = to get name
		equalsIdx := strings.Index(key, "=")
		if equalsIdx == -1 {
			break
		}
		name := key[:equalsIdx]
		key = key[equalsIdx+1:]

		if len(labels)+1 == numLabels {
			// This is the last label - the rest is the value (minus trailing comma)
			if key != "" && key[len(key)-1] == ',' {
				key = key[:len(key)-1]
			}
			labels = append(labels, promwrite.Label{Name: name, Value: unescapeCommas(key)})
			break
		}

		// Not the last label - find unescaped comma
		commaIdx := -1
		for i := 0; i < len(key); i++ {
			if key[i] == ',' && (i == 0 || key[i-1] != '\\') {
				commaIdx = i
				break
			}
		}
		if commaIdx == -1 {
			break
		}
		value := key[:commaIdx]
		labels = append(labels, promwrite.Label{Name: name, Value: unescapeCommas(value)})
		key = key[commaIdx+1:]
	}
	return labels
}

func labelsToKey(labels []promwrite.Label) string {
	// Fast path: if labels are already sorted or only 1-2 labels, skip sorting
	if len(labels) <= 1 {
		if len(labels) == 0 {
			return ""
		}
		return labels[0].Name + "=" + escapeCommas(labels[0].Value) + ","
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
		b.WriteString(escapeCommas(l.Value))
		b.WriteByte(',')
	}
	return b.String()
}
