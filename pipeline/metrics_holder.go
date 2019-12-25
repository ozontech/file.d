package pipeline

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type metricsHolder struct {
	pipelineName   string
	metricsGen     int // generation is used to drop unused metrics from counters
	metricsGenTime time.Time
	actionMetrics  []*actionMetrics
	registry       *prometheus.Registry
}

func newMetricsHolder(pipelineName string, registry *prometheus.Registry) *metricsHolder {
	return &metricsHolder{
		pipelineName:  pipelineName,
		registry:      registry,
		actionMetrics: make([]*actionMetrics, 0, 0),
	}

}

func (m *metricsHolder) AddAction(metricName string, metricLabels []string) {
	m.actionMetrics = append(m.actionMetrics, &actionMetrics{
		name:        metricName,
		labels:      metricLabels,
		counter:     nil,
		counterPrev: nil,
		mu:          &sync.RWMutex{},
	})
}

func (m *metricsHolder) start() {
	m.nextMetricsGen()
}


func (m *metricsHolder) nextMetricsGen() {
	metricsGen := strconv.Itoa(m.metricsGen)

	for index, metrics := range m.actionMetrics {
		if metrics.name == "" {
			continue
		}

		opts := prometheus.CounterOpts{
			Namespace:   "filed",
			Subsystem:   "pipeline_" + m.pipelineName,
			Name:        metrics.name + "_events_total",
			Help:        fmt.Sprintf("how many events processed by pipeline %q and #%d action", m.pipelineName, index),
			ConstLabels: map[string]string{"gen": metricsGen},
		}
		counter := prometheus.NewCounterVec(opts, append([]string{"status"}, metrics.labels...))
		prev := metrics.counterPrev

		metrics.mu.Lock()
		metrics.counterPrev = metrics.counter
		metrics.counter = counter
		metrics.mu.Unlock()

		m.registry.MustRegister(counter)
		if prev != nil {
			m.registry.Unregister(prev)
		}
	}

	m.metricsGen++
	m.metricsGenTime = time.Now()
}

func (m *metricsHolder) countEvent(event *Event, actionIndex int, eventStatus eventStatus, valuesBuf []string) []string {
	if len(m.actionMetrics) == 0 {
		return valuesBuf
	}

	metrics := m.actionMetrics[actionIndex]
	if metrics.name == "" {
		return valuesBuf
	}

	valuesBuf = valuesBuf[:0]
	valuesBuf = append(valuesBuf, string(eventStatus))

	for _, field := range metrics.labels {
		node := event.Root.Dig(field)

		if node == nil {
			valuesBuf = append(valuesBuf, defaultFieldValue)
		} else {
			valuesBuf = append(valuesBuf, node.AsString())
		}
	}

	metrics.mu.RLock()
	metrics.counter.WithLabelValues(valuesBuf...).Inc()
	metrics.mu.RUnlock()

	return valuesBuf
}

func (m *metricsHolder) maintenance() {
	if time.Now().Sub(m.metricsGenTime) < metricsGenInterval {
		return
	}

	m.nextMetricsGen()
}
