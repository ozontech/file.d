package pipeline

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type metricsHolder struct {
	pipelineName       string
	metricsGen         int // generation is used to drop unused metrics from counters
	metricsGenTime     time.Time
	metricsGenInterval time.Duration
	metrics            []*metrics
	registry           *prometheus.Registry
}

type metrics struct {
	name   string
	labels []string

	root *mNode

	current  *prometheus.CounterVec
	previous *prometheus.CounterVec
}

type mNode struct {
	childs map[string]*mNode
	mu     *sync.RWMutex
	self   string
}

func newMetricsHolder(pipelineName string, registry *prometheus.Registry, metricsGenInterval time.Duration) *metricsHolder {
	return &metricsHolder{
		pipelineName: pipelineName,
		registry:     registry,

		metrics:            make([]*metrics, 0, 0),
		metricsGenInterval: metricsGenInterval,
	}

}

func (m *metricsHolder) AddAction(metricName string, metricLabels []string) {
	m.metrics = append(m.metrics, &metrics{
		name:   metricName,
		labels: metricLabels,
		root: &mNode{
			childs: make(map[string]*mNode),
			mu:     &sync.RWMutex{},
		},
		current:  nil,
		previous: nil,
	})
}

func (m *metricsHolder) start() {
	m.nextMetricsGen()
}

func (m *metricsHolder) nextMetricsGen() {
	metricsGen := strconv.Itoa(m.metricsGen)

	for index, metrics := range m.metrics {
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
		obsolete := metrics.previous

		metrics.previous = metrics.current
		metrics.current = counter

		m.registry.MustRegister(counter)
		if obsolete != nil {
			m.registry.Unregister(obsolete)
		}
	}

	m.metricsGen++
	m.metricsGenTime = time.Now()
}

func (m *metricsHolder) count(event *Event, actionIndex int, eventStatus eventStatus, valuesBuf []string) []string {
	if len(m.metrics) == 0 {
		return valuesBuf
	}

	metrics := m.metrics[actionIndex]
	if metrics.name == "" {
		return valuesBuf
	}

	valuesBuf = valuesBuf[:0]
	valuesBuf = append(valuesBuf, string(eventStatus))

	mn := metrics.root
	for _, field := range metrics.labels {
		val := DefaultFieldValue

		node := event.Root.Dig(field)
		if node != nil {
			val = node.AsString()
		}

		mn.mu.RLock()
		nextMN, has := mn.childs[val]
		mn.mu.RUnlock()

		if !has {
			mn.mu.Lock()
			nextMN, has = mn.childs[val];
			if !has {
				key := DefaultFieldValue
				if node != nil {
					key = string(node.AsBytes()) // make string from []byte to make map string keys works good
				}

				nextMN = &mNode{
					childs: make(map[string]*mNode),
					self:   key,
					mu:     &sync.RWMutex{},
				}
				mn.childs[key] = nextMN
			}
			mn.mu.Unlock()
		}

		valuesBuf = append(valuesBuf, nextMN.self)
		mn = nextMN
	}

	metrics.current.WithLabelValues(valuesBuf...).Inc()

	return valuesBuf
}

func (m *metricsHolder) maintenance() {
	if time.Now().Sub(m.metricsGenTime) < metricsGenInterval {
		return
	}

	m.nextMetricsGen()
}
