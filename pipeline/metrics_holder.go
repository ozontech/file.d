package pipeline

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
)

type metricsHolder struct {
	pipelineName       string
	metricsGen         int // generation is used to drop unused metrics from counters
	metricsGenTime     time.Time
	metricsGenInterval time.Duration
	metrics            []*metrics
	registry           *prometheus.Registry
}

type counter struct {
	count        *prometheus.CounterVec
	totalCounter *atomic.Uint64
	size         *prometheus.CounterVec
}

type metrics struct {
	name   string
	labels []string

	root *mNode

	current  counter
	previous counter
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

		metrics:            make([]*metrics, 0),
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
		current:  counter{nil, atomic.NewUint64(0), nil},
		previous: counter{nil, atomic.NewUint64(0), nil},
	})
}

func (m *metricsHolder) start() {
	m.nextMetricsGen()
}

func (c *counter) register(registry *prometheus.Registry) {
	registry.MustRegister(c.count)
	registry.MustRegister(c.size)
}

func (c *counter) unregister(registry *prometheus.Registry) {
	registry.Unregister(c.count)
	registry.Unregister(c.size)
}

func (m *metricsHolder) nextMetricsGen() {
	metricsGen := strconv.Itoa(m.metricsGen)
	for index, metrics := range m.metrics {
		if metrics.name == "" {
			continue
		}

		cnt := counter{nil, atomic.NewUint64(0), nil}
		opts := prometheus.CounterOpts{
			Namespace:   "file_d",
			Subsystem:   "pipeline_" + m.pipelineName,
			Name:        metrics.name + "_events_count_total",
			Help:        fmt.Sprintf("how many events processed by pipeline %q and #%d action", m.pipelineName, index),
			ConstLabels: map[string]string{"gen": metricsGen},
		}
		cnt.count = prometheus.NewCounterVec(opts, append([]string{"status"}, metrics.labels...))

		opts = prometheus.CounterOpts{
			Namespace:   "file_d",
			Subsystem:   "pipeline_" + m.pipelineName,
			Name:        metrics.name + "_events_size_total",
			Help:        fmt.Sprintf("total size of events processed by pipeline %q and #%d action", m.pipelineName, index),
			ConstLabels: map[string]string{"gen": metricsGen},
		}
		cnt.size = prometheus.NewCounterVec(opts, append([]string{"status"}, metrics.labels...))

		obsolete := metrics.previous

		metrics.previous = metrics.current
		metrics.current = cnt

		metrics.current.register(m.registry)
		if obsolete.count != nil {
			obsolete.unregister(m.registry)
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
			nextMN, has = mn.childs[val]
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

	metrics.current.count.WithLabelValues(valuesBuf...).Inc()
	metrics.current.totalCounter.Inc()
	metrics.current.size.WithLabelValues(valuesBuf...).Add(float64(event.Size))

	return valuesBuf
}

func (m *metricsHolder) maintenance() {
	if time.Since(m.metricsGenTime) < metricsGenInterval {
		return
	}

	m.nextMetricsGen()
}
