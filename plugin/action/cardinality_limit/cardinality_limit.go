package discard

import (
	"sort"
	"strings"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type Plugin struct {
	cache  *Cache
	config *Config
	keys   []string
	fields []string
	logger *zap.Logger

	cardinalityDiscardCounter *prometheus.CounterVec
}

const (
	actionDiscard      = "discard"
	actionRemoveFields = "remove_fields"
)

type Config struct {
	KeyFields []cfg.FieldSelector `json:"key" slice:"true" required:"true"`
	Fields    []cfg.FieldSelector `json:"fields" slice:"true" required:"true"`
	Action    string              `json:"action" default:"discard" options:"discard|remove_fields"`
	Limit     int                 `json:"limit" default:"10000"`
	TTL       cfg.Duration        `json:"ttl" default:"1h" parse:"duration"` // *
	TTL_      time.Duration
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "cardinality_limit",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) makeMetric(ctl *metric.Ctl, name, help string, labels ...string) *prometheus.CounterVec {
	if name == "" {
		return nil
	}

	uniq := make(map[string]struct{})
	labelNames := make([]string, 0, len(labels))
	for _, label := range labels {
		if label == "" {
			p.logger.Fatal("empty label name")
		}
		if _, ok := uniq[label]; ok {
			p.logger.Fatal("metric labels must be unique")
		}
		uniq[label] = struct{}{}

		labelNames = append(labelNames, label)
	}

	return ctl.RegisterCounterVec(name, help, labelNames...)
}

func (p *Plugin) registerMetrics(ctl *metric.Ctl) {
	p.cardinalityDiscardCounter = p.makeMetric(ctl,
		"cardinality_discard_total",
		"Total number of events discarded due to cardinality limits",
		p.keys...,
	)
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
	p.logger = params.Logger.Desugar()

	var err error
	p.cache, err = NewCache(p.config.TTL_)
	if err != nil {
		panic(err)
	}

	p.keys = make([]string, 0, len(p.config.KeyFields))
	for _, fs := range p.config.KeyFields {
		if fs != "" {
			p.keys = append(p.keys, cfg.ParseFieldSelector(string(fs))[0])
		}
	}

	p.fields = make([]string, 0, len(p.config.Fields))
	for _, fs := range p.config.Fields {
		if fs != "" {
			p.fields = append(p.fields, cfg.ParseFieldSelector(string(fs))[0])
		}
	}

	p.registerMetrics(params.MetricCtl)
}

func (p *Plugin) Stop() {

}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	cacheKey := make(map[string]string, len(p.keys))

	for _, key := range p.keys {
		value := pipeline.CloneString(event.Root.Dig(key).AsString())
		cacheKey[key] = value
	}

	cacheValue := make(map[string]string, len(p.fields))
	for _, key := range p.fields {
		value := pipeline.CloneString(event.Root.Dig(key).AsString())
		cacheValue[key] = value
	}

	key := mapToKey(cacheKey)
	value := mapToStringSorted(cacheKey, cacheValue)
	keysCount := p.cache.CountPrefix(key)

	if p.config.Limit > 0 && keysCount > p.config.Limit {
		labelsValues := make([]string, 0, len(p.keys))
		for _, key := range p.keys {
			if val, exists := cacheKey[key]; exists {
				labelsValues = append(labelsValues, val)
			} else {
				labelsValues = append(labelsValues, "unknown")
			}
		}
		p.cardinalityDiscardCounter.WithLabelValues(labelsValues...).Inc()
		switch p.config.Action {
		case actionDiscard:
			return pipeline.ActionDiscard
		case actionRemoveFields:
			for _, key := range p.fields {
				event.Root.Dig(key).Suicide()
			}
		}
	} else {
		p.cache.Set(value)
	}
	return pipeline.ActionPass
}

func mapToStringSorted(m, n map[string]string) string {
	var sb strings.Builder
	sb.WriteString(mapToKey(m))

	keys := make([]string, 0, len(n))
	for k := range n {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	sb.WriteString("map[")
	for i, k := range keys {
		if i > 0 {
			sb.WriteString(" ")
		}
		sb.WriteString(k + ":" + n[k])
	}
	sb.WriteString("]")
	return sb.String()
}

func mapToKey(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	sb.WriteString("map[")
	for i, k := range keys {
		if i > 0 {
			sb.WriteString(" ")
		}
		sb.WriteString(k + ":" + m[k])
	}
	sb.WriteString("];")
	return sb.String()
}
