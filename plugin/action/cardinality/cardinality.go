package cardinality

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/prometheus/client_golang/prometheus"

	"go.uber.org/zap"
)

/*{ introduction
Limits the cardinality of fields on events, drops events or just do nothing.
}*/

/*{ examples
Discarding events with high cardinality field:
```yaml
pipelines:
  example_pipeline:
    ...
    - type: cardinality
      limit: 2
      action: discard
      ttl: 1m
      metric_prefix: service_client
      key:
        - service
      fields:
        - client_id
    ...
```
Events:
```json
{"service": "registration", "client_id": "1"}
{"service": "registration", "client_id": "1"}
{"service": "registration", "client_id": "2"}
{"service": "registration", "client_id": "3"} // will be discarded
```
---

Discarding events with high cardinality field:
```yaml
pipelines:
  example_pipeline:
    ...
    - type: cardinality
      limit: 2
      action: remove_fields
      ttl: 1m
      metric_prefix: service_client
      key:
        - service
      fields:
        - client_id
    ...
```
The original events:
```json
{"service": "registration", "client_id": "1"}
{"service": "registration", "client_id": "2"}
{"service": "registration", "client_id": "3"}
```
The resulting events:
```json
{"service": "registration", "client_id": "1"}
{"service": "registration", "client_id": "2"}
{"service": "registration"}
```
}*/

type Plugin struct {
	cache        *Cache
	config       *Config
	keys         *parsedFields
	fields       *parsedFields
	labelsValues []string
	logger       *zap.Logger
	bufPool      sync.Pool

	cardinalityUniqueValuesLimit prometheus.Gauge
	cardinalityUniqueValuesGauge *prometheus.GaugeVec
}

type parsedField struct {
	name  string
	value []string
}

type parsedFields struct {
	fields  []parsedField
	valsBuf []string
}

func (f *parsedFields) appendTo(buf []byte) []byte {
	if len(f.fields) == 0 {
		return append(buf, '[', ']')
	}

	buf = append(buf, '[')

	for i := range f.fields {
		if i > 0 {
			buf = append(buf, ' ')
		}
		buf = append(buf, f.fields[i].name...)
		buf = append(buf, ':')
		buf = append(buf, f.valsBuf[i]...)
	}

	buf = append(buf, ']')
	return buf
}

const (
	actionDiscard      = "discard"
	actionRemoveFields = "remove_fields"
	actionNothing      = "nothing"
)

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > Fields used to group events before calculating cardinality.
	// > Events with the same key values are aggregated together.
	// > Required for proper cardinality tracking per logical group.
	KeyFields []cfg.FieldSelector `json:"key" slice:"true" required:"true"` // *

	// > @3@4@5@6
	// >
	// > Target fields whose unique values are counted within each key group.
	// > The plugin monitors how many distinct values these fields contain.
	// > Required to define what constitutes high cardinality.
	Fields []cfg.FieldSelector `json:"fields" slice:"true" required:"true"` // *

	// > @3@4@5@6
	// >
	// > Action to perform when cardinality limit is exceeded.
	// > Determines whether to discard events, remove fields, or just monitor.
	// > Choose based on whether you need to preserve other event data.
	Action string `json:"action" default:"nothing" options:"discard|remove_fields|nothing"` // *

	// > @3@4@5@6
	// >
	// > Prefix added to metric names for better organization.
	// > Useful when running multiple instances to avoid metric name collisions.
	// > Leave empty for default metric naming.
	MetricPrefix string `json:"metric_prefix" default:""` // *

	// > @3@4@5@6
	// >
	// > Maximum allowed number of unique values for monitored fields.
	// > When exceeded within a key group, the configured action triggers.
	// > Set based on expected diversity and system capacity.
	Limit int `json:"limit" default:"10000"` // *

	// > @3@4@5@6
	// >
	// > Time-to-live for cardinality tracking cache entries.
	// > Prevents unbounded memory growth by forgetting old unique values.
	// > Should align with typical patterns of field value changes.
	TTL  cfg.Duration `json:"ttl" default:"1h" parse:"duration"` // *
	TTL_ time.Duration
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "cardinality",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
	p.logger = params.Logger.Desugar()

	p.cache = NewCache(p.config.TTL_)

	if len(p.config.Fields) == 0 {
		p.logger.Fatal("you have to set fields")
	}
	p.bufPool = sync.Pool{
		New: func() any {
			return make([]byte, 0, 256)
		},
	}

	p.keys = parseFields(p.config.KeyFields)
	p.fields = parseFields(p.config.Fields)
	p.labelsValues = make([]string, len(p.config.KeyFields))

	p.registerMetrics(params.MetricCtl, p.config.MetricPrefix)
}

func parseFields(fields []cfg.FieldSelector) *parsedFields {
	f := make([]parsedField, 0, len(fields))
	for _, fs := range fields {
		if fs == "" {
			continue
		}
		parsed := cfg.ParseFieldSelector(string(fs))
		f = append(f, parsedField{
			name:  strings.Join(parsed, "_"),
			value: parsed,
		})
	}
	return &parsedFields{
		fields:  f,
		valsBuf: make([]string, len(f)),
	}
}

func (p *Plugin) registerMetrics(ctl *metric.Ctl, prefix string) {
	var metricName string
	if prefix == "" {
		metricName = "cardinality_unique_values_count"
	} else {
		metricName = fmt.Sprintf(`cardinality_unique_values_%s_count`, prefix)
	}
	p.cardinalityUniqueValuesGauge = ctl.RegisterGaugeVec(
		metricName,
		"Count of unique values",
		keyMetricLabels(p.keys)...,
	)

	if prefix == "" {
		metricName = "cardinality_values_limit"
	} else {
		metricName = fmt.Sprintf(`cardinality_values_%s_limit`, prefix)
	}
	p.cardinalityUniqueValuesLimit = ctl.RegisterGauge(
		metricName,
		"Limit of unique values",
	)
	p.cardinalityUniqueValuesLimit.Set(float64(p.config.Limit))
}

func keyMetricLabels(fields *parsedFields) []string {
	result := make([]string, 0, len(fields.fields))
	seen := make(map[string]bool, len(fields.fields))

	for i := range fields.fields {
		name := fields.fields[i].name
		if !seen[name] {
			seen[name] = true
			result = append(result, name)
		}
	}
	return result
}

func (p *Plugin) Stop() {

}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	for i, key := range p.keys.fields {
		value := event.Root.Dig(key.value...).AsString()
		p.keys.valsBuf[i] = value
	}
	buf := p.bufPool.Get().([]byte)
	buf = buf[:0]
	defer p.bufPool.Put(buf)

	prefixKey := p.keys.appendTo(buf)
	keysCount := p.cache.CountPrefix(string(prefixKey))

	shouldUpdateCache := false

	if p.config.Limit >= 0 && keysCount >= p.config.Limit {
		switch p.config.Action {
		case actionDiscard:
			return pipeline.ActionDiscard
		case actionRemoveFields:
			for _, key := range p.fields.fields {
				event.Root.Dig(key.value...).Suicide()
			}
		case actionNothing:
			shouldUpdateCache = true
		}
	} else {
		shouldUpdateCache = true
	}

	if shouldUpdateCache {
		for i, key := range p.fields.fields {
			value := event.Root.Dig(key.value...).AsString()
			p.fields.valsBuf[i] = value
		}

		isOldValue := p.cache.Set(string(p.fields.appendTo(prefixKey)))
		if !isOldValue {
			// is new value
			keysCount++
			for i, _ := range p.keys.fields {
				p.labelsValues[i] = p.keys.valsBuf[i]
			}
			p.cardinalityUniqueValuesGauge.WithLabelValues(p.labelsValues...).Set(float64(keysCount))
		}
	}

	return pipeline.ActionPass
}
