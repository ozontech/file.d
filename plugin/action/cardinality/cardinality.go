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

	"github.com/elliotchance/orderedmap/v2"
	"go.uber.org/zap"
)

/*{ introduction
Limits the cardinality of fields on events or drops events.

**An example for discarding events with high cardinality field:**
```yaml
pipelines:
  example_pipeline:
    ...
    - type: cardinality
      limit: 10
      action: discard
      metric_prefix: service_zone
      key:
        - service
      fields:
        - zone
    ...
```
}*/

type Plugin struct {
	cache  *Cache
	config *Config
	keys   []parsedField
	fields []parsedField
	logger *zap.Logger

	cardinalityApplyCounter *prometheus.CounterVec
}

type parsedField struct {
	name  string
	value []string
	// value *orderedmap.NewOrderedMap[string, any]
}

const (
	actionDiscard      = "discard"
	actionRemoveFields = "remove_fields"
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
		p.logger.Fatal("you have to set key fields")
	}

	p.keys = parseFields(p.config.KeyFields)
	p.fields = parseFields(p.config.Fields)

	p.registerMetrics(params.MetricCtl, p.config.MetricPrefix)
}

func parseFields(fields []cfg.FieldSelector) []parsedField {
	res := make([]parsedField, 0, len(fields))
	for _, fs := range fields {
		if fs == "" {
			continue
		}
		parsed := cfg.ParseFieldSelector(string(fs))
		res = append(res, parsedField{
			name:  strings.Join(parsed, "_"),
			value: parsed,
		})
	}
	return res
}

func (p *Plugin) registerMetrics(ctl *metric.Ctl, prefix string) {
	var metricName string
	if prefix == "" {
		metricName = "cardinality_applied_total"
	} else {
		metricName = fmt.Sprintf(`cardinality_applied_%s_total`, prefix)
	}
	p.cardinalityApplyCounter = ctl.RegisterCounterVec(
		metricName,
		"Total number of events applied due to cardinality limits",
		keyMetricLabels(p.keys)...,
	)
}

func keyMetricLabels(fields []parsedField) []string {
	result := make([]string, 0, len(fields))
	seen := make(map[string]bool, len(fields))

	for i := range fields {
		name := fields[i].name
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
	cacheKey := orderedmap.NewOrderedMap[string, string]()
	for _, key := range p.keys {
		value := pipeline.CloneString(event.Root.Dig(key.value...).AsString())
		cacheKey.Set(key.name, value)
	}

	prefixKey := mapToKey(cacheKey)
	keysCount := p.cache.CountPrefix(prefixKey)

	if p.config.Limit >= 0 && keysCount >= p.config.Limit {
		labelsValues := getLabelsValues(len(p.keys))
		defer labelsValuesPool.Put(labelsValues)

		for _, key := range p.keys {
			if val, exists := cacheKey.Get(key.name); exists {
				labelsValues = append(labelsValues, val)
			}
		}
		p.cardinalityApplyCounter.WithLabelValues(labelsValues...).Inc()
		switch p.config.Action {
		case actionDiscard:
			return pipeline.ActionDiscard
		case actionRemoveFields:
			for _, key := range p.fields {
				event.Root.Dig(key.value...).Suicide()
			}
		}
	} else {
		cacheValue := orderedmap.NewOrderedMap[string, string]()
		for _, key := range p.fields {
			value := pipeline.CloneString(event.Root.Dig(key.value...).AsString())
			cacheValue.Set(key.name, value)
		}
		value := mapToStringSorted(cacheKey, cacheValue)
		p.cache.Set(value)
	}
	return pipeline.ActionPass
}

var labelsValuesPool = sync.Pool{
	New: func() any {
		return make([]string, 0, 5)
	},
}

func getLabelsValues(minCapacity int) []string {
	slice := labelsValuesPool.Get().([]string)

	if cap(slice) < minCapacity {
		labelsValuesPool.Put(slice)
		return make([]string, 0, minCapacity)
	}

	return slice[:0]
}

var builderPool = sync.Pool{
	New: func() any {
		return &strings.Builder{}
	},
}

func mapToStringSorted(cacheKey, cacheValue *orderedmap.OrderedMap[string, string]) string {
	sb := builderPool.Get().(*strings.Builder)
	defer func() {
		sb.Reset()
		builderPool.Put(sb)
	}()

	estimatedSize := (cacheKey.Len()+cacheValue.Len())*20 + 10
	sb.Grow(estimatedSize)

	sb.WriteString(mapToKey(cacheKey))
	sb.WriteString(mapToKey(cacheValue))
	out := sb.String()
	return out
}

func mapToKey(m *orderedmap.OrderedMap[string, string]) string {
	sb := new(strings.Builder)

	sb.Grow(m.Len() * 15)
	sb.WriteString("map[")

	for el := m.Front(); el != nil; el = el.Next() {
		sb.WriteString(el.Key)
		sb.WriteByte(':')
		sb.WriteString(el.Value)
		sb.WriteByte(' ')
	}
	sb.WriteString("];")

	out := sb.String()
	return out
}
