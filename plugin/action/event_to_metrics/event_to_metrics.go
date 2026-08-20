package event_to_metrics

import (
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/pipeline/doif"
	"github.com/ozontech/file.d/xtime"

	"go.uber.org/zap"
)

/*{ introduction
Get metric from event

This plugin transforms incoming events into metric data. Each event can generate one or more metrics with configurable labels and values. Using the Prometheus output plugin, you can send the generated metrics to Prometheus.

**Important:** This action should be the last one in the pipeline, as it consumes events and does not pass them further.
}*/

/*{ examples
**Example 1: Simple counter**

```yaml
pipelines:
  example:
    actions:
      - type: event_to_metrics
        metrics:
          - name: events_total
            type: counter
            value: []
            ttl: 60s
            labels:
              service: service
              environment: environment
    output:
      type: prometheus
```

Input event:
```json
{"time": "2024-01-15T10:30:00Z", "message": "request processed", "service": "app", "environment": "production"}
```

Generated metric:
```json
{
  "name": "events_total",
  "type": "counter",
  "value": 1,
  "timestamp": 1705315800000,
  "labels": {
    "service": "app",
    "environment": "production"
  },
  "ttl": 60000
}
```

**Example 2: Gauge with dynamic labels and value extraction**

```yaml
pipelines:
  example:
    actions:
      - type: event_to_metrics
        time_field: timestamp
        time_field_format: rfc3339
        metrics:
          - name: response_time_ms
            type: gauge
            value:
              - response.duration_ms
            ttl: 5m
            labels:
              method: request.method
              endpoint: request.path
              status: response.status_code
          - name: request_size_bytes
            type: gauge
            value:
              - request.size
            ttl: 5m
            labels:
              method: request.method
    output:
      type: prometheus
```

Input event:
```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "request": {
    "method": "GET",
    "path": "/api/users",
    "size": 256
  },
  "response": {
    "status_code": "200",
    "duration_ms": 42.5
  }
}
```

Generated metrics:
```json
{
  "name": "response_time_ms",
  "type": "gauge",
  "value": 42.5,
  "timestamp": 1705315800000,
  "labels": {
    "method": "GET",
    "endpoint": "/api/users",
    "status": "200"
  },
  "ttl": 300000
}
{
  "name": "request_size_bytes",
  "type": "gauge",
  "value": 256,
  "timestamp": 1705315800000,
  "labels": {
    "method": "GET"
  },
  "ttl": 300000
}
```

**Example 3: Using with Prometheus output plugin**

This sample demonstrates how to use `event_to_metrics` with the Prometheus output plugin to send metrics to a Prometheus server via remote write.

```yaml
pipelines:
	metrics:
	    actions:
	    - type: event_to_metrics
        time_field: timestamp
        time_field_format: rfc3339
        metrics:
            # Counter for total requests
            - name: http_requests_total
            type: counter
            value: []
            ttl: 60s
            labels:
                service: api
                method: request.method
                path: request.path
                status: response.status_code

            # Gauge for response time
            - name: http_response_time_ms
            type: gauge
            value:
                - response.duration_ms
            ttl: 5m
            labels:
                service: api
                method: request.method
                path: request.path

	    output:
		    type: prometheus
		    config:
		        # Prometheus remote write endpoint
		        endpoint: http://localhost:9090/api/v1/write
```

Input event:
```json
{
    "timestamp": "2024-01-15T10:30:00Z",
    "request": {
        "method": "GET",
        "path": "/api/users/123"
    },
    "response": {
        "status_code": "200",
        "duration_ms": 45.3
    }
}
```

The plugin will generate the following metrics that get sent to Prometheus:

1. **http_requests_total** - Counter incremented by 1
    - Labels: `service="api"`, `method="GET"`, `path="/api/users/123"`, `status="200"`

2. **http_response_time_ms** - Gauge set to 45.3
    - Labels: `service="api"`, `method="GET"`, `path="/api/users/123"`

These metrics are then sent to Prometheus via the remote write API, where they can be queried and visualized in Grafana or other tools.
}*/

type Plugin struct {
	config           *Config
	logger           *zap.Logger
	pluginController pipeline.ActionPluginController
	format           string

	Metrics       []Metric
	metricIndices []int

	metricDataList []metricData
}

type metricData struct {
	name       []byte
	metricType []byte
	ttl        int64
	timestamp  int64
	value      float64
	labels     map[string]string
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The event field which defines the time when event was fired.
	// > It is used to detect the event throughput in a particular time range.
	// > If not set, the current time will be taken.
	TimeField  cfg.FieldSelector `json:"time_field" default:"time" parse:"selector"` // *
	TimeField_ []string

	// > @3@4@5@6
	// >
	// > It defines how to parse the time field format. Can be specified as a datetime layout in Go [time.Parse](https://pkg.go.dev/time#Parse) format or by alias.
	// > List of available datetime format aliases can be found [here](/pipeline/README.md#datetime-parse-formats).
	TimeFieldFormat string `json:"time_field_format" default:"rfc3339nano"` // *

	// > @3@4@5@6
	// >
	// > List of metrics.
	Metrics []Metric `json:"metrics" slice:"true" required:"true"` // *
}

type Metric struct {
	// > @3@4@5@6
	// >
	// > Name of metric
	Name string `json:"name" required:"true"` // *

	// > @3@4@5@6
	// >
	// > The type of metric. Supported types: `counter`, `gauge`.
	Type string `json:"type" options:"counter|gauge" required:"true"` // *

	// > @3@4@5@6
	// >
	// > Field selector(s) to extract the metric value from the event. If not specified or empty, the value defaults to 1 (useful for counters).
	Value       []cfg.FieldSelector `json:"value"` // *
	valueFields [][]string

	// > @3@4@5@6
	// >
	// > Labels are key-value pairs that provide context for the metric.
	Labels      map[string]cfg.FieldSelector `json:"labels"` // *
	labelFields map[string][]string

	// > @3@4@5@6
	// >
	// > Time-to-live for the metric. Defines how long the metric value should be kept in the Prometheus collector before being expired. This determines the retention period for the metric in Prometheus.
	TTL  cfg.Duration `json:"ttl" parse:"duration" default:"0"` // *
	TTL_ time.Duration

	// > @3@4@5@6
	// >
	// > Condition to check before emitting the metric.
	DoIfCheckerMap map[string]any `json:"do_if"` // *

	DoIfChecker *doif.Checker
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "event_to_metrics",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
	p.logger = params.Logger.Desugar()
	p.pluginController = params.Controller

	p.Metrics = prepareCheckersForMetrics(p.config.Metrics, p.logger)
	p.metricIndices = make([]int, 0, len(p.Metrics))
	p.metricDataList = make([]metricData, 0, len(p.Metrics))
	for i := range p.Metrics {
		metric := p.Metrics[i]
		p.metricDataList = append(p.metricDataList, metricData{
			name:       pipeline.StringToByteUnsafe(metric.Name),
			metricType: pipeline.StringToByteUnsafe(metric.Type),
			ttl:        metric.TTL_.Milliseconds(),
		})
	}

	format, err := xtime.ParseFormatName(p.config.TimeFieldFormat)
	if err != nil {
		format = p.config.TimeFieldFormat
	}
	p.format = format
}

func prepareCheckersForMetrics(configMetrics []Metric, logger *zap.Logger) []Metric {
	// Copy to avoid mutating the original config,
	// which is shared across all processor instances.
	metrics := make([]Metric, len(configMetrics))
	copy(metrics, configMetrics)

	for i := range metrics {
		m := &metrics[i]
		if m.DoIfCheckerMap != nil {
			var err error
			m.DoIfChecker, err = doif.NewFromMap(m.DoIfCheckerMap)
			if err != nil {
				logger.Fatal(
					"can't init do_if for metric",
					zap.Error(err),
					zap.String("metric_name", m.Name),
				)
			}
		}

		m.valueFields = make([][]string, 0, len(m.Value))
		for _, fs := range m.Value {
			if fs == "" {
				continue
			}
			m.valueFields = append(m.valueFields, cfg.ParseFieldSelector(string(fs)))
		}

		if m.Labels != nil {
			m.labelFields = make(map[string][]string, len(m.Labels))
			for labelName, fs := range m.Labels {
				if fs != "" {
					m.labelFields[labelName] = cfg.ParseFieldSelector(string(fs))
				}
			}
		}
	}

	return metrics
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	p.metricIndices = p.metricIndices[:0]
	for i := range p.Metrics {
		if p.Metrics[i].DoIfChecker == nil || p.Metrics[i].DoIfChecker.Check(doif.NewEventData(event.Root)) {
			p.metricIndices = append(p.metricIndices, i)
		}
	}

	if len(p.metricIndices) == 0 {
		return pipeline.ActionDiscard
	}

	var timestamp time.Time

	if len(p.config.TimeField_) != 0 {
		tsValue := event.Root.Dig(p.config.TimeField_...).AsString()
		t, err := xtime.ParseTime(p.format, tsValue)
		if err != nil || t.IsZero() {
			p.logger.Warn(
				"can't parse field with timestamp using format",
				zap.Any("time_field", p.config.TimeField),
				zap.String("TimeFieldFormat", p.config.TimeFieldFormat),
				zap.String("value", tsValue),
			)
			timestamp = xtime.GetInaccurateTime()
		} else {
			timestamp = t
		}
	} else {
		timestamp = xtime.GetInaccurateTime()
	}

	var timestampMs int64 = timestamp.UnixMilli()

	for _, metricIdx := range p.metricIndices {
		metric := &p.Metrics[metricIdx]

		var value float64 = 1
		if len(metric.valueFields) > 0 {
			value = 0
			for _, fieldPath := range metric.valueFields {
				node := event.Root.Dig(fieldPath...)
				if node == nil {
					continue
				}
				if !node.IsNumber() {
					p.logger.Warn("non-numeric metric field", zap.Any("path", fieldPath))
					continue
				}
				value += node.AsFloat()
			}
		}

		labels := make(map[string]string)
		for labelName, fieldPath := range metric.labelFields {
			labels[labelName] = event.Root.Dig(fieldPath...).AsString()
		}
		metricData := &p.metricDataList[metricIdx]
		metricData.timestamp = timestampMs
		metricData.value = value
		metricData.labels = labels
	}

	metricsArray := event.Root.MutateToArray()
	for _, metricIdx := range p.metricIndices {
		metricData := p.metricDataList[metricIdx]

		object := metricsArray.AddElement().MutateToObject()

		object.AddField("name").MutateToBytes(metricData.name)
		object.AddField("type").MutateToBytes(metricData.metricType)
		object.AddField("ttl").MutateToInt64(metricData.ttl)
		object.AddField("timestamp").MutateToInt64(metricData.timestamp)
		object.AddField("value").MutateToFloat(metricData.value)

		if len(metricData.labels) > 0 {
			labelsObject := object.AddField("labels").MutateToObject()
			for labelName, value := range metricData.labels {
				labelsObject.AddField(labelName).MutateToBytesCopy(event.Root, []byte(value))
			}
		}
	}

	return pipeline.ActionPass
}
