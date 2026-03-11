package event_to_metrics

import (
	"maps"
	"sync"
	"time"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/pipeline/doif"
	"github.com/ozontech/file.d/xtime"
	insaneJSON "github.com/ozontech/insane-json"
	"go.uber.org/zap"
)

/*{ introduction
Get metric from event

This plugin transforms incoming events into metric data. Each event can generate one or more metrics with configurable labels and values. Using the Prometheus output plugin, you can send the generated metrics to Prometheus.
}*/

/*{ examples
**Example 1: Simple counter**

```yaml
pipelines:
  example:
    actions:
      - type: event_to_metrics
        config:
          metrics:
            - name: events_total
              type: counter
              value: []
              ttl: 60s
              labels:
                service: app
                environment: production
    output:
      plugin: prometheus
```

Input event:
```json
{"time": "2024-01-15T10:30:00Z", "message": "request processed"}
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
        config:
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
      plugin: prometheus
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
	        config:
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
		    plugin: prometheus
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

	Metrics []Metric
	mu      *sync.Mutex
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
	valueFields []string

	// > @3@4@5@6
	// >
	// > Labels are key-value pairs that provide context for the metric.
	Labels map[string]string `json:"labels"` // *

	// > @3@4@5@6
	// >
	// > Time-to-live for the metric. Defines how long the metric value should be kept in the Prometheus collector before being expired. This determines the retention period for the metric in Prometheus.
	TTL  cfg.Duration `json:"ttl" parse:"duration"` // *
	TTL_ time.Duration

	// > @3@4@5@6
	// >
	// > Condition to check before emitting the metric.
	DoIfCheckerMap map[string]any `json:"do_if"` // *

	DoIfChecker *doif.Checker

	use bool
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
	p.mu = &sync.Mutex{}
	p.config = config.(*Config)
	p.logger = params.Logger.Desugar()
	p.pluginController = params.Controller
	p.Metrics = prepareCheckersForMetrics(p.config.Metrics, p.logger)

	format, err := xtime.ParseFormatName(p.config.TimeFieldFormat)
	if err != nil {
		format = p.config.TimeFieldFormat
	}
	p.format = format
}

func prepareCheckersForMetrics(metrics []Metric, logger *zap.Logger) []Metric {
	for i := range metrics {
		m := &metrics[i]
		if m.DoIfCheckerMap != nil {
			var err error
			m.DoIfChecker, err = doif.NewFromMap(m.DoIfCheckerMap)
			if err != nil {
				logger.Fatal("can't init do_if for mask", zap.Error(err))
			}
		} else {
			m.use = true
		}

		fields := make([]string, 0, len(m.Value))
		for _, fs := range m.Value {
			if fs == "" {
				continue
			}
			parsed := cfg.ParseFieldSelector(string(fs))
			fields = append(fields, parsed...)
		}
		m.valueFields = fields
	}

	return metrics
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	p.mu.Lock()
	copyMetrics := make([]Metric, 0, len(p.Metrics))
	for i := range p.Metrics {
		if p.Metrics[i].DoIfChecker == nil {
			copyMetrics = append(copyMetrics, p.Metrics[i])
		} else {
			if !p.config.Metrics[i].DoIfChecker.Check(event.Root) {
				continue
			}
			copyMetrics = append(copyMetrics, p.Metrics[i])
		}
		if p.Metrics[i].Labels != nil {
			copyMetrics[len(copyMetrics)-1].Labels = make(map[string]string, len(p.Metrics[i].Labels))
			maps.Copy(copyMetrics[len(copyMetrics)-1].Labels, p.Metrics[i].Labels)
		}
	}
	p.mu.Unlock()

	var ts time.Time

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
			ts = time.Now()
		} else {
			ts = t
		}
	} else {
		ts = time.Now()
	}

	children := make([]*insaneJSON.Node, 0, len(copyMetrics))
	for _, metric := range copyMetrics {
		elem := new(insaneJSON.Node)
		object := elem.MutateToObject()

		object.AddField("name").MutateToBytes([]byte(metric.Name))
		object.AddField("type").MutateToBytes([]byte(metric.Type))
		object.AddField("ttl").MutateToInt64(metric.TTL_.Milliseconds())
		object.AddField("timestamp").MutateToInt64(ts.UnixMilli())

		if len(metric.Value) == 0 {
			object.AddField("value").MutateToInt(1)
		} else {
			valueNode := event.Root.Dig(metric.valueFields...).AsFloat()
			object.AddField("value").MutateToFloat(valueNode)
		}

		if len(metric.Labels) > 0 {
			labelsObject := object.AddField("labels").MutateToObject()

			for labelName, labelValue := range metric.Labels {
				node := event.Root.Dig(labelValue)
				value := node.AsString()
				labelsObject.AddField(labelName).MutateToBytes([]byte(value))
			}
		}

		children = append(children, elem)
	}

	if len(children) == 0 {
		// zero array or an array that does not contain objects
		return pipeline.ActionDiscard
	}

	p.pluginController.Spawn(event, children)
	return pipeline.ActionBreak
}
