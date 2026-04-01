# Event to metrics plugin
Get metric from event

This plugin transforms incoming events into metric data. Each event can generate one or more metrics with configurable labels and values. Using the Prometheus output plugin, you can send the generated metrics to Prometheus.

## Examples
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
                service: service
                environment: environment
    output:
      plugin: prometheus
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

## Config params
**`time_field`** *`cfg.FieldSelector`* *`default=time`* 

The event field which defines the time when event was fired.
It is used to detect the event throughput in a particular time range.
If not set, the current time will be taken.

<br>

**`time_field_format`** *`string`* *`default=rfc3339nano`* 

It defines how to parse the time field format. Can be specified as a datetime layout in Go [time.Parse](https://pkg.go.dev/time#Parse) format or by alias.
List of available datetime format aliases can be found [here](/pipeline/README.md#datetime-parse-formats).

<br>

**`metrics`** *`[]Metric`* *`required`* 

List of metrics.

<br>

**`name`** *`string`* *`required`* 

Name of metric

<br>

**`type`** *`string`* *`required`* *`options=counter|gauge`* 

The type of metric. Supported types: `counter`, `gauge`.

<br>

**`value`** *`[]cfg.FieldSelector`* 

Field selector(s) to extract the metric value from the event. If not specified or empty, the value defaults to 1 (useful for counters).

<br>

**`labels`** *`map[string]string`* 

Labels are key-value pairs that provide context for the metric.

<br>

**`ttl`** *`cfg.Duration`* 

Time-to-live for the metric. Defines how long the metric value should be kept in the Prometheus collector before being expired. This determines the retention period for the metric in Prometheus.

<br>

**`do_if`** *`map[string]any`* 

Condition to check before emitting the metric.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*