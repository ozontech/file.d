package parse_http

import (
	"encoding/base64"
	"net/url"
	"strings"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

/*{ introduction
It adds a field containing the source name to the event and extracts remote_ip and auth login.
It is only applicable for input plugin http.
You need add action add_file_name before it.

**Example:**
Service for receiving events from a static page:
```yaml
pipelines:
  example_http_pipeline:
    input:
      # define input type.
      type: http
      # define http port.
      address: ":9400"
      auth:
        strategy: basic
        secrets:
          frontend: 398fc645-e660-45f4-96bb-53b7a2b120e4
      cors:
          allowed_headers:
            - DNT
            - X-CustomHeader
            - Keep-Alive
            - User-Agent
            - X-Requested-With
            - If-Modified-Since
            - Cache-Control
            - Content-Type
            - Authorization
          allowed_origins:
            - http://localhost:8090
    actions:
    - type: add_file_name
      field: source_name
      # parse http info
    - type: parse_http
      field: source_name
      allowed_params:
        - env
    - type: convert_log_level
      field: level
      style: number
      default_level: info
      remove_on_fail: true
    - type: mask
      metric_name: errors_total
      metric_skip_status: true
      metric_labels:
        - login
        - level
    - type: remove_fields
      fields:
        - source_name
    output:
      type: stdout
      # Or we can write to file:
      # type: file
      # target_file: "./output.txt"
```

Setup:
```bash
# run server.
# config.yaml should contains yaml config above.
go run ./cmd/file.d --config=config.yaml

# now do requests.
curl "127.0.0.1:9400/?env=cli" -H 'Content-Type: application/json' -H 'Authorization: Basic ZnJvbnRlbmQ6Mzk4ZmM2NDUtZTY2MC00NWY0LTk2YmItNTNiN2EyYjEyMGU0' -d \
'{"message": "Test event", "level": "info"}'

# run nginx with static page
docker run -p 8090:80 -v `pwd`/plugin/action/parse_http:/usr/share/nginx/html -it --rm --name my-static-html-server nginx:alpine

# open http://localhost:8090 and click "Send Log Request" button

# check metric
curl localhost:9000/metrics | grep "file_d_pipeline_example_http_pipeline_errors_total_events_count_total"
```
}*/

type Plugin struct {
	logger                *zap.SugaredLogger
	config                *Config
	allowedParams         map[string]struct{}
	eventsProcessedMetric *prometheus.CounterVec
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The event field to which put the source name. Must be a string.
	// >
	Field  cfg.FieldSelector `json:"field" parse:"selector" required:"false" default:"source_name"` // *
	Field_ []string

	// > @3@4@5@6
	// >
	// > List of the allowed GET params.
	AllowedParams []string `json:"allowed_params"` // *
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "parse_http",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
	p.logger = params.Logger

	if len(p.config.AllowedParams) > 0 {
		p.allowedParams = make(map[string]struct{}, len(p.config.AllowedParams))
		for _, field := range p.config.AllowedParams {
			p.allowedParams[field] = struct{}{}
		}
	}

	p.registerMetrics(params.MetricCtl)
}

func (p *Plugin) registerMetrics(ctl *metric.Ctl) {
	p.eventsProcessedMetric = ctl.RegisterCounterVec("events_processed_total", "How many events processed", "login")
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	node := event.Root.Dig(p.config.Field_...)
	sourceName := node.AsString()

	sourceInfo := strings.Split(sourceName, "_")

	if sourceInfo[0] != "http" {
		p.logger.Error(
			"Wrong format. Do you have http input plugin or add_file_name action? Maybe wrong field? You got: ",
			zap.String("sourceName", sourceName),
		)
	}

	infoStr, _ := base64.StdEncoding.DecodeString(sourceInfo[1])
	info := strings.Split(string(infoStr), "_")
	login := info[0]
	rawQuery, _ := base64.StdEncoding.DecodeString(info[2])

	params, _ := url.ParseQuery(string(rawQuery))
	if v, exists := params["login"]; exists {
		login = v[0]
	}

	pipeline.CreateNestedField(event.Root, []string{"login"}).MutateToString(login)
	pipeline.CreateNestedField(event.Root, []string{"remote_ip"}).MutateToString(info[1])

	p.eventsProcessedMetric.With(prometheus.Labels{"login": login}).Inc()

	for k, v := range params {
		_, paramIsAllowed := p.allowedParams[k]
		if paramIsAllowed {
			pipeline.CreateNestedField(event.Root, []string{k}).MutateToString(v[0])
		}
	}

	return pipeline.ActionPass
}
