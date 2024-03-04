package add_file_name

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
It adds a field containing the file name to the event.
It is only applicable for input plugins k8s and file.
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
	// > The event field to which put the file name. Must be a string.
	// >
	// > Warn: it overrides fields if it contains non-object type on the path. For example:
	// > if `field` is `info.level` and input
	// > `{ "info": [{"userId":"12345"}] }`,
	// > output will be: `{ "info": {"level": <level>} }`
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
