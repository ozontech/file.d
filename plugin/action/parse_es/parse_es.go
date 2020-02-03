package parse_es

import (
	"gitlab.ozon.ru/sre/file-d/fd"
	"gitlab.ozon.ru/sre/file-d/logger"
	"gitlab.ozon.ru/sre/file-d/pipeline"
)

/*{ introduction
Plugin parses HTTP input using Elasticsearch /_bulk API format: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
It converts sources defining by create/index actions to the events. Update/delete actions are ignored.
}*/
type Plugin struct {
	config      *Config
	passNext    bool
	discardNext bool
}

type Config struct {
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "parse_es",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(_ pipeline.AnyConfig, _ *pipeline.ActionPluginParams) {

}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	if event.IsTimeoutKind() {
		logger.Errorf("timeout while parsing elasticsearch event stream")
		return pipeline.ActionDiscard
	}

	root := event.Root

	if p.passNext && p.discardNext {
		logger.Panicf("wrong state")
	}

	if p.passNext {
		p.passNext = false
		return pipeline.ActionPass
	}

	if p.discardNext {
		p.discardNext = false
		return pipeline.ActionCollapse
	}

	if root.Dig("delete") != nil {
		return pipeline.ActionCollapse
	}

	if root.Dig("update") != nil {
		p.discardNext = true
		return pipeline.ActionCollapse
	}

	if root.Dig("index") != nil {
		p.passNext = true
		return pipeline.ActionCollapse
	}

	if root.Dig("create") != nil {
		p.passNext = true
		return pipeline.ActionCollapse
	}

	logger.Fatalf("wrong ES input format, expected action, got: %s", root.EncodeToString())

	return pipeline.ActionDiscard
}
