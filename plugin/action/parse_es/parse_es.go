package parse_es

import (
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"go.uber.org/zap"
)

/*{ introduction
It parses HTTP input using Elasticsearch `/_bulk` API format. It converts sources defining create/index actions to the events. Update/delete actions are ignored.
> Check out the details in [Elastic Bulk API](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html).
}*/
type Plugin struct {
	logger      *zap.SugaredLogger
	passNext    bool
	discardNext bool
	isStrict    bool
}

type Config struct{}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "parse_es",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(_ pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.logger = params.Logger
	p.isStrict = params.PipelineSettings.IsStrict
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	if event.IsTimeoutKind() {
		p.logger.Errorf("timeout while parsing elasticsearch event stream")
		return pipeline.ActionDiscard
	}

	root := event.Root

	if p.passNext && p.discardNext {
		p.logger.Panicf("wrong state")
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

	// If request invalid skip bad event.
	p.logger.Error("wrong ES input format, expected action, got: %s", root.EncodeToString())

	return pipeline.ActionDiscard
}
