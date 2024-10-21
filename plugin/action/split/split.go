package split

import (
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	insaneJSON "github.com/ozontech/insane-json"
	"go.uber.org/zap"
)

/*{ introduction
It splits array of objects into different events.

For example:
```json
{
	"data": [
		{ "message": "go" },
		{ "message": "rust" },
		{ "message": "c++" }
	]
}
```

Split produces:
```json
{ "message": "go" },
{ "message": "rust" },
{ "message": "c++" }
```

Parent event will be discarded.
If the value of the JSON field is not an array of objects, then the event will be pass unchanged.
}*/

type Plugin struct {
	config           *Config
	logger           *zap.Logger
	pluginController pipeline.ActionPluginController
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > Path to the array of objects.
	Field  cfg.FieldSelector `json:"field" parse:"selector"` // *
	Field_ []string
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "split",
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
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	if event.IsChildKind() {
		return pipeline.ActionPass
	}

	data := event.Root.Dig(p.config.Field_...)
	if data == nil {
		return pipeline.ActionPass
	}

	if !data.IsArray() {
		p.logger.Warn("skip an event because field is not an array", zap.String("type", data.TypeStr()))
		return pipeline.ActionPass
	}

	nodeArray := data.AsArray()
	children := make([]*insaneJSON.Node, 0, len(nodeArray))
	for _, elem := range nodeArray {
		if !elem.IsObject() {
			p.logger.Warn("skip an event because it is not an object", zap.String("type", data.TypeStr()))
			continue
		}
		children = append(children, elem)
	}

	if len(children) == 0 {
		// zero array or an array that does not contain objects
		return pipeline.ActionPass
	}

	p.pluginController.Spawn(event, children)

	return pipeline.ActionBreak
}
