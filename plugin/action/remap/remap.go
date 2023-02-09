package remap

import (
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/plugin"
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

Remap produces:
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
	plugin.NoMetricsPlugin
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
		Type:    "remap",
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
	node := event.Root.Dig(p.config.Field_...)
	if !node.IsArray() {
		p.logger.Warn("skip an event because is not an array", zap.String("type", node.TypeStr()))
		return pipeline.ActionPass
	}

	var anyObject bool
	for _, elem := range node.AsArray() {
		if !elem.IsObject() {
			p.logger.Warn("skip an event because %s is not an object", zap.String("type", node.TypeStr()))
			continue
		}
		anyObject = true
		p.pluginController.Spawn(event, elem)
	}

	if anyObject {
		// discard event if the array contains one or more objects
		return pipeline.ActionDiscard
	}

	// zero array or an array that does not contain objects
	return pipeline.ActionPass
}
