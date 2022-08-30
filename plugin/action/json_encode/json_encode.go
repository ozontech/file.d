package json_encode

import (
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
)

/*{ introduction
It replaces field with its JSON string representation.

**Example:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: json_encode
      field: server
    ...
```
It transforms `{"server":{"os":"linux","arch":"amd64"}}` into `{"server":"{\"os\":\"linux\",\"arch\":\"amd64\"}"}`.

}*/

type Plugin struct {
	config *Config
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The event field to encode. Must be a string.
	Field  cfg.FieldSelector `json:"field" parse:"selector" required:"true"` // *
	Field_ []string
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "json_encode",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, _ *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	node := event.Root.Dig(p.config.Field_...)
	if node == nil {
		return pipeline.ActionPass
	}

	s := len(event.Buf)
	event.Buf = node.Encode(event.Buf)

	node.MutateToString(pipeline.ByteToStringUnsafe(event.Buf[s:]))
	return pipeline.ActionPass
}

func (p *Plugin) RegisterMetrics(ctl *metric.Ctl) {
}
