package remove_fields

import (
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/logger"
	"github.com/ozontech/file.d/pipeline"
)

/*{ introduction
It removes the list of the event fields and keeps others.
Nested fields supported: list subfield names separated with dot.
Example:
```
fields: ["a.b.c"]

# event before processing
{
  "a": {
    "b": {
      "c": 100,
      "d": "some"
    }
  }
}

# event after processing
{
  "a": {
    "b": {
      "d": "some" # "c" removed
    }
  }
}
```

If field name contains dots use backslash for escaping.
Example:
```
fields:
  - exception\.type

# event before processing
{
  "message": "Exception occurred",
  "exception.type": "SomeType"
}

# event after processing
{
  "message": "Exception occurred" # "exception.type" removed
}
```
}*/

type Plugin struct {
	config     *Config
	fieldPaths [][]string
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The list of the fields to remove.
	Fields []string `json:"fields"` // *
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "remove_fields",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, _ *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
	if p.config == nil {
		logger.Panicf("config is nil for the remove fields plugin")
	}

	var err error
	p.fieldPaths, err = cfg.ParseNestedFields(p.config.Fields)
	if err != nil {
		logger.Fatalf("can't parse nested fields: %s", err.Error())
	}
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	if !event.Root.IsObject() {
		return pipeline.ActionPass
	}

	for _, fieldPath := range p.fieldPaths {
		event.Root.Dig(fieldPath...).Suicide()
	}

	return pipeline.ActionPass
}
