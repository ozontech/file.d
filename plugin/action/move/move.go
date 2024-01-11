package move

import (
	"errors"
	"fmt"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	insaneJSON "github.com/vitkovskii/insane-json"
)

/*{ introduction
It moves fields to the target field in a certain mode.
> In `allow` mode, the specified `fields` will be moved;
> in `block` mode, the unspecified `fields` will be moved.

### Examples
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: move
      mode: allow
      target: other
      fields:
        - log.stream
        - zone
    ...
```
The original event:
```json
{
  "service": "test",
  "log": {
    "level": "error",
    "message": "error occurred",
    "ts": "2023-10-30T13:35:33.638720813Z",
    "stream": "stderr"
  },
  "zone": "z501"
}
```
The resulting event:
```json
{
  "service": "test",
  "log": {
    "level": "error",
    "message": "error occurred",
    "ts": "2023-10-30T13:35:33.638720813Z"
  },
  "other": {
    "stream": "stderr",
    "zone": "z501"
  }
}
```
---
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: move
      mode: block
      target: other
      fields:
        - log
    ...
```
The original event:
```json
{
  "service": "test",
  "log": {
    "level": "error",
    "message": "error occurred",
    "ts": "2023-10-30T13:35:33.638720813Z",
    "stream": "stderr"
  },
  "zone": "z501",
  "other": {
    "user": "ivanivanov"
  }
}
```
The resulting event:
```json
{
  "log": {
    "level": "error",
    "message": "error occurred",
    "ts": "2023-10-30T13:35:33.638720813Z"
  },
  "other": {
    "user": "ivanivanov",
    "service": "test",
    "zone": "z501"
  }
}
```
}*/

type Plugin struct {
	config *Config
	fields map[string][]string
}

const (
	modeAllow string = "allow"
	modeBlock string = "block"
)

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The list of the fields to move.
	// >> In `block` mode, the maximum `fields` depth is 1.
	Fields []cfg.FieldSelector `json:"fields" slice:"true" required:"true"` // *

	// > @3@4@5@6
	// >
	// > The mode of the moving. Available modes are one of: `allow|block`.
	Mode string `json:"mode" required:"true"` // *

	// > @3@4@5@6
	// >
	// > The target field of the moving.
	// >> If the `target` field is existing non-object field, it will be overwritten as object field.
	// >
	// >> In `block` mode, the maximum `target` depth is 1.
	Target  cfg.FieldSelector `json:"target" parse:"selector" required:"true"` // *
	Target_ []string
}

func (c *Config) validate() error {
	if !(c.Mode == modeAllow || c.Mode == modeBlock) {
		return fmt.Errorf("invalid mode %q", c.Mode)
	}
	if c.Mode == modeBlock && len(c.Target_) > 1 {
		return errors.New(`in "block" mode, the maximum "target" depth is 1`)
	}
	return nil
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "move",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
	if err := p.config.validate(); err != nil {
		params.Logger.Fatalf("invalid config: %s", err.Error())
	}

	p.fields = make(map[string][]string)
	isBlockMode := p.config.Mode == modeBlock
	for _, fs := range p.config.Fields {
		// in `block` mode, max field depth is 1
		if f := cfg.ParseFieldSelector(string(fs)); len(f) > 0 && (!isBlockMode || len(f) == 1) {
			p.fields[f[len(f)-1]] = f
		}
	}
}

func (p *Plugin) Stop() {}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	targetNode := pipeline.CreateNestedField(event.Root, p.config.Target_)

	moveNode := func(name string, node *insaneJSON.Node) {
		node.Suicide()
		targetNode.AddFieldNoAlloc(event.Root, name).MutateToNode(node)
	}

	if p.config.Mode == modeAllow {
		for name, field := range p.fields {
			if node := event.Root.Dig(field...); node != nil && node != targetNode {
				moveNode(name, node)
			}
		}
	} else {
		for _, node := range event.Root.AsFields() {
			value := node.AsFieldValue()
			if value == targetNode {
				continue
			}

			name := node.AsString()
			if _, ok := p.fields[name]; !ok {
				moveNode(name, value)
			}
		}
	}

	return pipeline.ActionPass
}
