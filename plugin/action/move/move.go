package move

import (
	"slices"

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
	fields [][]string

	invalidConfig bool
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
	// >> In `block` mode, the maximum `target` depth is 1.
	Target  cfg.FieldSelector `json:"target" parse:"selector" required:"true"` // *
	Target_ []string
}

func (c *Config) isValid() bool {
	if !(c.Mode == modeAllow || c.Mode == modeBlock) {
		return false
	}
	if c.Mode == modeBlock && len(c.Target_) > 1 {
		return false
	}
	return true
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

func (p *Plugin) Start(config pipeline.AnyConfig, _ *pipeline.ActionPluginParams) {
	p.config = config.(*Config)

	p.invalidConfig = !p.config.isValid()
	if p.invalidConfig {
		return
	}

	isBlockMode := p.config.Mode == modeBlock
	p.fields = make([][]string, 0, len(p.config.Fields))
	for _, fs := range p.config.Fields {
		// in `block` mode, max field depth is 1
		if f := cfg.ParseFieldSelector(string(fs)); len(f) > 0 && (!isBlockMode || len(f) == 1) {
			p.fields = append(p.fields, f)
		}
	}
}

func (p *Plugin) Stop() {}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	if p.invalidConfig {
		return pipeline.ActionPass
	}

	targetNode := event.Root.Node
	continueDig := true
	for _, tField := range p.config.Target_ {
		if !continueDig {
			targetNode = targetNode.AddFieldNoAlloc(event.Root, tField).MutateToObject()
			continue
		}

		node := targetNode.Dig(tField)
		if node == nil {
			node = targetNode.AddFieldNoAlloc(event.Root, tField).MutateToObject()
			continueDig = false
		} else if !node.IsObject() {
			return pipeline.ActionPass
		}
		targetNode = node
	}

	moveNode := func(name string, node *insaneJSON.Node) {
		node.Suicide()
		targetNode.AddFieldNoAlloc(event.Root, name).MutateToNode(node)
	}

	if p.config.Mode == modeAllow {
		for _, field := range p.fields {
			if node := event.Root.Dig(field...); node != nil && node != targetNode {
				moveNode(field[len(field)-1], node)
			}
		}
	} else {
		for _, node := range event.Root.AsFields() {
			value := node.AsFieldValue()
			if value == targetNode {
				continue
			}

			name := node.AsString()
			if slices.IndexFunc(p.fields, func(f []string) bool { return f[0] == name }) == -1 {
				moveNode(name, value)
			}
		}
	}

	return pipeline.ActionPass
}
