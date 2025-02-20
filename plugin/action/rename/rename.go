package rename

import (
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
)

/*{ introduction
It renames the fields of the event. You can provide an unlimited number of config parameters. Each parameter handled as `cfg.FieldSelector`:`string`.
When `override` is set to `false`, the field won't be renamed in the case of field name collision.
Sequence of rename operations isn't guaranteed. Use different actions for prioritization.

**Note**: if the renamed field name starts with underscore "_", it should be escaped with preceding underscore. E.g.
if the renamed field is "_HOSTNAME", in config it should be "___HOSTNAME". Only one preceding underscore is needed.
Renamed field names with only one underscore in config are considered as without preceding underscore:
if there is "_HOSTNAME" in config the plugin searches for "HOSTNAME" field.

**Example common:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: rename
      override: false
      my_object.field.subfield: new_sub_field
    ...
```

Input event:

```json
{
  "my_object": {
    "field": {
      "subfield":"value"
    }
  }
}
```

Output event:

```json
{
  "my_object": {
    "field": {
      "new_sub_field":"value"  // renamed
    }
  }
}
```

**Example journalctl:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: rename
      override: false
      __HOSTNAME: host
      ___REALTIME_TIMESTAMP: ts
    ...
```

Input event:

```json
{
  "_HOSTNAME": "example-host",
  "__REALTIME_TIMESTAMP": "1739797379239590"
}
```

Output event:

```json
{
  "host": "example-host",  // renamed
  "ts": "1739797379239590"  // renamed
}
```
}*/

const (
	overrideKey = "override"
)

type Plugin struct {
	paths          [][]string
	names          []string
	preserveFields bool
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "rename",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, _ *pipeline.ActionPluginParams) {
	sharedConfig := *config.(*Config)
	conf := sharedConfig.Clone() // clone shared config to be able to modify it

	val, idx := conf.Find(overrideKey)

	p.preserveFields = idx == NotFoundIdx || val == "false"

	conf.Remove(overrideKey)
	conf = unescapeMap(conf)

	conf.ForEach(func(path string, name string) {
		selector := cfg.ParseFieldSelector(path)
		p.paths = append(p.paths, selector)
		p.names = append(p.names, name)
	})
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	for index, path := range p.paths {
		if p.preserveFields {
			if event.Root.Dig(p.names[index]) != nil {
				continue
			}
		}

		node := event.Root.Dig(path...)
		if node == nil {
			continue
		}

		node.Suicide()
		event.Root.AddFieldNoAlloc(event.Root, p.names[index]).MutateToNode(node)
	}

	return pipeline.ActionPass
}

func unescapeMap(fields Config) Config {
	newConfig := make(Config, 0, len(fields))
	fields.ForEach(func(key string, value string) {
		if key == "" {
			return
		}

		if key[0] == '_' {
			key = key[1:]
		}
		newConfig.Append(key, value)
	})
	return newConfig
}
