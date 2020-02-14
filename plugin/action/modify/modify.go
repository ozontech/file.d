package modify

import (
	"github.com/ozonru/file.d/cfg"
	"github.com/ozonru/file.d/fd"
	"github.com/ozonru/file.d/pipeline"
	"go.uber.org/zap"
)

/*{ introduction
Modifies content for a field. Works only with strings.
There can be provided unlimited config parameters. Each parameter handled as `cfg.FieldSelector`:`cfg.Substitution`.

Example:
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: modify
      my_object.field.subfield: value is ${another_object.value}.
    ...
```

Result event could looks like:
```
{
  "my_object": {
    "field": {
      "subfield":"value is 666."
    }
  },
  "another_object": {
    "value": 666
  }
```
}*/
type Plugin struct {
	config *Config
	logger *zap.SugaredLogger
	ops    map[string][]cfg.SubstitutionOp
	buf    []byte
}

type Config map[string]string

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "modify",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
	p.ops = make(map[string][]cfg.SubstitutionOp)
	p.logger = params.Logger

	for key, value := range *p.config {
		ops, err := cfg.ParseSubstitution(value)
		if err != nil {
			p.logger.Fatalf("can't parse substitution: %s", err.Error())
		}

		if len(ops) == 0 {
			continue
		}

		p.ops[key] = ops
	}
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	for field, list := range p.ops {
		p.buf = p.buf[:0]
		for _, op := range list {
			switch op.Kind {
			case cfg.SubstitutionOpKindRaw:
				p.buf = append(p.buf, op.Data[0]...)
			case cfg.SubstitutionOpKindField:
				p.buf = append(p.buf, event.Root.Dig(op.Data...).AsBytes()...)
			default:
				p.logger.Panicf("unknown substitution kind %d", op.Kind)
			}
		}

		event.Root.AddFieldNoAlloc(event.Root, field).MutateToBytesCopy(event.Root, p.buf)
	}

	return pipeline.ActionPass
}
