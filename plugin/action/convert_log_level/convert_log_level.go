package convert_log_level

import (
	"strings"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/metric"
	"github.com/ozontech/file.d/pipeline"
	"go.uber.org/zap"
)

const (
	emptyLevelStr = ""
)

/*{ introduction
It converts the log level field according RFC-5424.
}*/

type Plugin struct {
	config *Config
	logger *zap.SugaredLogger
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The name of the event field to convert.
	// > The value of the field will be converted to lower case and trimmed for parsing.
	// >
	// > Warn: it overrides fields if it contains non-object type on the path. For example:
	// > if `field` is `info.level` and input
	// > `{ "info": [{"userId":"12345"}] }`,
	// > output will be: `{ "info": {"level": <level>} }`
	Field  cfg.FieldSelector `json:"field" parse:"selector" required:"false" default:"level"` // *
	Field_ []string

	// > @3@4@5@6
	// >
	// > Style format to convert. Must be one of number or string.
	// > Available RFC-5424 levels:
	// > <ul>
	// > <li>0: emergency</li>
	// > <li>1: alert </li>
	// > <li>2: critical </li>
	// > <li>3: error </li>
	// > <li>4: warning </li>
	// > <li>5: notice </li>
	// > <li>6: informational </li>
	// > <li>7: debug </li>
	// > </ul>
	Style string `json:"style" default:"number" options:"number|string"` // *

	// > @3@4@5@6
	// >
	// > The default log level if the field cannot be parsed. If empty, no default level will be set.
	// >
	// > Also it uses if field contains non-object type. For example:
	// > if `default_level` is `informational` and input:
	// > `{"level":[5]}`
	// > the output will be: `{"level":"informational"}`
	DefaultLevel string `json:"default_level" default:""` // *

	// > @3@4@5@6
	// >
	// > Remove field if conversion fails.
	// > This can happen when the level is unknown. For example:
	// > `{ "level": "my_error_level" }`
	RemoveOnFail bool `json:"remove_on_fail" default:"false"` // *
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "convert_log_level",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
	p.logger = params.Logger

	p.config.Style = strings.ToLower(strings.TrimSpace(p.config.Style))
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	node := event.Root.Dig(p.config.Field_...)
	if node == nil {
		// pass action if node does not exist and default level is not set
		if p.config.DefaultLevel == emptyLevelStr {
			return pipeline.ActionPass
		}

		// create field with default level
		node = pipeline.CreateNestedField(event.Root, p.config.Field_)
		node.MutateToString(p.config.DefaultLevel)
	}

	level := node.AsString()
	if level == emptyLevelStr && p.config.DefaultLevel != emptyLevelStr {
		level = p.config.DefaultLevel
	}

	var fail bool
	if p.config.Style == "string" {
		parsedLevel := pipeline.ParseLevelAsString(level)
		fail = parsedLevel == pipeline.LevelUnknownStr
		if !fail {
			node.MutateToString(parsedLevel)
		}
	} else {
		parsedLevel := pipeline.ParseLevelAsNumber(level)
		fail = parsedLevel == pipeline.LevelUnknown
		if !fail {
			node.MutateToInt(int(parsedLevel))
		}
	}

	if fail && p.config.RemoveOnFail {
		node.Suicide()
	}

	return pipeline.ActionPass
}

func (p *Plugin) RegisterMetrics(ctl *metric.Ctl) {
}
