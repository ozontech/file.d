package convert_log_level

import (
	"strings"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"go.uber.org/zap"
)

/*{ introduction
It converts the log level field according RFC-5424.
}*/
type Plugin struct {
	config *Config
	logger *zap.SugaredLogger
}

//! config-params
//^ config-params
type Config struct {
	//> @3@4@5@6
	//>
	//> The name of the event field to convert.
	//> The value of the field will be converted to lower case and trimmed for parsing.
	Field  cfg.FieldSelector `json:"field" parse:"selector" required:"false" default:"level"` //*
	Field_ []string

	//> @3@4@5@6
	//>
	//> Style format to convert. Must be one of number or string.
	//> Available RFC-5424 levels:
	//> <ul>
	//> <li>0: emergency</li>
	//> <li>1: alert </li>
	//> <li>2: critical </li>
	//> <li>3: error </li>
	//> <li>4: warning </li>
	//> <li>5: notice </li>
	//> <li>6: informational </li>
	//> </ul>
	Style string `json:"style" default:"number" options:"number|string"` //*

	//> @3@4@5@6
	//>
	//> The default log level if the field cannot be parsed. If empty, no default level will be set.
	DefaultLevel string `json:"default_level" default:""` //*

	//> @3@4@5@6
	//>
	//> Remove field if conversion fails.
	RemoveOnFail bool `json:"remove_on_fail" default:"false"` //*
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "convert_date",
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
		if p.config.DefaultLevel == "" {
			return pipeline.ActionPass
		}

		// create field with default level
		var err error
		node, err = pipeline.CreateNestedField(event.Root, p.config.Field_)
		if err != nil {
			p.logger.Warn("can't create nested field: %s", err.Error())
			return pipeline.ActionPass
		}
		node.MutateToString(p.config.DefaultLevel)
	}

	if !node.IsString() && !node.IsNumber() {
		p.logger.Warn("can't override field with value=%s", node.EncodeToString())
		return pipeline.ActionPass
	}

	level := node.AsString()
	if level == "" && p.config.DefaultLevel != "" {
		level = p.config.DefaultLevel
	}

	var fail bool
	if p.config.Style == "string" {
		parsedLevel := pipeline.ParseLevelAsString(level)
		if parsedLevel == "" {
			fail = true
		} else {
			node.MutateToString(parsedLevel)
		}
	} else {
		parsedLevel := pipeline.ParseLevelAsNumber(level)
		if parsedLevel == -1 {
			fail = true
		} else {
			node.MutateToInt(parsedLevel)
		}
	}

	if fail && p.config.RemoveOnFail {
		node.Suicide()
	}

	return pipeline.ActionPass
}
