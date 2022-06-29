package convert_log_level

import (
	"strconv"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
)

/*{ introduction
It converts the log level field according RFC-5424.
}*/
type Plugin struct {
	config *Config
}

//! config-params
//^ config-params
type Config struct {
	//> @3@4@5@6
	//>
	//> The event field name which log level.
	Field  cfg.FieldSelector `json:"field" parse:"selector" required:"false" default:"level"` //*
	Field_ []string

	//> @3@4@5@6
	//>
	//> Date format to convert to.
	Style string `json:"style" default:"number" options:"number|string"` //*

	//> @3@4@5@6
	//>
	//> Default log level if if cannot be parsed. Pass empty, to skip set default level.
	DefaultLevel       string `json:"default_level" default:""` //*
	defaultNumberLevel int

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

	if p.config.Style == "number" && p.config.DefaultLevel != "" {
		var err error
		p.config.defaultNumberLevel, err = strconv.Atoi(p.config.DefaultLevel)
		if err != nil {
			params.Logger.Fatalf("can't parse default log level for number style: %s", err.Error())
		}
	}
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	node := event.Root.Dig(p.config.Field_...)
	if node == nil {
		if p.config.DefaultLevel == "" {
			return pipeline.ActionPass
		}
		node = event.Root.AddFieldNoAlloc(event.Root, "level").MutateToString(p.config.DefaultLevel)
	}

	//isValidType := node.IsString() || node.IsNumber()
	//if !isValidType {
	//	if p.config.RemoveOnFail {
	//		node.Suicide()
	//		return pipeline.ActionPass
	//	}
	//}

	level := node.AsString()
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
