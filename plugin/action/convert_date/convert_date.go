package convert_date

import (
	"time"

	"github.com/ozonru/file.d/cfg"
	"github.com/ozonru/file.d/fd"
	"github.com/ozonru/file.d/pipeline"
)

/*{ introduction
It decodes a JSON string from the event field and merges the result with the event root.
If the decoded JSON isn't an object, the event will be skipped.
}*/
type Plugin struct {
	config *Config
}

//! config-params
//^ config-params
type Config struct {
	//> @3@4@5@6
	//>
	//> The event field name which contains date information.
	Field  cfg.FieldSelector `json:"field" parse:"selector" required:"false" default:"time"` //*
	Field_ []string

	//> @3@4@5@6
	//>
	//> List of date formats to parse a field. Available list items should be one of `ansic|unixdate|rubydate|rfc822|rfc822z|rfc850|rfc1123|rfc1123z|rfc3339|rfc3339nano|kitchen|stamp|stampmilli|stampmicro|stampnano`.
	SourceFormats  []string `json:"source_formats" default:"rfc3339nano,rfc3339"` //*
	SourceFormats_ []string

	//> @3@4@5@6
	//>
	//> Date format to convert to.
	TargetFormat  string `json:"target_format" default:"timestamp"` //*
	TargetFormat_ string
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

	for _, formatName := range p.config.SourceFormats {
		format, err := pipeline.ParseFormatName(formatName)
		if err != nil {
			format = formatName
		}
		p.config.SourceFormats_ = append(p.config.SourceFormats_, format)
	}

	format, err := pipeline.ParseFormatName(p.config.TargetFormat)
	if err != nil {
		format = p.config.TargetFormat
	}

	p.config.TargetFormat_ = format
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	dateNode := event.Root.Dig(p.config.Field_...)
	if dateNode == nil {
		return pipeline.ActionPass
	}
	isValidType := dateNode.IsString() || dateNode.IsNumber()
	if !isValidType {
		return pipeline.ActionPass
	}

	date := dateNode.AsString()
	for _, format := range p.config.SourceFormats_ {
		t, err := time.Parse(format, date)
		if err == nil {
			if p.config.TargetFormat_ == "timestamp" {
				dateNode.MutateToInt(int(t.Unix()))
			} else {
				dateNode.MutateToString(t.Format(p.config.TargetFormat_))
			}
		}
	}

	return pipeline.ActionPass
}
