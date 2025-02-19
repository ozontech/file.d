package convert_date

import (
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
)

/*{ introduction
It converts field date/time data to different format.
}*/

type Plugin struct {
	config *Config
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The event field name which contains date information.
	Field  cfg.FieldSelector `json:"field" parse:"selector" required:"false" default:"time"` // *
	Field_ []string

	// > @3@4@5@6
	// >
	// > List of date formats to parse a field. Can be specified as a datetime layout in Go [time.Parse](https://pkg.go.dev/time#Parse) format or by alias.
	// > List of available datetime format aliases can be found [here](/pipeline/README.md#datetime-parse-formats).
	SourceFormats  []string `json:"source_formats" default:"rfc3339nano,rfc3339"` // *
	SourceFormats_ []string

	// > @3@4@5@6
	// >
	// > Date format to convert to. Can be specified as a datetime layout in Go [time.Parse](https://pkg.go.dev/time#Parse) format or by alias.
	// > List of available datetime format aliases can be found [here](/pipeline/README.md#datetime-parse-formats).
	TargetFormat  string `json:"target_format" default:"unixtime"` // *
	TargetFormat_ string

	// > @3@4@5@6
	// >
	// > Remove field if conversion fails.
	RemoveOnFail bool `json:"remove_on_fail" default:"false"` // *
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

func (p *Plugin) Start(config pipeline.AnyConfig, _ *pipeline.ActionPluginParams) {
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
	if isValidType {
		date := dateNode.AsString()
		for _, format := range p.config.SourceFormats_ {
			t, err := pipeline.ParseTime(format, date)
			if err == nil {
				switch p.config.TargetFormat_ {
				case pipeline.UnixTime:
					dateNode.MutateToInt(int(t.Unix()))
				case pipeline.UnixTimeMilli:
					dateNode.MutateToInt(int(t.UnixMilli()))
				case pipeline.UnixTimeMicro:
					dateNode.MutateToInt(int(t.UnixMicro()))
				case pipeline.UnixTimeNano:
					dateNode.MutateToInt(int(t.UnixNano()))
				default:
					dateNode.MutateToString(t.Format(p.config.TargetFormat_))
				}

				return pipeline.ActionPass // successful conversion
			}
		}
	}

	// failed conversion
	if p.config.RemoveOnFail {
		dateNode.Suicide()
	}

	return pipeline.ActionPass
}
