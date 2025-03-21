package set_time

import (
	"time"

	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"github.com/ozontech/file.d/xtime"
)

/*{ introduction
It adds time field to the event.
}*/

type Plugin struct {
	config *Config
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The event field to put the time.
	Field string `json:"field" required:"true" default:"time"` // *

	// > @3@4@5@6
	// >
	// > Date format to parse a field. Can be specified as a datetime layout in Go [time.Parse](https://pkg.go.dev/time#Parse) format or by alias.
	// > List of available datetime format aliases can be found [here](/pipeline/README.md#datetime-parse-formats).
	Format  string `json:"format" default:"rfc3339nano" required:"true"` // *
	Format_ string

	// > @3@4@5@6
	// >
	// > Override field if exists.
	Override bool `json:"override" default:"true"` // *
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "set_time",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, _ *pipeline.ActionPluginParams) {
	p.config = config.(*Config)

	format, err := xtime.ParseFormatName(p.config.Format)
	if err != nil {
		// to support custom formats
		format = p.config.Format
	}

	p.config.Format_ = format
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	return p.do(event, time.Now())
}

func (p *Plugin) do(event *pipeline.Event, t time.Time) pipeline.ActionResult {
	dateNode := event.Root.Dig(p.config.Field)
	if dateNode != nil && !p.config.Override {
		return pipeline.ActionPass
	}
	if dateNode == nil {
		dateNode = event.Root.AddFieldNoAlloc(event.Root, p.config.Field)
	}

	switch p.config.Format_ {
	case xtime.UnixTime:
		dateNode.MutateToInt64(t.Unix())
	case xtime.UnixTimeMilli, "timestampmilli": // timestamp(milli|micro|nano) are left for backward compatibility
		dateNode.MutateToInt64(t.UnixMilli())
	case xtime.UnixTimeMicro, "timestampmicro":
		dateNode.MutateToInt64(t.UnixMicro())
	case xtime.UnixTimeNano, "timestampnano":
		dateNode.MutateToInt64(t.UnixNano())
	default:
		dateNode.MutateToString(t.Format(p.config.Format_))
	}

	return pipeline.ActionPass
}
