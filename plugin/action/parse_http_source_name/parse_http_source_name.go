package add_file_name

import (
	"encoding/base64"
	"strings"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
)

/*{ introduction
It adds a field containing the file name to the event.
It is only applicable for input plugins k8s and file.
}*/

type Plugin struct {
	config *Config
}

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The event field to which put the file name. Must be a string.
	// >
	// > Warn: it overrides fields if it contains non-object type on the path. For example:
	// > if `field` is `info.level` and input
	// > `{ "info": [{"userId":"12345"}] }`,
	// > output will be: `{ "info": {"level": <level>} }`
	Field  cfg.FieldSelector `json:"field" parse:"selector" required:"false" default:"source_name"` // *
	Field_ []string
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "parse_http_source_name",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, _ *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	node := event.Root.Dig(p.config.Field_...)
	sourceName := node.AsString()

	sourceInfo := strings.Split(sourceName, "_")

	if sourceInfo[0] != "http" {
		panic("wrong format got: " + sourceName)
	}

	infoStr, _ := base64.StdEncoding.DecodeString(sourceInfo[1])
	info := strings.Split(string(infoStr), "_")

	pipeline.CreateNestedField(event.Root, []string{"login"}).MutateToString(info[0])
	pipeline.CreateNestedField(event.Root, []string{"remote_ip"}).MutateToString(info[1])

	return pipeline.ActionPass
}
