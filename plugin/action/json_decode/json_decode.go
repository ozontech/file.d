package json_decode

import (
	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	"go.uber.org/zap"
)

/*{ introduction
It decodes a JSON string from the event field and merges the result with the event root.
If the decoded JSON isn't an object, the event will be skipped.

> ⚠ DEPRECATED. Use `decode` plugin with `decoder: json` instead.
}*/

type Plugin struct {
	config *Config
	logger *zap.Logger
}

type logJsonParseErrorMode int

const (
	// ! "jsonParseErrorMode" #1 /`([a-z]+)`/
	logJsonParseErrorOff      logJsonParseErrorMode = iota // * `off` – do not log json parse errors
	logJsonParseErrorErrOnly                               // * `erronly` – log only errors without any other data
	logJsonParseErrorWithNode                              // * `withnode` – log errors with json node represented as string
)

// ! config-params
// ^ config-params
type Config struct {
	// > @3@4@5@6
	// >
	// > The event field to decode. Must be a string.
	Field  cfg.FieldSelector `json:"field" parse:"selector" required:"true"` // *
	Field_ []string

	// > @3@4@5@6
	// >
	// > A prefix to add to decoded object keys.
	Prefix string `json:"prefix" default:""` // *

	// > @3@4@5@6
	// >
	// > Defines how to handle logging of json parse error.
	// > @jsonParseErrorMode|comment-list
	// >
	// > Defaults to `off`.
	LogJSONParseErrorMode  string `json:"log_json_parse_error_mode" default:"off" options:"off|erronly|withnode"` // *
	LogJSONParseErrorMode_ logJsonParseErrorMode
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "json_decode",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
	p.logger = params.Logger.Desugar()
	switch p.config.LogJSONParseErrorMode {
	case "off":
		p.config.LogJSONParseErrorMode_ = logJsonParseErrorOff
	case "erronly":
		p.config.LogJSONParseErrorMode_ = logJsonParseErrorErrOnly
	case "withnode":
		p.config.LogJSONParseErrorMode_ = logJsonParseErrorWithNode
	default:
		p.config.LogJSONParseErrorMode_ = logJsonParseErrorOff
	}
}

func (p *Plugin) Stop() {
}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	jsonNode := event.Root.Dig(p.config.Field_...)
	if jsonNode == nil {
		return pipeline.ActionPass
	}

	node, err := event.Root.DecodeBytesAdditional(jsonNode.AsBytes())
	if err != nil {
		if p.config.LogJSONParseErrorMode_ == logJsonParseErrorErrOnly {
			p.logger.Error("failed to parse json", zap.Error(err))
		} else if p.config.LogJSONParseErrorMode_ == logJsonParseErrorWithNode {
			p.logger.Error("failed to parse json", zap.Error(err), zap.String("node", jsonNode.AsString()))
		}
		return pipeline.ActionPass
	}

	if !node.IsObject() {
		return pipeline.ActionPass
	}

	jsonNode.Suicide()

	if p.config.Prefix != "" {
		fields := node.AsFields()
		for _, field := range fields {
			l := len(event.Buf)
			event.Buf = append(event.Buf, p.config.Prefix...)
			event.Buf = append(event.Buf, field.AsString()...)
			field.MutateToField(pipeline.ByteToStringUnsafe(event.Buf[l:]))
		}
	}

	// place decoded object under root
	event.Root.MergeWith(node)

	return pipeline.ActionPass
}
