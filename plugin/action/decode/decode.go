package decode

import (
	"fmt"

	"github.com/ozontech/file.d/cfg"
	"github.com/ozontech/file.d/decoder"
	"github.com/ozontech/file.d/fd"
	"github.com/ozontech/file.d/pipeline"
	insaneJSON "github.com/ozontech/insane-json"
	"go.uber.org/zap"
)

/*{ introduction
It decodes a string from the event field and merges the result with the event root.
> If one of the decoded keys already exists in the event root, it will be overridden.
}*/

/*{ examples
### JSON decoder
JSON decoder is used by default, so there is no need to specify it explicitly.
You can specify `json_max_fields_size` in `params` to limit the length of string fields.
> If the decoded JSON isn't an object, the event will be skipped.

Default decoder:
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: decode
      field: log
      prefix: p_
    ...
```
The original event:
```json
{
  "level": "error",
  "log": "{\"message\":\"error occurred\",\"ts\":\"2023-10-30T13:35:33.638720813Z\",\"stream\":\"stderr\"}",
  "service": "test"
}
```
The resulting event:
```json
{
  "level": "error",
  "service": "test",
  "p_message": "error occurred",
  "p_ts": "2023-10-30T13:35:33.638720813Z",
  "p_stream": "stderr"
}
```
---
Decoder with `json_max_fields_size` param:
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: decode
      field: log
      decoder: json
      params:
        json_max_fields_size:
          message: 5
          ts: 10
    ...
```
The original event:
```json
{
  "level": "error",
  "log": "{\"message\":\"error occurred\",\"ts\":\"2023-10-30T13:35:33.638720813Z\",\"stream\":\"stderr\"}",
  "service": "test"
}
```
The resulting event:
```json
{
  "level": "error",
  "service": "test",
  "message": "error",
  "ts": "2023-10-30",
  "stream": "stderr"
}
```

### Postgres decoder
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: decode
      field: log
      decoder: postgres
    ...
```
The original event:
```json
{
  "level": "error",
  "log": "2021-06-22 16:24:27 GMT [7291] => [3-1] client=test_client,db=test_db,user=test_user LOG:  listening on Unix socket \"/var/run/postgresql/.s.PGSQL.5432\"",
  "service": "test"
}
```
The resulting event:
```json
{
  "level": "error",
  "service": "test",
  "time": "2021-06-22 16:24:27 GMT",
  "pid": "7291",
  "pid_message_number": "3-1",
  "client": "test_client",
  "db": "test_db",
  "user": "test_user",
  "log": "listening on Unix socket \"/var/run/postgresql/.s.PGSQL.5432\""
}
```

### NginxError decoder
You can specify `nginx_with_custom_fields: true` in `params` to decode custom fields.

Default decoder:
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: decode
      field: log
      decoder: nginx_error
    ...
```
The original event:
```json
{
  "level": "warn",
  "log": "2022/08/17 10:49:27 [error] 2725122#2725122: *792412315 lua udp socket read timed out, context: ngx.timer",
  "service": "test"
}
```
The resulting event:
```json
{
  "service": "test",
  "time": "2022/08/17 10:49:27",
  "level": "error",
  "pid": "2725122",
  "tid": "2725122",
  "cid": "792412315",
  "message": "lua udp socket read timed out, context: ngx.timer"
}
```
---
Decoder with `nginx_with_custom_fields` param:
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: decode
      field: log
      decoder: nginx_error
      params:
        nginx_with_custom_fields: true
    ...
```
The original event:
```json
{
  "level": "warn",
  "log": "2022/08/18 09:29:37 [error] 844935#844935: *44934601 upstream timed out (110: Operation timed out), while connecting to upstream, client: 10.125.172.251, server: , request: \"POST /download HTTP/1.1\", upstream: \"http://10.117.246.15:84/download\", host: \"mpm-youtube-downloader-38.name.tldn:84\"",
  "service": "test"
}
```
The resulting event:
```json
{
  "service": "test",
  "time": "2022/08/18 09:29:37",
  "level": "error",
  "pid": "844935",
  "tid": "844935",
  "cid": "44934601",
  "message": "upstream timed out (110: Operation timed out), while connecting to upstream",
  "client": "10.125.172.251",
  "server": "",
  "request": "POST /download HTTP/1.1",
  "upstream": "http://10.117.246.15:84/download",
  "host": "mpm-youtube-downloader-38.name.tldn:84"
}
```

### Protobuf decoder
For correct decoding, the protocol scheme and message name are required.
They must be specified in `params`.

Decoder with proto-file path:
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: decode
      field: log
      decoder: protobuf
      params:
        proto_file: 'path/to/proto/example.proto'
        proto_message: MyMessage
    ...
```

Decoder with proto-file content:
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: decode
      field: log
      decoder: protobuf
      params:
        proto_file: |
          syntax = "proto3";

          package example;
          option go_package = "example.v1";

          message Data {
            string string_data = 1;
            int32 int_data = 2;
          }

          message MyMessage {
            message InternalData {
              repeated string my_strings = 1;
              bool is_valid = 2;
            }

            Data data = 1;
            InternalData internal_data = 2;
            uint64 version = 3;
          }
        proto_message: MyMessage
    ...
```

Decoder with `proto_import_paths`:
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: decode
      field: log
      decoder: protobuf
      params:
        proto_file: 'example.proto'
        proto_message: MyMessage
        proto_import_paths:
          - path/to/proto_dir1
          - path/to/proto_dir2
    ...
```

The original event:
```json
{
  "level": "error",
  "log": *proto binary message, for example 'MyMessage'*,
  "service": "test"
}
```
The resulting event:
```json
{
  "level": "error",
  "service": "test",
  "data": {
	"string_data": "my_string",
	"int_data": 123
  },
  "internal_data": {
	"my_strings": ["str1","str2"],
	"is_valid": true
  },
  "version": 10
}
```

### Keep origin
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: decode
      field: log
      decoder: json
      prefix: p_
      keep_origin: true
    ...
```
The original event:
```json
{
  "level": "error",
  "log": "{\"message\":\"error occurred\",\"ts\":\"2023-10-30T13:35:33.638720813Z\",\"stream\":\"stderr\"}",
  "service": "test"
}
```
The resulting event:
```json
{
  "level": "error",
  "log": "{\"message\":\"error occurred\",\"ts\":\"2023-10-30T13:35:33.638720813Z\",\"stream\":\"stderr\"}",
  "service": "test",
  "p_message": "error occurred",
  "p_ts": "2023-10-30T13:35:33.638720813Z",
  "p_stream": "stderr"
}
```
}*/

type decoderType int

const (
	decJson decoderType = iota
	decPostgres
	decNginxError
	decProtobuf
)

type logDecodeErrorMode int

const (
	// ! "logDecodeErrorMode" #1 /`([a-z]+)`/
	logDecodeErrorModeOff      logDecodeErrorMode = iota // * `off` – do not log decode errors
	logDecodeErrorModeErrOnly                            // * `erronly` – log only errors without any other data
	logDecodeErrorModeWithNode                           // * `withnode` – log errors with field value
)

type Plugin struct {
	config  *Config
	decoder decoder.Decoder
	logger  *zap.Logger
}

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
	// > Decoder type.
	Decoder  string `json:"decoder" default:"json" options:"json|postgres|nginx_error|protobuf"` // *
	Decoder_ decoderType

	// > @3@4@5@6
	// >
	// > Decoding params.
	// >
	// > **Json decoder params**:
	// > * `json_max_fields_size` - map `{path}: {limit}` where **{path}** is path to field (`cfg.FieldSelector`) and **{limit}** is integer limit of the field length.
	// > If set, the fields will be cut to the specified limit.
	// >	> It works only with string values. If the field doesn't exist or isn't a string, it will be skipped.
	// >
	// > **NginxError decoder params**:
	// > * `nginx_with_custom_fields` - if set, custom fields will be extracted.
	// >
	// > **Protobuf decoder params**:
	// > * `proto_file` - protocol scheme, can be specified as both the path to the file and the contents of the file.
	// > * `proto_message` - message name in the specified `proto_file`.
	// > * `proto_import_paths` - optional list of paths within which the search will occur (including imports in `proto_file`).
	// > If present and not empty, then all file paths to find are assumed to be relative to one of these paths. Otherwise, all file paths to find are assumed to be relative to the current working directory.
	// >> If `proto_file` contains only system imports, then there is no need to add these files to one of the directories specified in `proto_import_paths`.
	// >> Otherwise, all imports specified in the `proto_file` must be added to one of the directories specified in `proto_import_paths` respecting the file system tree.
	// >>
	// >> List of system imports:
	// >> * google/protobuf/any.proto
	// >> * google/protobuf/api.proto
	// >> * google/protobuf/compiler/plugin.proto
	// >> * google/protobuf/descriptor.proto
	// >> * google/protobuf/duration.proto
	// >> * google/protobuf/empty.proto
	// >> * google/protobuf/field_mask.proto
	// >> * google/protobuf/source_context.proto
	// >> * google/protobuf/struct.proto
	// >> * google/protobuf/timestamp.proto
	// >> * google/protobuf/type.proto
	// >> * google/protobuf/wrappers.proto
	Params map[string]any `json:"params"` // *

	// > @3@4@5@6
	// >
	// > Prefix to add to decoded keys.
	Prefix string `json:"prefix" default:""` // *

	// > @3@4@5@6
	// >
	// > If set, the plugin will keep origin `field` after decoding.
	// >
	// >> If one of the decoded keys matches the original `field`, it will be overridden anyway.
	KeepOrigin bool `json:"keep_origin" default:"false"` // *

	// > @3@4@5@6
	// >
	// > Defines how to handle logging of decode errors.
	// > @logDecodeErrorMode|comment-list
	LogDecodeErrorMode  string `json:"log_decode_error_mode" default:"off" options:"off|erronly|withnode"` // *
	LogDecodeErrorMode_ logDecodeErrorMode
}

func init() {
	fd.DefaultPluginRegistry.RegisterAction(&pipeline.PluginStaticInfo{
		Type:    "decode",
		Factory: factory,
	})
}

func factory() (pipeline.AnyPlugin, pipeline.AnyConfig) {
	return &Plugin{}, &Config{}
}

func (p *Plugin) Start(config pipeline.AnyConfig, params *pipeline.ActionPluginParams) {
	p.config = config.(*Config)
	p.logger = params.Logger.Desugar()

	var err error
	switch p.config.Decoder_ {
	case decJson:
		p.decoder, err = decoder.NewJsonDecoder(p.config.Params)
	case decNginxError:
		p.decoder, err = decoder.NewNginxErrorDecoder(p.config.Params)
	case decProtobuf:
		p.decoder, err = decoder.NewProtobufDecoder(p.config.Params)
	}
	if err != nil {
		p.logger.Fatal(fmt.Sprintf("can't create %s decoder", p.config.Decoder), zap.Error(err))
	}
}

func (p *Plugin) Stop() {}

func (p *Plugin) Do(event *pipeline.Event) pipeline.ActionResult {
	fieldNode := event.Root.Dig(p.config.Field_...)
	if fieldNode == nil {
		return pipeline.ActionPass
	}

	switch p.config.Decoder_ {
	case decJson:
		p.decodeJson(event.Root, fieldNode, event.Buf)
	case decPostgres:
		p.decodePostgres(event.Root, fieldNode)
	case decNginxError:
		p.decodeNginxError(event.Root, fieldNode)
	case decProtobuf:
		p.decodeProtobuf(event.Root, fieldNode, event.Buf)
	}

	return pipeline.ActionPass
}

func (p *Plugin) decodeJson(root *insaneJSON.Root, node *insaneJSON.Node, buf []byte) {
	jsonNodeRaw, err := p.decoder.Decode(node.AsBytes(), root)
	if p.checkError(err, node) {
		return
	}
	jsonNode := jsonNodeRaw.(*insaneJSON.Node)
	if !jsonNode.IsObject() {
		return
	}

	if p.config.Prefix != "" {
		fields := jsonNode.AsFields()
		for _, field := range fields {
			l := len(buf)
			buf = append(buf, p.config.Prefix...)
			buf = append(buf, field.AsString()...)
			field.MutateToField(pipeline.ByteToStringUnsafe(buf[l:]))
		}
	}

	if !p.config.KeepOrigin {
		node.Suicide()
	}

	root.MergeWith(jsonNode)
}

func (p *Plugin) decodePostgres(root *insaneJSON.Root, node *insaneJSON.Node) {
	row, err := decoder.DecodePostgres(node.AsBytes())
	if p.checkError(err, node) {
		return
	}

	if !p.config.KeepOrigin {
		node.Suicide()
	}

	p.addFieldPrefix(root, "time", row.Time)
	p.addFieldPrefix(root, "pid", row.PID)
	p.addFieldPrefix(root, "pid_message_number", row.PIDMessageNumber)
	p.addFieldPrefix(root, "client", row.Client)
	p.addFieldPrefix(root, "db", row.DB)
	p.addFieldPrefix(root, "user", row.User)
	p.addFieldPrefix(root, "log", row.Log)
}

func (p *Plugin) decodeNginxError(root *insaneJSON.Root, node *insaneJSON.Node) {
	rowRaw, err := p.decoder.Decode(node.AsBytes())
	if p.checkError(err, node) {
		return
	}
	row := rowRaw.(decoder.NginxErrorRow)

	if !p.config.KeepOrigin {
		node.Suicide()
	}

	p.addFieldPrefix(root, "time", row.Time)
	p.addFieldPrefix(root, "level", row.Level)
	p.addFieldPrefix(root, "pid", row.PID)
	p.addFieldPrefix(root, "tid", row.TID)
	if len(row.CID) > 0 {
		p.addFieldPrefix(root, "cid", row.CID)
	}
	if len(row.Message) > 0 {
		p.addFieldPrefix(root, "message", row.Message)
	}
	for k, v := range row.CustomFields {
		p.addFieldPrefix(root, k, v)
	}
}

func (p *Plugin) decodeProtobuf(root *insaneJSON.Root, node *insaneJSON.Node, buf []byte) {
	jsonRaw, err := p.decoder.Decode(node.AsBytes())
	if p.checkError(err, node) {
		return
	}
	t, err := root.DecodeBytesAdditional(jsonRaw.([]byte))
	if p.checkError(err, node) {
		return
	}

	if p.config.Prefix != "" {
		fields := t.AsFields()
		for _, field := range fields {
			l := len(buf)
			buf = append(buf, p.config.Prefix...)
			buf = append(buf, field.AsString()...)
			field.MutateToField(pipeline.ByteToStringUnsafe(buf[l:]))
		}
	}

	if !p.config.KeepOrigin {
		node.Suicide()
	}

	root.MergeWith(t)
}

func (p *Plugin) addFieldPrefix(root *insaneJSON.Root, key string, val []byte) {
	if p.config.Prefix != "" {
		key = fmt.Sprintf("%s%s", p.config.Prefix, key)
	}
	root.AddFieldNoAlloc(root, key).MutateToBytesCopy(root, val)
}

func (p *Plugin) checkError(err error, node *insaneJSON.Node) bool {
	if p.config.LogDecodeErrorMode_ == logDecodeErrorModeOff {
		return err != nil
	}

	msg := fmt.Sprintf("failed to decode %s", p.config.Decoder)
	if p.config.LogDecodeErrorMode_ == logDecodeErrorModeErrOnly {
		p.logger.Error(msg, zap.Error(err))
	} else if p.config.LogDecodeErrorMode_ == logDecodeErrorModeWithNode {
		p.logger.Error(msg, zap.Error(err), zap.String("node", node.AsString()))
	}
	return true
}
