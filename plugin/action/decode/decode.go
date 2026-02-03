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

### Nginx-error decoder
The event root may contain any of the following fields:
* `time` *string*
* `level` *string*
* `pid` *string*
* `tid` *string*
* `cid` *string*
* `message` *string*

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

### Syslog-RFC3164 decoder
The event root may contain any of the following fields:
* `priority` *string*
* `facility` *string*
* `severity` *string*
* `timestamp` *string* (`Stamp` format)
* `hostname` *string*
* `app_name` *string*
* `process_id` *string*
* `message` *string*

You can specify `syslog_facility_format` and `syslog_severity_format` in `params`
for preferred `facility` and `severity` fields format.

Default decoder:
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: decode
      field: log
      decoder: syslog_rfc3164
    ...
```
The original event:
```json
{
  "log": "<34>Oct  5 22:14:15 mymachine.example.com myproc[10]: 'myproc' failed on /dev/pts/8",
  "service": "test"
}
```
The resulting event:
```json
{
  "service": "test",
  "priority": "34",
  "facility": "4",
  "severity": "2",
  "timestamp": "Oct  5 22:14:15",
  "hostname": "mymachine.example.com",
  "app_name": "myproc",
  "process_id": "10",
  "message": "'myproc' failed on /dev/pts/8"
}
```
---
Decoder with `syslog_*_format` params:
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: decode
      field: log
      decoder: syslog_rfc3164
      params:
        syslog_facility_format: 'string'
        syslog_severity_format: 'string'
    ...
```
The original event:
```json
{
  "log": "<34>Oct 11 22:14:15 mymachine.example.com myproc: 'myproc' failed on /dev/pts/8",
  "service": "test"
}
```
The resulting event:
```json
{
  "service": "test",
  "priority": "34",
  "facility": "AUTH",
  "severity": "CRIT",
  "timestamp": "Oct 11 22:14:15",
  "hostname": "mymachine.example.com",
  "app_name": "myproc",
  "message": "'myproc' failed on /dev/pts/8"
}
```

### Syslog-RFC5424 decoder
The event root may contain any of the following fields:
* `priority` *string*
* `facility` *string*
* `severity` *string*
* `proto_version` *string*
* `timestamp` *string* (`RFC3339`/`RFC3339Nano` format)
* `hostname` *string*
* `app_name` *string*
* `process_id` *string*
* `message_id` *string*
* `message` *string*
* `SD_1` *object*
* ...
* `SD_N` *object*

You can specify `syslog_facility_format` and `syslog_severity_format` in `params`
for preferred `facility` and `severity` fields format.

Default decoder:
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: decode
      field: log
      decoder: syslog_rfc5424
    ...
```
The original event:
```json
{
  "log": "<165>1 2003-10-11T22:14:15.003Z mymachine.example.com myproc 10 ID47 [exampleSDID iut=\"3\" eventSource=\"Application\" eventID=\"1011\"] An application event log",
  "service": "test"
}
```
The resulting event:
```json
{
  "service": "test",
  "priority": "165",
  "facility": "20",
  "severity": "5",
  "proto_version": "1",
  "timestamp": "2003-10-11T22:14:15.003Z",
  "hostname": "mymachine.example.com",
  "app_name": "myproc",
  "process_id": "10",
  "message_id": "ID47",
  "message": "An application event log",
  "exampleSDID": {
    "iut": "3",
    "eventSource": "Application",
    "eventID": "1011"
  }
}
```
---
Decoder with `syslog_*_format` params:
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: decode
      field: log
      decoder: syslog_rfc5424
      params:
        syslog_facility_format: 'string'
        syslog_severity_format: 'string'
    ...
```
The original event:
```json
{
  "log": "<165>1 2003-10-11T22:14:15.003Z mymachine.example.com myproc - ID47 [exampleSDID iut=\"3\" eventSource=\"Application\" eventID=\"1011\"]",
  "service": "test"
}
```
The resulting event:
```json
{
  "service": "test",
  "priority": "165",
  "facility": "LOCAL4",
  "severity": "NOTICE",
  "proto_version": "1",
  "timestamp": "2003-10-11T22:14:15.003Z",
  "hostname": "mymachine.example.com",
  "app_name": "myproc",
  "message_id": "ID47",
  "exampleSDID": {
    "iut": "3",
    "eventSource": "Application",
    "eventID": "1011"
  }
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

### CSV decoder

Default decoder:
```yaml
pipelines:
  example:
    ...
    actions:
    - type: decode
      field: log
      decoder: csv
    ...
```
The original event:
```json
{
  "level": "error",
  "log": "error,error occurred,2023-10-30T13:35:33.638720813Z,stderr",
  "service": "test"
}
```
The resulting event:
```json
{
  "level": "error",
  "service": "test",
  "0": "error",
  "1": "error occurred",
  "2": "2023-10-30T13:35:33.638720813Z",
  "3": "stderr"
}
```
---
Decoder with `columns` and `invalid_line_mode` param:
```yaml
pipelines:
  example:
    ...
    actions:
    - type: decode
      field: log
      decoder: csv
      params:
        columns:
        - a
        - b
        - c
        - d
        invalid_line_mode: continue
    ...
```
The original event:
```json
{
  "level": "error",
  "log": "error,error occurred,2023-10-30T13:35:33.638720813Z,stderr,additional field",
  "service": "test"
}
```
The resulting event:
```json
{
  "level": "error",
  "service": "test",
  "a": "error",
  "b": "error occurred",
  "c": "2023-10-30T13:35:33.638720813Z",
  "d": "stderr",
  "4": "additional field"
}
```
---
Decoder with `prefix` and `delimiter` params:
```yaml
pipelines:
  example:
    ...
    actions:
    - type: decode
      field: log
      decoder: csv
      params:
        prefix: 'csv_'
        delimiter: " "
    ...
```
The original event:
```json
{
  "level": "error",
  "log": "error "error occurred" 2023-10-30T13:35:33.638720813Z stderr",
  "service": "test"
}
```
The resulting event:
```json
{
  "level": "error",
  "service": "test",
  "csv_0": "error",
  "csv_1": "error occurred",
  "csv_2": "2023-10-30T13:35:33.638720813Z",
  "csv_3": "stderr"
}
```
}*/

type decoderType int

const (
	decJson decoderType = iota
	decPostgres
	decNginxError
	decProtobuf
	decSyslogRFC3164
	decSyslogRFC5424
	decCSV
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
	Decoder  string `json:"decoder" default:"json" options:"json|postgres|nginx_error|protobuf|syslog_rfc3164|syslog_rfc5424|csv"` // *
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
	// > **Nginx-error decoder params**:
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
	// >
	// > **Syslog-RFC3164 & Syslog-RFC5424 decoder params**:
	// > * `syslog_facility_format` - facility format, must be one of `number|string` (`number` by default).
	// > * `syslog_severity_format` - severity format, must be one of `number|string` (`number` by default).
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
	case decSyslogRFC3164:
		p.decoder, err = decoder.NewSyslogRFC3164Decoder(p.config.Params)
	case decSyslogRFC5424:
		p.decoder, err = decoder.NewSyslogRFC5424Decoder(p.config.Params)
	case decCSV:
		p.decoder, err = decoder.NewCSVDecoder(p.config.Params)
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
	case decSyslogRFC3164:
		p.decodeSyslogRFC3164(event.Root, fieldNode)
	case decSyslogRFC5424:
		p.decodeSyslogRFC5424(event.Root, fieldNode)
	case decCSV:
		p.decodeCSV(event.Root, fieldNode)
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

	pipeline.MergeToRoot(root, jsonNode)
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

	pipeline.MergeToRoot(root, t)
}

func (p *Plugin) decodeSyslogRFC3164(root *insaneJSON.Root, node *insaneJSON.Node) {
	rowRaw, err := p.decoder.Decode(node.AsBytes())
	if p.checkError(err, node) {
		return
	}
	row := rowRaw.(decoder.SyslogRFC3164Row)

	if !p.config.KeepOrigin {
		node.Suicide()
	}

	p.decodeSyslog(root, decoder.SyslogRFC5424Row{SyslogRFC3164Row: row})
}

func (p *Plugin) decodeSyslogRFC5424(root *insaneJSON.Root, node *insaneJSON.Node) {
	rowRaw, err := p.decoder.Decode(node.AsBytes())
	if p.checkError(err, node) {
		return
	}
	row := rowRaw.(decoder.SyslogRFC5424Row)

	if !p.config.KeepOrigin {
		node.Suicide()
	}

	p.decodeSyslog(root, row)
}

func (p *Plugin) decodeCSV(root *insaneJSON.Root, node *insaneJSON.Node) {
	rowRaw, err := p.decoder.Decode(node.AsBytes())
	if p.checkError(err, node) {
		return
	}
	row := rowRaw.(decoder.CSVRow)

	if !p.config.KeepOrigin {
		node.Suicide()
	}

	d := p.decoder.(*decoder.CSVDecoder)

	err = d.CheckInvalidLine(row)
	if err != nil {
		p.logger.Error("invalid line", zap.Strings("row", row), zap.Error(err))
		return
	}

	for i := range row {
		p.addFieldPrefix(root, d.GenerateColumnName(i), row[i])
	}
}

func (p *Plugin) decodeSyslog(root *insaneJSON.Root, row decoder.SyslogRFC5424Row) { // nolint: gocritic // hugeParam is ok
	p.addFieldPrefix(root, "priority", row.Priority)
	p.addFieldPrefix(root, "facility", row.Facility)
	p.addFieldPrefix(root, "severity", row.Severity)
	if len(row.ProtoVersion) > 0 {
		p.addFieldPrefix(root, "proto_version", row.ProtoVersion)
	}
	if len(row.Timestamp) > 0 {
		p.addFieldPrefix(root, "timestamp", row.Timestamp)
	}
	if len(row.Hostname) > 0 {
		p.addFieldPrefix(root, "hostname", row.Hostname)
	}
	if len(row.AppName) > 0 {
		p.addFieldPrefix(root, "app_name", row.AppName)
	}
	if len(row.ProcID) > 0 {
		p.addFieldPrefix(root, "process_id", row.ProcID)
	}
	if len(row.MsgID) > 0 {
		p.addFieldPrefix(root, "message_id", row.MsgID)
	}
	if len(row.Message) > 0 {
		p.addFieldPrefix(root, "message", row.Message)
	}

	for id, params := range row.StructuredData {
		if len(params) == 0 {
			continue
		}
		obj := root.AddFieldNoAlloc(root, id).MutateToObject()
		for k, v := range params {
			obj.AddFieldNoAlloc(root, k).MutateToBytesCopy(root, v)
		}
	}
}

func (p *Plugin) addFieldPrefix(root *insaneJSON.Root, key string, val any) {
	f := root.AddFieldNoAlloc(root, p.config.Prefix+key)
	switch v := val.(type) {
	case []byte:
		f.MutateToBytesCopy(root, v)
	case string:
		f.MutateToString(v)
	default:
		panic("")
	}
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
