# Decode plugin
It decodes a string from the event field and merges the result with the event root.
> If one of the decoded keys already exists in the event root, it will be overridden.

## Examples
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

## Config params
**`field`** *`cfg.FieldSelector`* *`required`* 

The event field to decode. Must be a string.

<br>

**`decoder`** *`string`* *`default=json`* *`options=json|postgres|nginx_error|protobuf|syslog_rfc3164|syslog_rfc5424`* 

Decoder type.

<br>

**`params`** *`map[string]any`* 

Decoding params.

**Json decoder params**:
* `json_max_fields_size` - map `{path}: {limit}` where **{path}** is path to field (`cfg.FieldSelector`) and **{limit}** is integer limit of the field length.
If set, the fields will be cut to the specified limit.
	> It works only with string values. If the field doesn't exist or isn't a string, it will be skipped.

**Nginx-error decoder params**:
* `nginx_with_custom_fields` - if set, custom fields will be extracted.

**Protobuf decoder params**:
* `proto_file` - protocol scheme, can be specified as both the path to the file and the contents of the file.
* `proto_message` - message name in the specified `proto_file`.
* `proto_import_paths` - optional list of paths within which the search will occur (including imports in `proto_file`).
If present and not empty, then all file paths to find are assumed to be relative to one of these paths. Otherwise, all file paths to find are assumed to be relative to the current working directory.
> If `proto_file` contains only system imports, then there is no need to add these files to one of the directories specified in `proto_import_paths`.
> Otherwise, all imports specified in the `proto_file` must be added to one of the directories specified in `proto_import_paths` respecting the file system tree.
>
> List of system imports:
> * google/protobuf/any.proto
> * google/protobuf/api.proto
> * google/protobuf/compiler/plugin.proto
> * google/protobuf/descriptor.proto
> * google/protobuf/duration.proto
> * google/protobuf/empty.proto
> * google/protobuf/field_mask.proto
> * google/protobuf/source_context.proto
> * google/protobuf/struct.proto
> * google/protobuf/timestamp.proto
> * google/protobuf/type.proto
> * google/protobuf/wrappers.proto

**Syslog-RFC3164 & Syslog-RFC5424 decoder params**:
* `syslog_facility_format` - facility format, must be one of `number|string` (`number` by default).
* `syslog_severity_format` - severity format, must be one of `number|string` (`number` by default).

<br>

**`prefix`** *`string`* 

Prefix to add to decoded keys.

<br>

**`keep_origin`** *`bool`* *`default=false`* 

If set, the plugin will keep origin `field` after decoding.

> If one of the decoded keys matches the original `field`, it will be overridden anyway.

<br>

**`log_decode_error_mode`** *`string`* *`default=off`* *`options=off|erronly|withnode`* 

Defines how to handle logging of decode errors.
*  `off` – do not log decode errors
*  `erronly` – log only errors without any other data
*  `withnode` – log errors with field value

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*