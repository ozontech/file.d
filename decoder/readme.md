# Decoders

In pipeline settings there is a parameter for choosing desired decoding format.
By default, every pipeline utilizes json decoder, which tries to parse json from log.

Some decoders support parameters, you can specify them in `decoder_params`.

Available values for `decoder` param:
+ auto -- selects decoder type depending on input (e.g. for k8s input plugin, the decoder will be selected depending on container runtime version)
+ json -- parses json format from log into event
+ raw -- writes raw log into event `message` field
+ cri -- parses cri format from log into event (e.g. `2016-10-06T00:17:09.669794203Z stderr F log content`)
+ postgres -- parses postgres format from log into event (e.g. `2021-06-22 16:24:27 GMT [7291] => [3-1] client=test_client,db=test_db,user=test_user LOG:  listening on Unix socket \"/var/run/postgresql/.s.PGSQL.5432\"\n`)
+ nginx_error -- parses nginx error log format from log into event (e.g. `2022/08/17 10:49:27 [error] 2725122#2725122: *792412315 lua udp socket read timed out, context: ngx.timer`)
+ protobuf -- parses protobuf message into event
+ syslog_rfc3164 -- parses syslog-RFC3164 format from log into event (see [RFC3164](https://datatracker.ietf.org/doc/html/rfc3164))
+ syslog_rfc5424 -- parses syslog-RFC5424 format from log into event (see [RFC5424](https://datatracker.ietf.org/doc/html/rfc5424))

> Currently `auto` is available only for usage with k8s input plugin.

## Json decoder

### Params
* `json_max_fields_size` - map `{path}: {limit}` where **{path}** is path to field (`cfg.FieldSelector`) and **{limit}** is integer limit of the field length.
If set, the fields will be cut to the specified limit.
  > It works only with string values. If the field doesn't exist or isn't a string, it will be skipped.

### Examples
Default decoder:
```yml
pipelines:
  example:
    settings:
      decoder: 'json'
```
From:

`"{\"level\":\"error\",\"message\":\"error occurred\",\"ts\":\"2023-10-30T13:35:33.638720813Z\",\"stream\":\"stderr\"}"`

To:
```json
{
  "level": "error",
  "message": "error occurred",
  "ts": "2023-10-30T13:35:33.638720813Z",
  "stream": "stderr"
}
```
---
Decoder with `json_max_fields_size` param:
```yaml
pipelines:
  example:
    settings:
      decoder: 'json'
      decoder_params:
        json_max_fields_size:
          level: 3
          message: 5
          ts: 10
```
From:

`"{\"level\":\"error\",\"message\":\"error occurred\",\"ts\":\"2023-10-30T13:35:33.638720813Z\",\"stream\":\"stderr\"}"`

To:
```json
{
  "level": "err",
  "message": "error",
  "ts": "2023-10-30",
  "stream": "stderr"
}
```

## Nginx-error decoder
The resulting event may contain any of the following fields:
* `time` *string*
* `level` *string*
* `pid` *string*
* `tid` *string*
* `cid` *string*
* `message` *string*

### Params
* `nginx_with_custom_fields` - if set, custom fields will be extracted.

### Examples
Default decoder:
```yml
pipelines:
  example:
    settings:
      decoder: 'nginx_error'
```
From:

`2022/08/17 10:49:27 [error] 2725122#2725122: *792412315 lua udp socket read timed out, context: ngx.timer`

To:
```json
{
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
  example:
    settings:
      decoder: 'nginx_error'
      decoder_params:
        nginx_with_custom_fields: true
```
From:

`2022/08/18 09:29:37 [error] 844935#844935: *44934601 upstream timed out (110: Operation timed out), while connecting to upstream, client: 10.125.172.251, server: , request: "POST /download HTTP/1.1", upstream: "http://10.117.246.15:84/download", host: "mpm-youtube-downloader-38.name.tldn:84"`

To:
```json
{
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

## Protobuf decoder
For correct decoding, the protocol scheme and message name are required.
They must be specified in `decoder_params`.

### Params
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

### Examples
Decoder with proto-file path:
```yml
pipelines:
  example:
    settings:
      decoder: protobuf
      decoder_params:
        proto_file: 'path/to/proto/example.proto'
        proto_message: MyMessage
```

Decoder with proto-file content:
```yml
pipelines:
  example:
    settings:
      decoder: protobuf
      decoder_params:
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
```

Decoder with `proto_import_paths`:
```yml
pipelines:
  example:
    settings:
      decoder: protobuf
      decoder_params:
        proto_file: 'example.proto'
        proto_message: MyMessage
        proto_import_paths:
          - path/to/proto_dir1
          - path/to/proto_dir2
```

## Syslog-RFC3164 decoder
The resulting event may contain any of the following fields:
* `priority` *string*
* `facility` *string*
* `severity` *string*
* `timestamp` *string* (`Stamp` format)
* `hostname` *string*
* `app_name` *string*
* `process_id` *string*
* `message` *string*

### Params
* `syslog_facility_format` - facility format, must be one of `number|string` (`number` by default).
* `syslog_severity_format` - severity format, must be one of `number|string` (`number` by default).

### Examples
Default decoder:
```yml
pipelines:
  example:
    settings:
      decoder: 'syslog_rfc3164'
```
From:

`<34>Oct  5 22:14:15 mymachine.example.com myproc[10]: 'myproc' failed on /dev/pts/8`

To:
```json
{
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
  example:
    settings:
      decoder: 'syslog_rfc3164'
      decoder_params:
        syslog_facility_format: 'string'
        syslog_severity_format: 'string'
```
From:

`<34>Oct 11 22:14:15 mymachine.example.com myproc: 'myproc' failed on /dev/pts/8`

To:
```json
{
  "priority": "34",
  "facility": "AUTH",
  "severity": "CRIT",
  "timestamp": "Oct 11 22:14:15",
  "hostname": "mymachine.example.com",
  "app_name": "myproc",
  "message": "'myproc' failed on /dev/pts/8"
}
```

## Syslog-RFC5424 decoder
The resulting event may contain any of the following fields:
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

### Params
* `syslog_facility_format` - facility format, must be one of `number|string` (`number` by default).
* `syslog_severity_format` - severity format, must be one of `number|string` (`number` by default).

### Examples
Default decoder:
```yml
pipelines:
  example:
    settings:
      decoder: 'syslog_rfc5424'
```
From:

`<165>1 2003-10-11T22:14:15.003Z mymachine.example.com myproc 10 ID47 [exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"] An application event log`

To:
```json
{
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
  "exampleSDID@32473": {
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
  example:
    settings:
      decoder: 'syslog_rfc5424'
      decoder_params:
        syslog_facility_format: 'string'
        syslog_severity_format: 'string'
```
From:

`<165>1 2003-10-11T22:14:15.003Z mymachine.example.com myproc - ID47 [exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"]`

To:
```json
{
  "priority": "165",
  "facility": "LOCAL4",
  "severity": "NOTICE",
  "proto_version": "1",
  "timestamp": "2003-10-11T22:14:15.003Z",
  "hostname": "mymachine.example.com",
  "app_name": "myproc",
  "message_id": "ID47",
  "exampleSDID@32473": {
    "iut": "3",
    "eventSource": "Application",
    "eventID": "1011"
  }
}
```