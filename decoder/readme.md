# Decoders

In pipeline settings there is a parameter for choosing desired decoding format.
By default, every pipeline utilizes json decoder, which tries to parse json from log.

Available values for decoders param:
+ auto -- selects decoder type depending on input (e.g. for k8s input plugin, the decoder will be selected depending on container runtime version)
+ json -- parses json format from log into event
+ raw -- writes raw log into event `message` field
+ cri -- parses cri format from log into event (e.g. `2016-10-06T00:17:09.669794203Z stderr F log content`)
+ postgres -- parses postgres format from log into event (e.g. `2021-06-22 16:24:27 GMT [7291] => [3-1] client=test_client,db=test_db,user=test_user LOG:  listening on Unix socket \"/var/run/postgresql/.s.PGSQL.5432\"\n`)
+ nginx_error -- parses nginx error log format from log into event (e.g. `2022/08/17 10:49:27 [error] 2725122#2725122: *792412315 lua udp socket read timed out, context: ngx.timer`)
+ protobuf -- parses protobuf message into event 

**Note**: currently `auto` is available only for usage with k8s input plugin.

## Nginx decoder

Example of decoder for nginx logs with line joins

```yml
pipelines:
  example:
    actions:
      - type: join
        field: message
        start: '/^\d{1,7}#\d{1,7}\:.*/'
        continue: '/(^\w+)/'
      - type: convert_date
        source_formats: ['2006/01/02 15:04:05']
        target_format: 'rfc822'
    settings:
      decoder: 'nginx_error'
    input:
      type: file
      watching_dir: /dir
      offsets_file: /dir/offsets
      filename_pattern: "error.log"
      persistence_mode: async

    output:
      type: stdout
```

## Protobuf decoder

For correct decoding, the protocol scheme and message name are required.
They must be specified in `decoder_params`:
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
        proto_message: 'MyMessage'
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
        proto_message: 'MyMessage'
```

Decoder with `proto_import_paths`:
```yml
pipelines:
  example:
    settings:
      decoder: protobuf
      decoder_params:
        proto_file: 'example.proto'
        proto_message: 'MyMessage'
        proto_import_paths:
          - path/to/proto_dir1
          - path/to/proto_dir2
```