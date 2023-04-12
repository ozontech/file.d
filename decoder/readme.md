# Decoders

In pipeline settings there is a parameter for choosing desired decoding format.
By default every pipeline utilizes json decoder, which tries to parse json from log.

Available values for decoders param:
+ auto -- selects decoder type depending on input (e.g. for k8s input plugin, the decoder will be selected depending on container runtime version)
+ json -- parses json format from log into event
+ raw -- writes raw log into event `message` field
+ cri -- parses cri format from log into event (e.g. `2016-10-06T00:17:09.669794203Z stderr F log content`)
+ postgres -- parses postgres format from log into event (e.g. `2021-06-22 16:24:27 GMT [7291] => [3-1] client=test_client,db=test_db,user=test_user LOG:  listening on Unix socket \"/var/run/postgresql/.s.PGSQL.5432\"\n`)
+ nginx_error -- parses nginx error log format from log into event (e.g. `2022/08/17 10:49:27 [error] 2725122#2725122: *792412315 lua udp socket read timed out, context: ngx.timer`)

### Nginx decoder

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
