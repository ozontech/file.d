# Configuring

You can specify several pipelines with plugins and their parameters in a yaml format.  
Examples can be found [here](./examples.md).

### Logging

Logging is configured with `LOG_LEVEL` environment variable ('info' by default).

Logging level can be changed in runtime with
[standard zap handler](https://github.com/uber-go/zap/blob/v1.23.0/http_handler.go#L33-L70)
exposed at `/log/level`.

### Actions debugging

To debug any working action you must enable http server via '-http' flag
and visit an endpoint `/pipelines/<pipeline_name>/<plugin_index_in_config>/sample`.
It will show 1 sample of the in/out events.

For example:
```yaml
pipelines:
  http_file:
    input:
      type: http
    actions:
      - type: discard
        match_fields:
          should_drop: ok
        match_mode: or
      - type: join
        field: log
        start: '/^(panic:)|(http: panic serving)/'
        continue: '/(^$)|(goroutine [0-9]+ \[)|(\([0-9]+x[0-9,a-f]+)|(\.go:[0-9]+ \+[0-9]x)|(\/.*\.go:[0-9]+)|(\(...\))|(main\.main\(\))|(created by .*\/.*\.)|(^\[signal)|(panic.+[0-9]x[0-9,a-f]+)|(panic:)/'
        match_fields:
          stream: stderr
    output:
      type: file
```

If `-http=':9090'` debug endpoints will be:

`http://127.0.0.1:9090/pipelines/http_file/1/sample` - for the discard plugin

`http://127.0.0.1:9090/pipelines/http_file/2/sample` - for the join plugin

### Overriding configurations

You can use multiple configuration files. This allows you to define a base configuration (e.g., common.yaml) and override or extend it with additional configurations (e.g., local.yaml).

```
./file.d
--config=common.yaml
--config=local.yaml
```

```yaml
# common.yaml
pipelines:
  test1:
    input:
        ...
    actions:
      - type: discard
    output:
        type: kafka
 
# local.yaml
pipelines:
  test1:
    actions:
      - type: modify
    output:
        type: file
 
# result
pipelines:
  test1:
    input:
        ...
    actions:
      - type: modify
    output:
        type: file
```

Arrays (or lists) are usually replaced entirely when merging configurations (e.g., actions). Dictionaries (or maps), on the other hand, are typically merged (e.g., output.type).

### Overriding by environment variables

`file.d` can override config fields if you specify environment variables with `FILED_` prefix.  
The name of the env will be divided by underscores and the config will set or override the config field by the resulted
path.

As for now, overriding works only for fields in JSON objects, but array field overriding can be added easily, please
submit an issue or pull request.

For instance, in order to add Vault token `example_token` to the configuration, you should
specify `FILED_VAULT_TOKEN=example_token`, and it will be added as:

```yaml
vault:
  token: example_token
pipelines:
  pipeline_name: ...
```

### Vault support

Consider this config:

```yaml
vault:
  token: example_token
  address: http://127.0.0.1:8200
pipelines:
  example:
    input:
      type: file
      filename_pattern: vault(secret/prod/file_settings, filename_pattern)
    output:
      type: devnull
```

`file.d` supports getting secrets from Vault as soon as you specify Vault token and an address in a configuration.  
Then you can write any field-string in both arrays and dictionaries using syntax `vault(path/to/secret, key)`,  
and `file.d` tries to connect to Vault and get the secret from there.  
If you need to pass a literal string that begins with `vault(`, you should escape the value with a
backslash: `\vault(path/to/secret, key)`.

### Env support

Consider this config:

```yaml
pipelines:
  example:
    input:
      type: fake
    output:
      type: devnull
    actions:
      - type: modify
        field: env(ENV_NAME)
```

`file.d` supports getting environment variables. Then you can write any
field-string in both arrays and dictionaries using syntax `env(ENV_NAME)`,  
and `file.d` tries to get environment variable value. If you need to pass
a literal string that begins with `env(`, you should escape the value with a
backslash: `\env(ENV_NAME)`.

### Do action if match

### match_fields

File.d can do any action if it matches by pattern.
In the `match_fields` you can pass some patterns, for example:

```yaml
pipelines:
  k8s:
    actions:
      - type: discard
        match_fields:
          k8s_pod:
            - seq-proxy-z501-75d49d84f9-j5jtd
            - seq-proxy-z502-76b68778b6-g7d9l
            - seq-proxy-z503-66bcdf4878-656gc
    input:
      offsets_file: /data/k8s-offsets.yaml
      type: k8s
    output:
      brokers:
        - kafka-z501-0.kafka-z501.logging.svc.cluster.local:9092
      default_topic: stg-k8s-logs
      topic_field: pipeline_kafka_topic
      type: kafka
```

It discards all logs from the listed pods.
But pod name may be changed later, so we can use one of match modes (all modes are described below): `or_prefix`
or `and_prefix`.
In this case both of them do the same logic.

```yaml
pipelines:
  k8s:
    actions:
      - type: discard
        match_fields:
          k8s_pod:
            - seq-proxy-z501
        match_mode: or_prefix
```

It discards all logs from pods whose name starts with seq-proxy-z501.

We can list several matches that will trigger before the plugin process an event, for example:

```yaml
pipelines:
  test:
    actions:
      - type: discard
        match_fields:
          k8s_namespace:
            - map
            - payment
            - checkout
          k8s_pod:
            - coredns
            - etcd_backup
          level: info
        match_mode: and_prefix
```

It discards logs if that contain the field `k8s_namespace` with any listed values (`map` or `payment` or `checkout`)
*and* `k8s_pod` with any listed values (`coredns` or `etcd_backup`) *and* `level` which contains `info` value.

Patterns must have a list ([]) or string type, not a number or null.

### Match modes
@match-modes|header-description

### Decoders

If you have logs in specific non-json format, you can specify decoder type in pipeline settings. By default `json` decoder is used. More details can be found [here](../decoder/readme.md).

```yml
pipelines:
  example:
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
    actions:
      - type: join
        field: message
        start: '/^\d{1,7}#\d{1,7}\:.*/'
        continue: '/(^\w+)/'
      - type: convert_date
        source_formats: ['2006/01/02 15:04:05']
        target_format: 'rfc822'
```
