# Configuring

You can specify several pipelines with plugins and their parameters in a yaml format.  
Examples can be found [here](./examples.md).

### Logging

Logging is configured with `LOG_LEVEL` environment variable ('info' by default).

Logging level can be changed in runtime with
[standard zap handler](https://github.com/uber-go/zap/blob/v1.23.0/http_handler.go#L33-L70)
exposed at `/log/level`.

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
  k8s_kafka_example:
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

### Supported match modes

File.d supports regexp patterns, but it may spend a lot of CPU. Prefer `(or|and)_prefix` or exact match if you
can.

#### `or`

It allows to find a match in listed patterns by regexp `/(^coredsn.*)|(^etcd_backup.*)/` or exact
comparison (`val == configVal`).

```
pipelines:
  test:
    actions:
      - type: discard
        match_fields:
          k8s_namespace: [payment, tarifficator] # exact compare one of values
          k8s_pod: /^payment-api.*/
        match_mode: or
```

And if we process some logs:

```
{"k8s_namespace": "payment", "k8s_pod":"payment-api-abcd"} # pass
{"k8s_namespace": "tarifficator", "k8s_pod":"payment-api"} # pass
{"k8s_namespace": "map", "k8s_pod":"payment-api"} # pass
{"k8s_namespace": "payment", "k8s_pod":"map-api"} # pass
{"k8s_namespace": "tarifficator", "k8s_pod":"tarifficator-go-api"} # pass
{"k8s_namespace": "sre", "k8s_pod":"cpu-quotas-abcd-1234"} # reject
```

#### `or_prefix`

It allows to find a prefix in the field value.
If you pass a regexp (config value which starts with `/`) it will find pattern, without prefix, like `or`.
I.e. it will work like `or` mode. So, it makes sense to use this mod without regexps.

```
pipelines:
  test:
    actions:
      - type: discard
        match_fields:
          k8s_namespace: [payment, tarifficator]
          k8s_pod: payment-api
        match_mode: or_prefix
```

And if we process some logs:

```
{"k8s_namespace": "payment", "k8s_pod":"payment-api-abcd-1234"} # pass
{"k8s_namespace": "payment", "k8s_pod":"checkout"} # pass
{"k8s_namespace": "map", "k8s_pod":"map-go-api-abcd-1234"} # reject
{"k8s_namespace": "map", "k8s_pod":"payment-api"} # pass
{"k8s_namespace": "tariff", "k8s_pod":"tarifficator"} # pass
```

### `and`

It allows to find a match in listed patterns like `or` and `or_prefix` modes. 
Event must have matches in all listed fields.

For example:

```
pipelines:
  test:
    actions:
      - type: discard
        match_fields:
          k8s_namespace: [payment, tariff]
          k8s_pod: "/^payment-api-.*/"
        match_mode: and
```

And if we process some logs:

```
{"k8s_namespace": "payment", "k8s_pod":"payment-api-abcd-1234"} # pass
{"k8s_namespace": "payment", "k8s_pod":"checkout"} # reject
{"k8s_namespace": "map", "k8s_pod":"payment-api-abcd-1234"} # reject
{"k8s_namespace": "payment", "k8s_pod":"payment-api"} # reject
```

### `and_prefix`

It allows to find a prefix in the field value.
If you pass a regexp (config value which starts with `/`) it will find pattern, without prefix, like `and`.
I.e. it will work like `and` mode. So, it makes sense to use this mod without regexps.

```
pipelines:
  test:
    actions:
      - type: discard
        match_fields:
          k8s_namespace: payment
          k8s_pod: payment-api-
        match_mode: and_prefix
```

And if we process some logs:

```
{"k8s_namespace": "payment", "k8s_pod":"payment-api-abcd-1234"} # pass
{"k8s_namespace": "payment", "k8s_pod":"checkout"} # reject
{"k8s_namespace": "map", "k8s_pod":"payment-api-abcd-1234"} # reject
{"k8s_namespace": "payment", "k8s_pod":"payment-api"} # reject
```

### Some advanced examples

```yaml
pipelines:
  test:
    actions:
      # set field (`pipeline_kafka_topic`) value (`stg-k8s-travel-logs`) if k8s_namespace is `payment` 
      # *and* `service` is `lms-go-service-sorter-system-vanderlande-driver`
      - type: modify
        pipeline_kafka_topic: stg-k8s-travel-logs
        match_fields:
          k8s_namespace: payment
          service: go-payment-api
        match_mode: and
```

<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*