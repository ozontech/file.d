# Kafka plugin
It reads events from multiple Kafka topics using `sarama` library.
> It guarantees at "at-least-once delivery" due to the commitment mechanism.

**Example**
Standard example:
```yaml
pipelines:
  example_pipeline:
    input:
      type: kafka
      brokers: [kafka:9092, kafka:9091]
      topics: [topic1, topic2]
      offset: newest
      meta:
        partition: '{{ .partition }}'
        topic: '{{ .topic }}'
        offset: '{{ .offset }}'
    # output plugin is not important in this case, let's emulate s3 output.
    output:
      type: s3
      file_config:
        retention_interval: 10s
      endpoint: "s3.fake_host.org:80"
      access_key: "access_key1"
      secret_key: "secret_key2"
      bucket: "bucket-logs"
      bucket_field_event: "bucket_name"
```

### Config params
**`brokers`** *`[]string`* *`required`* 

The name of kafka brokers to read from.

<br>

**`topics`** *`[]string`* *`required`* 

The list of kafka topics to read from.

<br>

**`consumer_group`** *`string`* *`default=file-d`* 

The name of consumer group to use.

<br>

**`client_id`** *`string`* *`default=file-d`* 

Kafka client ID.

<br>

**`channel_buffer_size`** *`int`* *`default=256`* 

The number of unprocessed messages in the buffer that are loaded in the background from kafka.

<br>

**`offset`** *`string`* *`default=newest`* *`options=newest|oldest`* 

The newest and oldest values is used when a consumer starts but there is no committed offset for the assigned partition.
* *`newest`* - set offset to the newest message
* *`oldest`* - set offset to the oldest message

<br>

**`consumer_max_processing_time`** *`cfg.Duration`* *`default=200ms`* 

The maximum amount of time the consumer expects a message takes to process for the user.

<br>

**`consumer_max_wait_time`** *`cfg.Duration`* *`default=250ms`* 

The maximum amount of time the broker will wait for Consumer.Fetch.Min bytes to become available before it returns fewer than that anyways.

<br>

**`is_sasl_enabled`** *`bool`* *`default=false`* 

If set, the plugin will use SASL authentications mechanism.

<br>

**`sasl_mechanism`** *`string`* *`default=SCRAM-SHA-512`* *`options=PLAIN|SCRAM-SHA-256|SCRAM-SHA-512`* 

SASL mechanism to use.

<br>

**`sasl_username`** *`string`* *`default=user`* 

SASL username.

<br>

**`sasl_password`** *`string`* *`default=password`* 

SASL password.

<br>

**`is_ssl_enabled`** *`bool`* *`default=false`* 

If set, the plugin will use SSL/TLS connections method.

<br>

**`ssl_skip_verify`** *`bool`* *`default=false`* 

If set, the plugin will skip SSL/TLS verification.

<br>

**`client_cert`** *`string`* 

Path or content of a PEM-encoded client certificate file.

<br>

**`client_key`** *`string`* 

> Path or content of a PEM-encoded client key file.

<br>

**`ca_cert`** *`string`* 

Path or content of a PEM-encoded CA file.

<br>

**`meta`** *`cfg.MetaTemplates`* 

Meta params

Add meta information to an event (look at Meta params)

Example: ```topic: '{{ .topic }}'```

<br>


### Meta params
**`topic`** 

**`partition`** 

**`offset`** 

<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*