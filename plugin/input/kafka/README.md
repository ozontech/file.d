# Kafka plugin
It reads events from multiple Kafka topics using `franz-go` library.
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

The number of unprocessed messages in the buffer that are loaded in the background from kafka. (max.poll.records)

<br>

**`max_concurrent_fetches`** *`int`* *`default=0`* 

MaxConcurrentFetches sets the maximum number of fetch requests to allow in
flight or buffered at once, overriding the unbounded (i.e. number of
brokers) default.

<br>

**`fetch_max_bytes`** *`cfg.Expression`* *`default=52428800`* 

FetchMaxBytes (fetch.max.bytes) sets the maximum amount of bytes a broker will try to send during a fetch

<br>

**`fetch_min_bytes`** *`cfg.Expression`* *`default=1`* 

FetchMinBytes (fetch.min.bytes) sets the minimum amount of bytes a broker will try to send during a fetch

<br>

**`offset`** *`string`* *`default=newest`* *`options=newest|oldest`* 

The newest and oldest values is used when a consumer starts but there is no committed offset for the assigned partition.
* *`newest`* - set offset to the newest message
* *`oldest`* - set offset to the oldest message

<br>

**`balancer`** *`string`* *`default=round-robin`* *`options=round-robin|range|sticky|cooperative-sticky`* 

Algorithm used by Kafka to assign partitions to consumers in a group.
* *`round-robin`* - M0: [t0p0, t0p2, t1p1], M1: [t0p1, t1p0, t1p2]
* *`range`* - M0: [t0p0, t0p1, t1p0, t1p1], M1: [t0p2, t1p2]
* *`sticky`* - ensures minimal partition movement on group changes while also ensuring optimal balancing
* *`cooperative-sticky`* - performs the sticky balancing strategy, but additionally opts the consumer group into "cooperative" rebalancing

<br>

**`consumer_max_processing_time`** *`cfg.Duration`* *`default=200ms`* 

The maximum amount of time the consumer expects a message takes to process for the user. (Not used anymore!)

<br>

**`consumer_max_wait_time`** *`cfg.Duration`* *`default=250ms`* 

The maximum amount of time the broker will wait for Consumer.Fetch.Min bytes to become available before it returns fewer than that anyways. (fetch.max.wait.ms)

<br>

**`auto_commit_interval`** *`cfg.Duration`* *`default=1s`* 

AutoCommitInterval sets how long to go between autocommits

<br>

**`session_timeout`** *`cfg.Duration`* *`default=10s`* 

SessionTimeout sets how long a member in the group can go between heartbeats

<br>

**`heartbeat_interval`** *`cfg.Duration`* *`default=3s`* 

HeartbeatInterval sets how long a group member goes between heartbeats to Kafka

<br>

**`is_sasl_enabled`** *`bool`* *`default=false`* 

If set, the plugin will use SASL authentications mechanism.

<br>

**`sasl_mechanism`** *`string`* *`default=SCRAM-SHA-512`* *`options=PLAIN|SCRAM-SHA-256|SCRAM-SHA-512|AWS_MSK_IAM`* 

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
Use [go-template](https://pkg.go.dev/text/template) syntax

Example: ```topic: '{{ .topic }}'```

<br>


### Meta params
**`topic`** 

**`partition`** 

**`offset`** 

<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*