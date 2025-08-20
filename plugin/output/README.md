# Output plugins

## clickhouse
It sends the event batches to Clickhouse database using
[Native format](https://clickhouse.com/docs/en/interfaces/formats/#native) and
[Native protocol](https://clickhouse.com/docs/en/interfaces/tcp/).

File.d uses low level Go client - [ch-go](https://github.com/ClickHouse/ch-go) to provide these features.

Supports [dead queue](/plugin/output/README.md#dead-queue).

[More details...](plugin/output/clickhouse/README.md)
## devnull
It provides an API to test pipelines and other plugins.

[More details...](plugin/output/devnull/README.md)
## elasticsearch
It sends events into Elasticsearch. It uses `_bulk` API to send events in batches.
If a network error occurs, the batch will infinitely try to be delivered to the random endpoint.

Supports [dead queue](/plugin/output/README.md#dead-queue).

[More details...](plugin/output/elasticsearch/README.md)
## file
It sends event batches into files.

[More details...](plugin/output/file/README.md)
## gelf
It sends event batches to the GELF endpoint. Transport level protocol TCP or UDP is configurable.
> It doesn't support UDP chunking. So don't use UDP if event size may be greater than 8192.

GELF messages are separated by null byte. Each message is a JSON with the following fields:
* `version` *`string=1.1`*
* `host` *`string`*
* `short_message` *`string`*
* `full_message` *`string`*
* `timestamp` *`number`*
* `level` *`number`*
* `_extra_field_1` *`string`*
* `_extra_field_2` *`string`*
* `_extra_field_3` *`string`*

Every field with an underscore prefix `_` will be treated as an extra field.
Allowed characters in field names are letters, numbers, underscores, dashes, and dots.

Supports [dead queue](/plugin/output/README.md#dead-queue).

[More details...](plugin/output/gelf/README.md)
## kafka
It sends the event batches to kafka brokers using `franz-go` lib.

Supports [dead queue](/plugin/output/README.md#dead-queue).

[More details...](plugin/output/kafka/README.md)
## loki
It sends the logs batches to Loki using HTTP API.

Supports [dead queue](/plugin/output/README.md#dead-queue).

[More details...](plugin/output/loki/README.md)
## postgres
It sends the event batches to postgres db using pgx.

Supports [dead queue](/plugin/output/README.md#dead-queue).

[More details...](plugin/output/postgres/README.md)
## s3
Sends events to s3 output of one or multiple buckets.
`bucket` is default bucket for events. Addition buckets can be described in `multi_buckets` section, example down here.
Field "bucket_field_event" is filed name, that will be searched in event.
If appears we try to send event to this bucket instead of described here.

> ⚠ Currently bucket names for bucket and multi_buckets can't intersect.

> ⚠ If dynamic bucket moved to config it can leave some not send data behind.
> To send this data to s3 move bucket dir from /var/log/dynamic_buckets/bucketName to /var/log/static_buckets/bucketName (/var/log is default path)
> and restart file.d

**Example**
Standard example:
```yaml
pipelines:
  mkk:
    settings:
      capacity: 128
    # input plugin is not important in this case, let's emulate http input.
    input:
      type: http
      emulate_mode: "no"
      address: ":9200"
	actions:
	- type: json_decode
		field: message
    output:
      type: s3
      file_config:
        retention_interval: 10s
      # endpoint, access_key, secret_key, bucket are required.
      endpoint: "s3.fake_host.org:80"
      access_key: "access_key1"
      secret_key: "secret_key2"
      bucket: "bucket-logs"
      bucket_field_event: "bucket_name"
```

Example with fan-out buckets:
```yaml
pipelines:
  mkk:
    settings:
      capacity: 128
    # input plugin is not important in this case, let's emulate http input.
    input:
      type: http
      emulate_mode: "no"
      address: ":9200"
	actions:
	- type: json_decode
		field: message
    output:
      type: s3
      file_config:
        retention_interval: 10s
      # endpoint, access_key, secret_key, bucket are required.
      endpoint: "s3.fake_host.org:80"
      access_key: "access_key1"
      secret_key: "secret_key2"
      bucket: "bucket-logs"
      # bucket_field_event - event with such field will be sent to bucket with its value
      # if such exists: {"bucket_name": "secret", "message": 123} to bucket "secret".
      bucket_field_event: "bucket_name"
      # multi_buckets is optional, contains array of buckets.
      multi_buckets:
        - endpoint: "otherS3.fake_host.org:80"
          access_key: "access_key2"
          secret_key: "secret_key2"
          bucket: "bucket-logs-2"
        - endpoint: "yet_anotherS3.fake_host.ru:80"
          access_key: "access_key3"
          secret_key: "secret_key3"
          bucket: "bucket-logs-3"
```

[More details...](plugin/output/s3/README.md)
## splunk
It sends events to splunk.

By default it only stores original event under the "event" key according to the Splunk output format.

If other fields are required it is possible to copy fields values from the original event to the other
fields relative to the output json. Copies are not allowed directly to the root of output event or
"event" field and any of its subfields.

For example, timestamps and service name can be copied to provide additional meta data to the Splunk:

```yaml
copy_fields:
  - from: ts
  	to: time
  - from: service
  	to: fields.service_name
```

Here the plugin will lookup for "ts" and "service" fields in the original event and if they are present
they will be copied to the output json starting on the same level as the "event" key. If the field is not
found in the original event plugin will not populate new field in output json.

In:

```json
{
  "ts":"1723651045",
  "service":"some-service",
  "message":"something happened"
}
```

Out:

```json
{
  "event": {
    "ts":"1723651045",
    "service":"some-service",
    "message":"something happened"
  },
  "time": "1723651045",
  "fields": {
    "service_name": "some-service"
  }
}
```

Supports [dead queue](/plugin/output/README.md#dead-queue).

[More details...](plugin/output/splunk/README.md)
## stdout
It writes events to stdout(also known as console).

[More details...](plugin/output/stdout/README.md)

## dead queue

Failed events from the main pipeline are redirected to a dead-letter queue (DLQ) to prevent data loss and enable recovery.

### Examples

#### Dead queue to the reserve elasticsearch

Consumes logs from a Kafka topic. Sends them to Elasticsearch (primary cluster). Fails over to a reserve ("dead-letter") Elasticsearch if the primary is unavailable.

```yaml
main_pipeline:
  input:
    type: kafka
    brokers:
      - kafka:9092
    topics:
      - logs
  output:
    type: elasticsearch
    workers_count: 32
    endpoints:
      - http://elasticsearch-primary:9200
    # route to reserve elasticsearch
    deadqueue:
      endpoints:
        - http://elasticsearch-reserve:9200
      type: elasticsearch
```

#### Dead queue with second kafka topic and low priority consumer

Main Pipeline: Processes logs from Kafka → Elasticsearch. Failed events go to a dead-letter Kafka topic.

Dead-Queue Pipeline: Re-processes failed events from the DLQ topic with lower priority.

```yaml
main_pipeline:
  input:
    type: kafka
    brokers:
      - kafka:9092
    topics:
      - logs
  output:
    type: elasticsearch
    workers_count: 32
    endpoints:
      - http://elasticsearch:9200
    # route to deadqueue pipeline
    deadqueue:
      brokers:
      - kafka:9092
      default_topic: logs-deadqueue
      type: kafka

deadqueue_pipeline:
  input:
    type: kafka
    brokers:
      - kafka:9092
    topics:
      - logs-deadqueue
  output:
    type: elasticsearch
    workers_count: 1 # low priority
    fatal_on_failed_insert: false
    endpoints:
      - http://elasticsearch:9200
```
<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*