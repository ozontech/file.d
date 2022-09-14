# Plugin list

# Inputs
## dmesg
It reads kernel events from /dev/kmsg

[More details...](plugin/input/dmesg/README.md)
## fake
It provides an API to test pipelines and other plugins.

[More details...](plugin/input/fake/README.md)
## file
It watches for files in the provided directory and reads them line by line.

Each line should contain only one event. It also correctly handles rotations (rename/truncate) and symlinks.

From time to time, it instantly releases and reopens descriptors of the completely processed files.
Such behavior allows files to be deleted by a third party software even though `file.d` is still working (in this case the reopening will fail).

A watcher is trying to use the file system events to detect file creation and updates.
But update events don't work with symlinks, so watcher also periodically manually `fstat` all tracking files to detect changes.

> ⚠ It supports the commitment mechanism. But "least once delivery" is guaranteed only if files aren't being truncated.
> However, `file.d` correctly handles file truncation, there is a little chance of data loss.
> It isn't a `file.d` issue. The data may have been written just before the file truncation. In this case, you may miss to read some events.
> If you care about the delivery, you should also know that the `logrotate` manual clearly states that copy/truncate may cause data loss even on a rotating stage.
> So use copy/truncate or similar actions only if your data isn't critical.
> In order to reduce potential harm of truncation, you can turn on notifications of file changes.
> By default the plugin is notified only on file creations. Note that following for changes is more CPU intensive.


**Reading docker container log files:**
```yaml
pipelines:
  example_docker_pipeline:
    input:
        type: file
        watching_dir: /var/lib/docker/containers
        offsets_file: /data/offsets.yaml
        filename_pattern: "*-json.log"
        persistence_mode: async
```

[More details...](plugin/input/file/README.md)
## http
Reads events from HTTP requests with the body delimited by a new line.

Also, it emulates some protocols to allow receiving events from a wide range of software that use HTTP to transmit data.
E.g. `file.d` may pretend to be Elasticsearch allows clients to send events using Elasticsearch protocol.
So you can use Elasticsearch filebeat output plugin to send data to `file.d`.

> ⚠ Currently event commitment mechanism isn't implemented for this plugin.
> Plugin answers with HTTP code `OK 200` right after it has read all the request body.
> It doesn't wait until events are committed.

**Example:**
Emulating elastic through http:
```yaml
pipelines:
  example_k8s_pipeline:
    settings:
      capacity: 1024
    input:
      # define input type.
      type: http
      # pretend elastic search, emulate it's protocol.
      emulate_mode: "elasticsearch"
      # define http port.
      address: ":9200"
    actions:
      # parse elastic search query.
      - type: parse_es
      # decode elastic search json.
      - type: json_decode
        # field is required.
        field: message
    output:
      # Let's write to kafka example.
      type: kafka
        brokers: [kafka-local:9092, kafka-local:9091]
        default_topic: yourtopic-k8s-data
        use_topic_field: true
        topic_field: pipeline_kafka_topic

      # Or we can write to file:
      # type: file
      # target_file: "./output.txt"
```

Setup:
```
# run server.
# config.yaml should contains yaml config above.
go run cmd/file.d.go --config=config.yaml

# now do requests.
curl "localhost:9200/_bulk" -H 'Content-Type: application/json' -d \
'{"index":{"_index":"index-main","_type":"span"}}
{"message": "hello", "kind": "normal"}
'

##


[More details...](plugin/input/http/README.md)
## journalctl
Reads `journalctl` output.

[More details...](plugin/input/journalctl/README.md)
## k8s
It reads Kubernetes logs and also adds pod meta-information. Also, it joins split logs into a single event.

Source log file should be named in the following format:<br> `[pod-name]_[namespace]_[container-name]-[container-id].log`

E.g. `my_pod-1566485760-trtrq_my-namespace_my-container-4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0.log`

An information which plugin adds:
* `k8s_node` – node name where pod is running;
* `k8s_node_label_*` – node labels;
* `k8s_pod` – pod name;
* `k8s_namespace` – pod namespace name;
* `k8s_container` – pod container name;
* `k8s_label_*` – pod labels.

**Example:**
```yaml
pipelines:
  example_k8s_pipeline:
    input:
      type: k8s
      offsets_file: /data/offsets.yaml
      file_config:                        // customize file plugin
        persistence_mode: sync
        read_buffer_size: 2048
```

[More details...](plugin/input/k8s/README.md)
## kafka
It reads events from multiple Kafka topics using `sarama` library.
> It guarantees at "at-least-once delivery" due to the commitment mechanism.

[More details...](plugin/input/kafka/README.md)

# Actions
## add_host
It adds field containing hostname to an event.

[More details...](plugin/action/add_host/README.md)
## convert_date
It converts field date/time data to different format.

[More details...](plugin/action/convert_date/README.md)
## convert_log_level
It converts the log level field according RFC-5424.

[More details...](plugin/action/convert_log_level/README.md)
## debug
It logs event to stdout. Useful for debugging.

[More details...](plugin/action/debug/README.md)
## discard
It drops an event. It is used in a combination with `match_fields`/`match_mode` parameters to filter out the events.

**An example for discarding informational and debug logs:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: discard
      match_fields:
        level: /info|debug/
    ...
```

[More details...](plugin/action/discard/README.md)
## flatten
It extracts the object keys and adds them into the root with some prefix. If the provided field isn't an object, an event will be skipped.

**Example:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: flatten
      field: animal
      prefix: pet_
    ...
```
It transforms `{"animal":{"type":"cat","paws":4}}` into `{"pet_type":"b","pet_paws":"4"}`.

[More details...](plugin/action/flatten/README.md)
## join
It makes one big event from the sequence of the events.
It is useful for assembling back together "exceptions" or "panics" if they were written line by line.
Also known as "multiline".

> ⚠ Parsing the whole event flow could be very CPU intensive because the plugin uses regular expressions.
> Consider `match_fields` parameter to process only particular events. Check out an example for details.

**Example of joining Go panics**:
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: join
      field: log
      start: '/^(panic:)|(http: panic serving)/'
      continue: '/(^\s*$)|(goroutine [0-9]+ \[)|(\([0-9]+x[0-9,a-f]+)|(\.go:[0-9]+ \+[0-9]x)|(\/.*\.go:[0-9]+)|(\(...\))|(main\.main\(\))|(created by .*\/.*\.)|(^\[signal)|(panic.+[0-9]x[0-9,a-f]+)|(panic:)/'
      match_fields:
        stream: stderr // apply only for events which was written to stderr to save CPU time
    ...
```

[More details...](plugin/action/join/README.md)
## join_template
Alias to "join" plugin with predefined `start` and `continue` parameters.

> ⚠ Parsing the whole event flow could be very CPU intensive because the plugin uses regular expressions.
> Consider `match_fields` parameter to process only particular events. Check out an example for details.

**Example of joining Go panics**:
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: join_template
      template: go_panic
      field: log
      match_fields:
        stream: stderr // apply only for events which was written to stderr to save CPU time
    ...
```

[More details...](plugin/action/join_template/README.md)
## json_decode
It decodes a JSON string from the event field and merges the result with the event root.
If the decoded JSON isn't an object, the event will be skipped.

[More details...](plugin/action/json_decode/README.md)
## json_encode
It replaces field with its JSON string representation.

**Example:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: json_encode
      field: server
    ...
```
It transforms `{"server":{"os":"linux","arch":"amd64"}}` into `{"server":"{\"os\":\"linux\",\"arch\":\"amd64\"}"}`.


[More details...](plugin/action/json_encode/README.md)
## keep_fields
It keeps the list of the event fields and removes others.

[More details...](plugin/action/keep_fields/README.md)
## mask
Mask plugin matches event with regular expression and substitutions successfully matched symbols via asterix symbol.
You could set regular expressions and submatch groups.

**Example:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: mask
      metric_subsystem_name: "some_name"
      masks:
      - mask:
        re: "\b(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\D?(\d{1,4})\b"
        groups: [1,2,3]
    ...
```


[More details...](plugin/action/mask/README.md)
## modify
It modifies the content for a field. It works only with strings.
You can provide an unlimited number of config parameters. Each parameter handled as `cfg.FieldSelector`:`cfg.Substitution`.

**Example:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: modify
      my_object.field.subfield: value is ${another_object.value}.
    ...
```

The resulting event could look like:
```
{
  "my_object": {
    "field": {
      "subfield":"value is 666."
    }
  },
  "another_object": {
    "value": 666
  }
```

[More details...](plugin/action/modify/README.md)
## parse_es
It parses HTTP input using Elasticsearch `/_bulk` API format. It converts sources defining create/index actions to the events. Update/delete actions are ignored.
> Check out the details in [Elastic Bulk API](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html).

[More details...](plugin/action/parse_es/README.md)
## parse_re2
It parses string from the event field using re2 expression with named subgroups and merges the result with the event root.

[More details...](plugin/action/parse_re2/README.md)
## remove_fields
It removes the list of the event fields and keeps others.

[More details...](plugin/action/remove_fields/README.md)
## rename
It renames the fields of the event. You can provide an unlimited number of config parameters. Each parameter handled as `cfg.FieldSelector`:`string`.
When `override` is set to `false`, the field won't be renamed in the case of field name collision.
Sequence of rename operations isn't guaranteed. Use different actions for prioritization.

**Example:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: rename
      override: false
      my_object.field.subfield: new_sub_field
    ...
```

The resulting event could look like:
```yaml
{
  "my_object": {
    "field": {
      "new_sub_field":"value"
    }
  },
```

[More details...](plugin/action/rename/README.md)
## set_time
It adds time field to the event.

[More details...](plugin/action/set_time/README.md)
## throttle
It discards the events if pipeline throughput gets higher than a configured threshold.

[More details...](plugin/action/throttle/README.md)

# Outputs
## devnull
It provides an API to test pipelines and other plugins.

[More details...](plugin/output/devnull/README.md)
## elasticsearch
It sends events into Elasticsearch. It uses `_bulk` API to send events in batches.
If a network error occurs, the batch will infinitely try to be delivered to the random endpoint.

[More details...](plugin/output/elasticsearch/README.md)
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

[More details...](plugin/output/gelf/README.md)
## kafka
It sends the event batches to kafka brokers using `sarama` lib.

[More details...](plugin/output/kafka/README.md)
## postgres
It sends the event batches to postgres db using pgx.

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
      file_plugin:
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

[More details...](plugin/output/splunk/README.md)
## stdout
It writes events to stdout(also known as console).

[More details...](plugin/output/stdout/README.md)


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*