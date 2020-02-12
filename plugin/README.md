# Plugin list

# Inputs
## fake
Plugin provides methods to use in test scenarios:



[More details...](plugin/input/fake/README.md)
## file
Plugin is watching for files in the provided directory and reads them line by line.
Each line should contain only one event. It also correctly handles rotations (rename/truncate) and symlinks.
From time to time it instantly releases and reopens descriptors of completely processed files.
Such behaviour allows files to be deleted by third party software even though `file-d` is still working (in this case reopen will fail).
Watcher is trying to use file system events detect file creation and updates.
But update events don't work with symlinks, so watcher also periodically manually `fstat` all tracking files to detect changes.


> ⚠ It supports commitment mechanism. But at least once delivery guarantees only if files aren't being truncated.
> However, `file-d` correctly handles file truncation there is a little chance of data loss.
> It isn't an `file-d` issue. Data may have been written just before file truncation. In this case, you may late to read some events.
> If you care about delivery, you should also know that `logrotate` manual clearly states that copy/truncate may cause data loss even on a rotating stage.
> So use copy/truncate or similar actions only if your data isn't very important.


Config example for reading docker container log files:
```yaml
pipelines:
  example_docker_pipeline:
    type: file
    watching_dir: /var/lib/docker/containers
    offsets_file: /data/offsets.yaml
    filename_pattern: "*-json.log"
    persistence_mode: async
```

[More details...](plugin/input/file/README.md)
## http
Plugin listens to HTTP requests. Request body should contain events delimited by a new line.
Also it emulates some protocols to allow receive events from wide range of software which use HTTP to transmit data.
E.g. `file-d` may pretends to be Elasticsearch allows clients to send events using Elasticsearch protocol.
So you can use Elasticsearch filebeat output plugin to send data to `file-d`.

> Currently event commitment mechanism isn't implemented for that plugin.
> Plugin answers with HTTP code `OK 200` right after it have read all the request body.
> It doesn't wait until events will be committed.

[More details...](plugin/input/http/README.md)
## kafka
Plugin reads events from listed kafka topics. It uses `sarama` lib.
It supports commitment mechanism, so it guaranties at least once delivery.

[More details...](plugin/input/kafka/README.md)

# Actions
## discard
Plugin simply drops event. Used in a combination with `match_fields` parameters to filter out events.

Example discarding informational and debug logs:
```
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
Plugin extracts object keys and adds them into the root with some prefix. If provided field isn't object, event will be skipped.

Example:
```
pipelines:
  example_pipeline:
    ...
    actions:
    - type: flatten
      field: animal
      prefix: pet_
    ...
```

Will transform `{"animal":{"type":"cat","paws":4}}` into `{"pet_type":"b","pet_paws":"4"}`.

[More details...](plugin/action/flatten/README.md)
## join
Plugin also known as "multiline" makes one big event from event sequence.
Useful for assembling back together "exceptions" or "panics" if they was written line by line.

> ⚠ Parsing all event flow could be very CPU intensive because plugin uses regular expressions.
> Consider `match_fields` parameter to process only particular events. Check out example for details.

Example of joining Golang panics:
```
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
## json_decode
Plugin decodes JSON string from event field and merges result with event root.
If decoded JSON isn't an object, event will be skipped.

[More details...](plugin/action/json_decode/README.md)
## k8s
Plugin adds k8s meta info to docker logs and also joins split docker logs into one event.
Source docker log file name should be in format: `[pod-name]_[namespace]_[container-name]-[container-id].log` e.g. `/docker-logs/advanced-logs-checker-1566485760-trtrq_sre_duty-bot-4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0.log`

[More details...](plugin/action/k8s/README.md)
## keep_fields
Plugin keeps list of the fields of the event and removes others.

[More details...](plugin/action/keep_fields/README.md)
## modify
Plugin modifies content for a field. Works only with strings.
There can be provided unlimited config parameters. Each parameter handled as `cfg.FieldSelector`:`cfg.Substitution`.

Example:
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: modify
      my_object.field.subfield: value is ${another_object.value}.
    ...
```

Result event could looks like:
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
Plugin parses HTTP input using Elasticsearch /_bulk API format: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html
It converts sources defining by create/index actions to the events. Update/delete actions are ignored.

[More details...](plugin/action/parse_es/README.md)
## remove_fields
Plugin removes list of the fields of the event and keeps others.

[More details...](plugin/action/remove_fields/README.md)
## rename
Plugin renames fields of the event. There can be provided unlimited config parameters. Each parameter handled as `cfg.FieldSelector`:`string`.
When `override` is set to `false` no renaming will be done in the case of field name collision.

Example:
pipelines:
  example_pipeline:
    ...
    actions:
    - type: rename
      override: false
      my_object.field.subfield: new_sub_field
    ...

Result event could looks like:
```
{
  "my_object": {
    "field": {
      "new_sub_field":"value"
    }
  },
```

[More details...](plugin/action/rename/README.md)
## throttle
Plugin drops events if event flow gets higher than a configured threshold.

[More details...](plugin/action/throttle/README.md)

# Outputs
## devnull
Plugin provides methods to use in test scenarios:



[More details...](plugin/output/devnull/README.md)
## elasticsearch
Plugin writes events into Elasticsearch. It uses `_bulk` API to send events in batches.
If a network error occurs batch will be infinitely tries to be delivered to random endpoint.

[More details...](plugin/output/elasticsearch/README.md)
## gelf
Plugin sends event batches to the GELF endpoint. Transport level protocol TCP or UDP is configurable.
> It doesn't support UDP chunking. So don't use UDP if event size may be grater than 8192.

GELF messages are separated by null byte. Each message is a JSON with the following fields:
* `version`, string, should be `1.1`
* `host`, string
* `short_message`, string
* `full_message`, string
* `timestamp`, number
* `level`, number
* `_extra_field_1`, string
* `_extra_field_2`, string
* `_extra_field_3`, string

Every field with an underscore prefix (_) will be treated as an extra field.
Allowed characters in a field names are any word character(letter, number, underscore), dashes and dots.

[More details...](plugin/output/gelf/README.md)
## kafka
Plugin sends event batches to the kafka brokers. It uses `sarama` lib.

[More details...](plugin/output/kafka/README.md)
## stdout
Plugin simply writes events to stdout(also known as console).

[More details...](plugin/output/stdout/README.md)


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*