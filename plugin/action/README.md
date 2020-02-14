# Action plugins

## discard
Simply drops event. Used in a combination with `match_fields`/`match_mode` parameters to filter out events.

**Example discarding informational and debug logs:**
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
Extracts object keys and adds them into the root with some prefix. If provided field isn't object, event will be skipped.

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
Transforms `{"animal":{"type":"cat","paws":4}}` into `{"pet_type":"b","pet_paws":"4"}`.

[More details...](plugin/action/flatten/README.md)
## join
Makes one big event from event sequence.
Useful for assembling back together "exceptions" or "panics" if they was written line by line. 
Also known as "multiline".

> ⚠ Parsing all event flow could be very CPU intensive because plugin uses regular expressions.
> Consider `match_fields` parameter to process only particular events. Check out example for details.

**Example of joining Go panics**:
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
Decodes JSON string from event field and merges result with event root.
If decoded JSON isn't an object, event will be skipped.

[More details...](plugin/action/json_decode/README.md)
## k8s
Adds kubernetes meta information into events collected from docker log files. Also joins split docker logs into one event.

Source docker log file name should be in format:<br> `[pod-name]_[namespace]_[container-name]-[container-id].log` 

E.g. `my_pod-1566485760-trtrq_my-namespace_my-container-4e0301b633eaa2bfdcafdeba59ba0c72a3815911a6a820bf273534b0f32d98e0.log`

Information which plugin adds: 
* `k8s_node` – node name where pod is running
* `k8s_pod` – pod name
* `k8s_namespace` – pod namespace name
* `k8s_container` – pod container name
* `k8s_label_*` – pod labels


[More details...](plugin/action/k8s/README.md)
## keep_fields
Keeps list of the event fields and removes others.

[More details...](plugin/action/keep_fields/README.md)
## modify
Modifies content for a field. Works only with strings.
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
Parses HTTP input using Elasticsearch `/_bulk` API format. It converts sources defining by create/index actions to the events. Update/delete actions are ignored.
> Check out for details: https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html

[More details...](plugin/action/parse_es/README.md)
## remove_fields
Removes list of the event fields and keeps others.

[More details...](plugin/action/remove_fields/README.md)
## rename
Renames fields of the event. There can be provided unlimited config parameters. Each parameter handled as `cfg.FieldSelector`:`string`.
When `override` is set to `false` no renaming will be done in the case of field name collision.

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

**Result event could looks like:**
```yaml
{
  "my_object": {
    "field": {
      "new_sub_field":"value"
    }
  },
```

[More details...](plugin/action/rename/README.md)
## throttle
Discards events if pipeline throughput gets higher than a configured threshold.

[More details...](plugin/action/throttle/README.md)
<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*