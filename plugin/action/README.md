# Action plugins

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

*Generated using __insane-doc__*