# Action plugins

## convert_date
It converts field date/time data to different format.

[More details...](/plugin/action/convert_date/README.md)
## debug
It logs event to stdout. Useful for debugging.

[More details...](/plugin/action/debug/README.md)
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

[More details...](/plugin/action/discard/README.md)
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

[More details...](/plugin/action/flatten/README.md)
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

[More details...](/plugin/action/join/README.md)
## json_decode
It decodes a JSON string from the event field and merges the result with the event root.
If the decoded JSON isn't an object, the event will be skipped.

[More details...](/plugin/action/json_decode/README.md)
## keep_fields
It keeps the list of the event fields and removes others.

[More details...](/plugin/action/keep_fields/README.md)
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

[More details...](/plugin/action/modify/README.md)
## parse_es
It parses HTTP input using Elasticsearch `/_bulk` API format. It converts sources defining create/index actions to the events. Update/delete actions are ignored.
> Check out the details in [Elastic Bulk API](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html).

[More details...](/plugin/action/parse_es/README.md)
## remove_fields
It removes the list of the event fields and keeps others.

[More details...](/plugin/action/remove_fields/README.md)
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

[More details...](/plugin/action/rename/README.md)
## throttle
It discards the events if pipeline throughput gets higher than a configured threshold.

[More details...](/plugin/action/throttle/README.md)
<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*