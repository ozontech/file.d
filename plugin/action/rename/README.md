# Rename plugin
It renames the fields of the event. You can provide an unlimited number of config parameters. Each parameter handled as `cfg.FieldSelector`:`string`.
When `override` is set to `false`, the field won't be renamed in the case of field name collision.
Sequence of rename operations isn't guaranteed. Use different actions for prioritization.

**Note**: if the renamed field name starts with underscore "_", it should be escaped with preceding underscore. E.g.
if the renamed field is "_HOSTNAME", in config it should be "___HOSTNAME". Only one preceding underscore is needed.
Renamed field names with only one underscore in config are considered as without preceding underscore:
if there is "_HOSTNAME" in config the plugin searches for "HOSTNAME" field.

**Example common:**
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

Input event:

```json
{
  "my_object": {
    "field": {
      "subfield":"value"
    }
  }
}
```

Output event:

```json
{
  "my_object": {
    "field": {
      "new_sub_field":"value"  // renamed
    }
  }
}
```

**Example journalctl:**
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: rename
      override: false
      __HOSTNAME: host
      ___REALTIME_TIMESTAMP: ts
    ...
```

Input event:

```json
{
  "_HOSTNAME": "example-host",
  "__REALTIME_TIMESTAMP": "1739797379239590"
}
```

Output event:

```json
{
  "host": "example-host",  // renamed
  "ts": "1739797379239590"  // renamed
}
```

<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*