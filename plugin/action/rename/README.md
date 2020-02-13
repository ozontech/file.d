# Rename plugin
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

<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*