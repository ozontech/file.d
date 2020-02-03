# Rename action plugin
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

##
 *Generated using **insane-doc***