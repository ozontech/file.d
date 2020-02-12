# Modify action plugin
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

<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*