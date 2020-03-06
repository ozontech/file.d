# Modify plugin
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

<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*