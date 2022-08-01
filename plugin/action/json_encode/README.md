# JSON encode plugin
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


### Config params
**`field`** *`cfg.FieldSelector`* *`required`* 

The event field to encode. Must be a string.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*