# JSON extract plugin
It extracts a field from JSON-encoded event field and adds extracted field to the event root.
> If extracted field already exists in the event root, it will be overridden.

### Examples
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: json_extract
      field: log
      extract_field: error.code
    ...
```
The original event:
```json
{
  "log": "{\"level\":\"error\",\"message\":\"error occurred\",\"service\":\"my-service\",\"error\":{\"code\":2,\"args\":[]}}",
  "time": "2024-03-01T10:49:28.263317941Z"
}
```
The resulting event:
```json
{
  "log": "{\"level\":\"error\",\"message\":\"error occurred\",\"service\":\"my-service\",\"error\":{\"code\":2,\"args\":[]}}",
  "time": "2024-03-01T10:49:28.263317941Z",
  "code": 2
}
```

### Benchmarks
Performance comparison of `json_extract` and `json_decode` plugins.
`json_extract` on average 3 times faster than `json_decode`.

| json (length) | json_extract (time ns) | json_decode (time ns) |
|---------------|------------------------|-----------------------|
| 129           | 33                     | 176                   |
| 309           | 264                    | 520                   |
| 2109          | 2263                   | 6778                  |
| 10909         | 11289                  | 32205                 |
| 21909         | 23277                  | 62819                 |

### Config params
**`field`** *`cfg.FieldSelector`* *`required`* 

The event field from which to extract. Must be a string.

<br>

**`extract_field`** *`cfg.FieldSelector`* *`required`* 

Field to extract.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*