# JSON extract plugin
It extracts fields from JSON-encoded event field and adds extracted fields to the event root.
> If extracted field already exists in the event root, it will be overridden.

## Examples
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: json_extract
      field: log
      extract_fields:
        - error.code
        - level
        - meta
        - flags
    ...
```
The original event:
```json
{
  "log": "{\"level\":\"error\",\"message\":\"error occurred\",\"error\":{\"code\":2,\"args\":[]},\"meta\":{\"service\":\"my-service\",\"pod\":\"my-service-5c4dfcdcd4-4v5zw\"},\"flags\":[\"flag1\",\"flag2\"]}",
  "time": "2024-03-01T10:49:28.263317941Z"
}
```
The resulting event:
```json
{
  "log": "{\"level\":\"error\",\"message\":\"error occurred\",\"error\":{\"code\":2,\"args\":[]},\"meta\":{\"service\":\"my-service\",\"pod\":\"my-service-5c4dfcdcd4-4v5zw\"},\"flags\":[\"flag1\",\"flag2\"]}",
  "time": "2024-03-01T10:49:28.263317941Z",
  "code": 2,
  "level": "error",
  "meta": {
    "service": "my-service",
    "pod": "my-service-5c4dfcdcd4-4v5zw"
  },
  "flags": ["flag1", "flag2"]
}
```

## Benchmarks
Performance comparison of `json_extract` and `json_decode` plugins.
`json_extract` on average 2.5 times faster than `json_decode` and
doesn't allocate memory during the extract process.

### Extract 1 field
| json (length) | json_extract (time ns) | json_decode (time ns) |
|---------------|------------------------|-----------------------|
| 309           | 300                    | 560                   |
| 2109          | 2570                   | 7250                  |
| 10909         | 13550                  | 34250                 |
| 21909         | 26000                  | 67940                 |
| 237909        | 262500                 | 741530                |

### Extract 5 fields
| json (length) | json_extract (time ns) | json_decode (time ns) |
|---------------|------------------------|-----------------------|
| 309           | 450                    | 685                   |
| 2109          | 2990                   | 7410                  |
| 10909         | 14540                  | 35000                 |
| 21909         | 28340                  | 69950                 |
| 237909        | 286600                 | 741600                |

## Config params
**`field`** *`cfg.FieldSelector`* *`required`* 

The event field from which to extract. Must be a string.

<br>

**`extract_field`** *`cfg.FieldSelector`* 

Field to extract.
> ⚠ DEPRECATED. Use `extract_fields` instead.

<br>

**`extract_fields`** *`[]cfg.FieldSelector`* 

Fields to extract.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*