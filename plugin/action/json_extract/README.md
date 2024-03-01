# JSON extract plugin
It extracts a fields from JSON-encoded event field and adds extracted fields to the event root.
Supports: `object`, `string`, `int`, `float`, `bool` and `null` values.
> If extracted field already exists in the event root, it will be overridden.

### Examples
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
  "level": "error",
  "code": 2
}
```

### Benchmarks
Performance comparison of `json_extract` and  `json_decode` plugins. Each bench named by length of json-field.

**json_extract**

`$ go test -bench=BenchmarkExtract -benchmem ./plugin/action/json_extract/...`

|                                         |          |             |        |             |
|-----------------------------------------|----------|-------------|--------|-------------|
| BenchmarkExtractObj/json_length_129-8   | 24161701 | 50.80 ns/op | 0 B/op | 0 allocs/op |
| BenchmarkExtractObj/json_length_309-8   | 4276380  | 282.9 ns/op | 0 B/op | 0 allocs/op |
| BenchmarkExtractObj/json_length_2109-8  | 522370   | 2313 ns/op  | 0 B/op | 0 allocs/op |
| BenchmarkExtractObj/json_length_10909-8 | 104278   | 11589 ns/op | 0 B/op | 0 allocs/op |

**json_decode**

`$ go test -bench=BenchmarkInsane -benchmem ./plugin/action/json_extract/...`

|                                              |         |             |        |             |
|----------------------------------------------|---------|-------------|--------|-------------|
| BenchmarkInsaneDecodeDig/json_length_129-8   | 6769700 | 173.2 ns/op | 0 B/op | 0 allocs/op |
| BenchmarkInsaneDecodeDig/json_length_309-8   | 2282385 | 522.9 ns/op | 0 B/op | 0 allocs/op |
| BenchmarkInsaneDecodeDig/json_length_2109-8  | 177818  | 6784 ns/op  | 8 B/op | 1 allocs/op |
| BenchmarkInsaneDecodeDig/json_length_10909-8 | 38685   | 32629 ns/op | 8 B/op | 1 allocs/op |

### Config params
**`field`** *`cfg.FieldSelector`* *`required`* 

The event field from which to extract. Must be a string.

<br>

**`extract_fields`** *`[]cfg.FieldSelector`* *`required`* 

Fields to extract.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*