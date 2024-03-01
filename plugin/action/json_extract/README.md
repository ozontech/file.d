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
| BenchmarkExtractObj/json_length_129-8   | 24082884 | 50.99 ns/op | 0 B/op | 0 allocs/op |
| BenchmarkExtractObj/json_length_329-8   | 4063610  | 303.0 ns/op | 0 B/op | 0 allocs/op |
| BenchmarkExtractObj/json_length_2329-8  | 498919   | 2383 ns/op  | 0 B/op | 0 allocs/op |
| BenchmarkExtractObj/json_length_11129-8 | 103920   | 11756 ns/op | 0 B/op | 0 allocs/op |

**json_decode**

`$ go test -bench=BenchmarkInsane -benchmem ./plugin/action/json_extract/...`

|                                              |         |             |        |             |
|----------------------------------------------|---------|-------------|--------|-------------|
| BenchmarkInsaneDecodeDig/json_length_129-8   | 6968088 | 171.7 ns/op | 0 B/op | 0 allocs/op |
| BenchmarkInsaneDecodeDig/json_length_329-8   | 2122134 | 576.8 ns/op | 0 B/op | 0 allocs/op |
| BenchmarkInsaneDecodeDig/json_length_2329-8  | 250518  | 4808 ns/op  | 8 B/op | 1 allocs/op |
| BenchmarkInsaneDecodeDig/json_length_11129-8 | 54476   | 25769 ns/op | 8 B/op | 1 allocs/op |

### Config params
**`field`** *`cfg.FieldSelector`* *`required`* 

The event field from which to extract. Must be a string.

<br>

**`extract_fields`** *`[]cfg.FieldSelector`* *`required`* 

Fields to extract.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*