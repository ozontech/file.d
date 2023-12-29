# Convert UTF-8-encoded bytes plugin
It converts multiple UTF-8-encoded bytes to corresponding characters.
Supports unicode (`\u...` and `\U...`), hex (`\x...`) and octal (`\{0-3}{0-7}{0-7}`) encoded bytes.

### Examples
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: convert_utf8_bytes
      field: obj.field
    ...
```

The original event:
```json
{
  "obj": {
    "field": "\\xD0\\xA1\\xD0\\x98\\xD0\\xA1\\xD0\\xA2\\xD0\\x95\\xD0\\x9C\\xD0\\x90.xml"
  }
}
```
The resulting event:
```json
{
  "obj": {
    "field": "СИСТЕМА.xml"
  }
}
```
---
The original event:
```json
{
  "obj": {
    "field": "$\\110\\145\\154\\154\\157\\054\\040\\146\\151\\154\\145\\056\\144!"
  }
}
```
The resulting event:
```json
{
  "obj": {
    "field": "$Hello, file.d!"
  }
}
```
---
The original event:
```json
{
  "obj": {
    "field": "$\\u0048\\u0065\\u006C\\u006C\\u006F\\u002C\\u0020\\ud801\\udc01!"
  }
}
```
The resulting event:
```json
{
  "obj": {
    "field": "$Hello, 𐐁!"
  }
}
```
---
The original event:
```json
{
  "obj": {
    "field": "{\"Dir\":\"C:\\\\Users\\\\username\\\\.prog\\\\120.67.0\\\\x86_64\\\\x64\",\"File\":\"$Storage$\\xD0\\x9F\\xD1\\x80\\xD0\\xB8\\xD0\\xB7\\xD0\\xBD\\xD0\\xB0\\xD0\\xBA.20.tbl.xml\"}"
  }
}
```
The resulting event:
```json
{
  "obj": {
    "field": "{\"Dir\":\"C:\\\\Users\\\\username\\\\.prog\\\\120.67.0\\\\x86_64\\\\x64\",\"File\":\"$Storage$Признак.20.tbl.xml\"}"
  }
}
```

### Config params
**`field`** *`cfg.FieldSelector`* *`required`* 

The name of the event field to convert.
> The field value must be a string.

<br>

<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*