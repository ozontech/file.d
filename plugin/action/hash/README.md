# Hash plugin
It calculates the hash for one of the specified event fields and adds a new field with result in the event root.
> Fields can be of any type except for an object and an array.

## Examples
Hashing without normalization (first found field is `error.code`):
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: hash
      fields:
        - field: error.code
        - field: level
      result_field: hash
    ...
```
The original event:
```json
{
  "level": "error",
  "error": {
    "code": "unauthenticated",
    "message": "bad token format"
  }
}
```
The resulting event:
```json
{
  "level": "error",
  "error": {
    "code": "unauthenticated",
    "message": "bad token format"
  },
  "hash": 6584967863753642363,
}
```
---
Hashing with normalization (first found field is `message`):
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: hash
      fields:
        - field: error.code
        - field: message
          format: normalize
      result_field: hash
    ...
```
The original event:
```json
{
  "level": "error",
  "message": "2023-10-30T13:35:33.638720813Z error occurred, client: 10.125.172.251, upstream: \"http://10.117.246.15:84/download\", host: \"mpm-youtube-downloader-38.name.com:84\""
}
```

Normalized 'message':
`<datetime> error occurred, client: <ip>, upstream: "<url>", host: "<host>:<int>"`

The resulting event:
```json
{
  "level": "error",
  "message": "2023-10-30T13:35:33.638720813Z error occurred, client: 10.125.172.251, upstream: \"http://10.117.246.15:84/download\", host: \"mpm-youtube-downloader-38.name.com:84\"",
  "hash": 13863947727397728753,
}
```

## Config params
**`fields`** *`[]Field`* *`required`* 

Prioritized list of fields. The first field found will be used to calculate the hash.

`Field` params:
* **`field`** *`cfg.FieldSelector`* *`required`*

	The event field for calculating the hash.

* **`format`** *`string`* *`default=no`* *`options=no|normalize`*

	The field format for various hashing algorithms.

<br>

**`result_field`** *`cfg.FieldSelector`* *`required`* 

The event field to which put the hash.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*