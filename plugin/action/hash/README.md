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

* **`max_size`** *`int`* *`default=0`*

	The maximum field size used in hash calculation of any format.
	If set to `0`, the entire field will be used in hash calculation.

	> If the field size is greater than `max_size`, then
	the first `max_size` bytes will be used in hash calculation.
	>
	> It can be useful in case of performance degradation when calculating the hash of long fields.

<br>

**`result_field`** *`cfg.FieldSelector`* *`required`* 

The event field to which put the hash.

<br>

**`normalizer`** *`*NormalizerConfig`* 

Normalizer params. It works for `fields` with `format: normalize`.
> For more information, see [Normalization](/plugin/action/hash/normalize/README.md).

`NormalizerConfig` params:
* **`with_defaults`** *`bool`* *`default=false`*

	If set to `true`, normalizer will use `patterns` in combination with [default patterns](/plugin/action/hash/normalize/README.md#default-patterns).

* **`patterns`** *`[]NormalizePattern`*

	List of normalization patterns.

	`NormalizePattern` params:
	* **`placeholder`** *`string`* *`required`*

		A placeholder that replaces the parts of string that falls under specified pattern.

	* **`re`** *`string`* *`required`*

		A regular expression that describes a pattern.
		> We have some [limitations](/plugin/action/hash/normalize/README.md#limitations-of-the-re-language) of the RE syntax.

	* **`priority`** *`string`* *`default=first`*  *`options=first|last`*

		A priority of pattern. Works only if `normalizer.with_defaults=true`.

		If set to `first`, pattern will be added before defaults, otherwise - after.

		> If `normalizer.with_defaults=false`, then the priority is determined
		by the order of the elements in `normalizer.patterns`.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*