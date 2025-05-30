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
  "hash": 6584967863753642363
}
```
---
Hashing with `field.max_size`:
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: hash
      fields:
        - field: message
          max_size: 10
      result_field: hash
    ...
```
The original event:
```json
{
  "level": "error",
  "message": "bad token format"
}
```

The part of the "message" field for which the hash will be calculated:
`bad token `

The resulting event:
```json
{
  "level": "error",
  "message": "bad token format",
  "hash": 6584967863753642363
}
```
---
Hashing with normalization (built-in patterns only):
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

Normalized "message":
`<datetime> error occurred, client: <ip>, upstream: "<url>", host: "<host>:<int>"`

The resulting event:
```json
{
  "level": "error",
  "message": "2023-10-30T13:35:33.638720813Z error occurred, client: 10.125.172.251, upstream: \"http://10.117.246.15:84/download\", host: \"mpm-youtube-downloader-38.name.com:84\"",
  "hash": 13863947727397728753
}
```
---
Hashing with normalization (custom patterns only):
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: hash
      fields:
        - field: message
          format: normalize
      result_field: hash
      normalizer:
        builtin_patterns: "no"
        patterns:
          - placeholder: '<quoted_str>'
            re: '"[^"]*"'
            priority: 'first'
          - placeholder: '<date>'
            re: '\d\d.\d\d.\d\d\d\d'
            priority: 'first'
    ...
```
The original event:
```json
{
  "level": "error",
  "message": "request from \"ivanivanov\", signed on 19.03.2025"
}
```

Normalized "message":
`request from <quoted_str>, signed on <date>`

The resulting event:
```json
{
  "level": "error",
  "message": "request from \"ivanivanov\", signed on 19.03.2025",
  "hash": 6933347847764028189
}
```
---
Hashing with normalization (all built-in & custom patterns):
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: hash
      fields:
        - field: message
          format: normalize
      result_field: hash
      normalizer:
        builtin_patterns: "all"
        patterns:
          - placeholder: '<nginx_datetime>'
            re: '\d\d\d\d/\d\d/\d\d\ \d\d:\d\d:\d\d'
            priority: last
    ...
```
The original event:
```json
{
  "level": "error",
  "message": "2006/01/02 15:04:05 error occurred, client: 10.125.172.251, upstream: \"http://10.117.246.15:84/download\", host: \"mpm-youtube-downloader-38.name.com:84\""
}
```

Normalized "message":
`<nginx_datetime> error occurred, client: <ip>, upstream: <double_quoted>, host: <double_quoted>`

The resulting event:
```json
{
  "level": "error",
  "message": "2006/01/02 15:04:05 error occurred, client: 10.125.172.251, upstream: \"http://10.117.246.15:84/download\", host: \"mpm-youtube-downloader-38.name.com:84\"",
  "hash": 4150276598667727274
}
```
---
Hashing with normalization (partial built-in patterns):
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: hash
      fields:
        - field: message
          format: normalize
      result_field: hash
      normalizer:
        builtin_patterns: "square_bracketed|ip"
    ...
```
The original event:
```json
{
  "level": "error",
  "message": "2006/01/02 15:04:05 error occurred, client: 10.125.172.251, upstream: \"http://10.117.246.15:84/download\", host: \"mpm-youtube-downloader-38.name.com:84\", params: [param1, param2]"
}
```

Normalized "message":
`2006/01/02 15:04:05 error occurred, client: <ip>, upstream: \"http://10.117.246.15:84/download\", host: \"mpm-youtube-downloader-38.name.com:84\", params: <square_bracketed>`

The resulting event:
```json
{
  "level": "error",
  "message": "2006/01/02 15:04:05 error occurred, client: 10.125.172.251, upstream: \"http://10.117.246.15:84/download\", host: \"mpm-youtube-downloader-38.name.com:84\", params: [param1, param2]",
  "hash": 15982987157336450215
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

**`normalizer`** *`NormalizerConfig`* 

Normalizer params. It works for `fields` with `format: normalize`.
> For more information, see [Normalization](/plugin/action/hash/normalize/README.md).

`NormalizerConfig` params:
* **`builtin_patterns`** *`string`* *`default="all"`*

	List of [built-in patterns](/plugin/action/hash/normalize/README.md#built-in-patterns) (see `pattern id` column).

	Format: `pattern_id1|pattern_id2|...|pattern_idN`.

	Example: `host|url|square_bracketed`.

	> * If set to `all` - all built-in patterns will be used.
	> * If set to `no` - built-in patterns will not be used.

* **`custom_patterns`** *`[]NormalizePattern`*

	List of normalization patterns.

	`NormalizePattern` params:
	* **`placeholder`** *`string`* *`required`*

		A placeholder that replaces the parts of string that falls under specified pattern.

	* **`re`** *`string`* *`required`*

		A regular expression that describes a pattern.
		> We have some [limitations](/plugin/action/hash/normalize/README.md#limitations-of-the-re-language) of the RE syntax.

	* **`priority`** *`string`* *`default=first`* *`options=first|last`*

		A priority of pattern. Works only if `normalizer.builtin_patterns != "no"`.

		If set to `first`, pattern will be added before built-in, otherwise - after.

		> If `normalizer.builtin_patterns = "no"`, then the priority is determined
		by the order of the elements in `normalizer.custom_patterns`.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*