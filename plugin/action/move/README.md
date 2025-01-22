# Move plugin
It moves fields to the target field in a certain mode.
* In `allow` mode, the specified `fields` will be moved
* In `block` mode, the unspecified `fields` will be moved

## Examples
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: move
      mode: allow
      target: other
      fields:
        - log.stream
        - zone
    ...
```
The original event:
```json
{
  "service": "test",
  "log": {
    "level": "error",
    "message": "error occurred",
    "ts": "2023-10-30T13:35:33.638720813Z",
    "stream": "stderr"
  },
  "zone": "z501"
}
```
The resulting event:
```json
{
  "service": "test",
  "log": {
    "level": "error",
    "message": "error occurred",
    "ts": "2023-10-30T13:35:33.638720813Z"
  },
  "other": {
    "stream": "stderr",
    "zone": "z501"
  }
}
```
---
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: move
      mode: block
      target: other
      fields:
        - log
    ...
```
The original event:
```json
{
  "service": "test",
  "log": {
    "level": "error",
    "message": "error occurred",
    "ts": "2023-10-30T13:35:33.638720813Z",
    "stream": "stderr"
  },
  "zone": "z501",
  "other": {
    "user": "ivanivanov"
  }
}
```
The resulting event:
```json
{
  "log": {
    "level": "error",
    "message": "error occurred",
    "ts": "2023-10-30T13:35:33.638720813Z"
  },
  "other": {
    "user": "ivanivanov",
    "service": "test",
    "zone": "z501"
  }
}
```
---
```yaml
pipelines:
  example_pipeline:
    ...
    actions:
    - type: move
      mode: allow
      target: other
      fields:
        - log.message
        - error.message
        - zone
    ...
```
The original event:
```json
{
  "service": "test",
  "log": {
    "message": "some log",
    "ts": "2023-10-30T13:35:33.638720813Z"
  },
  "error": {
    "code": 1,
    "message": "error occurred"
  },
  "zone": "z501"
}
```
The resulting event:
```json
{
  "service": "test",
  "log": {
    "ts": "2023-10-30T13:35:33.638720813Z"
  },
  "error": {
    "code": 1,
  },
  "other": {
    "message": "error occurred",
    "zone": "z501"
  }
}
```

## Config params
**`fields`** *`[]cfg.FieldSelector`* *`required`* 

The list of the fields to move.
> 1. In `block` mode, the maximum `fields` depth is 1.
> 2. If several fields have the same end of the path,
> the last specified field will overwrite the previous ones.

<br>

**`mode`** *`string`* *`required`* 

The mode of the moving. Available modes are one of: `allow|block`.

<br>

**`target`** *`cfg.FieldSelector`* *`required`* 

The target field of the moving.
> 1. In `block` mode, the maximum `target` depth is 1.
> 2. If the `target` field is existing non-object field,
> it will be overwritten as object field.

<br>

<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*