# File multiline action
It joins split stdout/container log chunks into a single event.

Docker/CRI splits long logs into ~16kb chunks. Use this action with [file input plugin](/plugin/input/file/README.md) when reading container logs from files (e.g. `/var/log/containers/*.log`).

> ⚠ Place it **before** `json_decode`, `decode`, and other actions that expect a complete field value. For Docker JSON logs use `field: log`.

**Example:**
```yaml
pipelines:
  example_pipeline:
    settings:
      decoder: json
    input:
      type: file
      offsets_file: /data/offsets.yaml
      watching_dir: /var/log/containers/
    actions:
    - type: file_multiline
      field: log
      split_event_size: 1000000
    - type: json_decode
      field: log
```

### How it works
1. Reads the configured field (`log` by default) from each sequential event in the same file stream.
2. If the value does **not** end with a real newline character `\n`, the chunk is buffered and the plugin waits for the next event (`ActionCollapse`). Further actions are not run yet.
3. When the next chunk arrives, it is appended to the buffer and the end is checked again.
4. When the value ends with `\n`, all buffered chunks are merged into one field and a single event is passed downstream (`ActionPass`).
5. If the joined event exceeds `split_event_size`, it may be split forcibly even without `\n` at the end.
6. If no continuation arrives within the pipeline `event_timeout` (default `30s`), the buffer is reset and the partial event is discarded.

### Config params
**`field`** *`cfg.FieldSelector`* *`default=log`* 

The event field which will be joined.

<br>

**`split_event_size`** *`int`* *`default=1000000`* 

Docker splits long logs by 16kb chunks. The plugin joins them back, but if an event is longer than this value in bytes, it will be split after all.
> Due to the optimization process it's not a strict rule. Events may be split even if they won't exceed the limit.

<br>
