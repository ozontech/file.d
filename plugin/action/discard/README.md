# Discard plugin
Simply drops event. Used in a combination with `match_fields`/`match_mode` parameters to filter out events.

**Example discarding informational and debug logs:**
```
pipelines:
  example_pipeline:
    ...
    actions:
    - type: discard
      match_fields:
        level: /info|debug/
    ...
```

> No config params

<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*