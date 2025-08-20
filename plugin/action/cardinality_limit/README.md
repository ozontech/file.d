# Discard plugin
It drops an event. It is used in a combination with `match_fields`/`match_mode` parameters to filter out the events.

**An example for discarding informational and debug logs:**
```yaml
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