# Discard action plugin
Plugin simply drops event. Used in a combination with `match_fields` parameters to filter out events.

Example discarding informational and debug logs:
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


*Generated using __insane-doc__*