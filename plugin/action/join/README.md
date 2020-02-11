# Join action plugin
Plugin also known as "multiline" makes one big event from event sequence.
Useful for assembling back together "exceptions" or "panics" if they was written line by line.

> ⚠ Parsing all event flow could be very CPU intensive because plugin uses regular expressions.
> Consider `match_fields` parameter to process only particular events. Check out example for details.

Example of joining Golang panics:
```
pipelines:
  example_pipeline:
    ...
    actions:
    - type: join
      field: log
      start: '/^(panic:)|(http: panic serving)/'
      continue: '/(^\s*$)|(goroutine [0-9]+ \[)|(\([0-9]+x[0-9,a-f]+)|(\.go:[0-9]+ \+[0-9]x)|(\/.*\.go:[0-9]+)|(\(...\))|(main\.main\(\))|(created by .*\/.*\.)|(^\[signal)|(panic.+[0-9]x[0-9,a-f]+)|(panic:)/'
      match_fields:
        stream: stderr // apply only for events which was written to stderr to save CPU time
    ...
```

## Config params
### field

`cfg.FieldSelector`  `required` 

Field of event which will be analyzed for joining with each other.

### start

`cfg.Regexp`  `required` 

Regexp which will start join sequence.

### continue

`cfg.Regexp`  `required` 

Regexp which will continue join sequence.


### Understanding start/continue regexps
No joining:
```
event 1
event 2 – matches start regexp
event 3
event 4 – matches continue regexp
event 5
```

Events `event 2` and `event 3` will be joined:
```
event 1
event 2 – matches start regexp
event 3 – matches continue regexp
event 4
```

Events from `event 2` to `event N` will be joined:
```
event 1
event 2 matches start regexp
event 3 matches continue regexp
event ... matches continue regexp
event N matches continue regexp
event N+1
```

*Generated using __insane-doc__*