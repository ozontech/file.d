# Join plugin
It makes one big event from the sequence of the events.
It is useful for assembling back together "exceptions" or "panics" if they were written line by line.
Also known as "multiline".

> ⚠ Parsing the whole event flow could be very CPU intensive because the plugin uses regular expressions.
> Consider `match_fields` parameter to process only particular events. Check out an example for details.

**Example of joining Go panics**:
```yaml
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

### Config params
**`field`** *`cfg.FieldSelector`* *`required`* 

The event field which will be checked for joining with each other.

<br>

**`start`** *`cfg.Regexp`* *`required`* 

A regexp which will start the join sequence.

<br>

**`continue`** *`cfg.Regexp`* *`required`* 

A regexp which will continue the join sequence.

<br>

**`max_event_size`** *`int`* *`default=0`* 

Max size of the resulted event. If it is set and the event exceeds the limit, the event will be truncated.

<br>

**`negate`** *`bool`* *`default=false`* 

Negate match logic for Continue (lets you implement negative lookahead while joining lines)

<br>


### Understanding start/continue regexps
**No joining:**
```
event 1
event 2 – matches start regexp
event 3
event 4 – matches continue regexp
event 5
```

**Events `event 2` and `event 3` will be joined:**
```
event 1
event 2 – matches start regexp
event 3 – matches continue regexp
event 4
```

**Events from `event 2` to `event N` will be joined:**
```
event 1
event 2 matches start regexp
event 3 matches continue regexp
event ... matches continue regexp
event N matches continue regexp
event N+1
```
<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*