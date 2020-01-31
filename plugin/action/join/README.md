# Join action plugin
Plugin also known as "multiline" makes one big event from event sequence.
Useful for assembly back "exceptions" or "panics" together then they written line by line.

### Understanding `first_re`/`next_re`

No joining:
```
event 1
event 2 – matches first_re
event 3
event 4 – matches next_re
event 5
```

Events `event 2` and `event 3` will be joined:
```
event 1
event 2 – matches first_re
event 3 – matches next_re
event 4
```

Events from `event 2` to `event N` will be joined:
```
event 1
event 2 matches first_re
event 3 matches next_re
event ... matches next_re
event N matches next_re
event N+1
```
<br/>

Example joining Golang panics:
```
pipelines:
  example_pipeline:
    ...
    actions:
    - type: join
	  field: log
	  first_re: '/^(panic:)|(http: panic serving)/'
	  next_re: '/(^\s*$)|(goroutine [0-9]+ \[)|(\([0-9]+x[0-9,a-f]+)|(\.go:[0-9]+ \+[0-9]x)|(\/.*\.go:[0-9]+)|(\(...\))|(main\.main\(\))|(created by .*\/.*\.)|(^\[signal)|(panic.+[0-9]x[0-9,a-f]+)|(panic:)/'
    ...
```

## Config params
### field

`string`   

Field of events which will be analyzed for joining with each other.

### first_re

`string`   

Regexp which will start join sequence.

### next_re

`string`   

Regexp which will continue join sequence.


##
 *Generated using **insane-doc***