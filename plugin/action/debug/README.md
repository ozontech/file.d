# Debug plugin
It logs event to stderr. Useful for debugging.

It may sample by logging the `first` N entries each tick.
If more events are seen during the same `interval`,
every `thereafter` message is logged and the rest are dropped.

For example,

```yaml
- type: debug
  interval: 1s
  first: 10
  thereafter: 5
```

This will log the first 10 events in a one second interval as-is.
Following that, it will allow through every 5th event in that interval.


### Config params
**`interval`** *`cfg.Duration`* 

<br>

**`first`** *`int`* 

<br>

**`thereafter`** *`int`* 

Check the example above for more information.

<br>

**`message`** *`string`* *`default=event sample`* 

'message' field content.
Use it to determine which 'debug' action has written the log.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*