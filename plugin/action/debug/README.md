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

If it is needed to log every entry, logger without sampling can be used,

```yaml
- type: debug
  interval: 0s
```


### Config params
**`interval`** *`cfg.Duration`* 

Tick interval for sampling logging. The first N entries with a given level and message
each tick. If more Entries with the same level and message are seen during
the same interval, every Mth message is logged and the rest are dropped.

If set to 0, plugin uses parent logger without sampling.

Check the example above for more information.

<br>

**`first`** *`int`* 

Specifies the first N entries with a given level and message each tick.

Check the example above for more information.

<br>

**`thereafter`** *`int`* 

Specifies entries frequency after the first N entries.
If greater than 0, every Mth message is logged and the rest are dropped.
If set to 0, every entry after the first N are dropped.

Check the example above for more information.

<br>

**`message`** *`string`* *`default=event sample`* 

'message' field content.
Use it to determine which 'debug' action has written the log.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*