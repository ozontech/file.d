# Pipeline

Pipeline is an entity which handles data. It consists of input plugin, list of action plugins and output plugin. The input plugin sends the data to `pipeline.In` controller. There the data is validated, if the data is empty, it is discarded, the data size is also checked, the behaviour for the long logs is defined by `cut_off_event_by_limit` setting. Then the data is checked in `antispam` if it is enabled. After all checks are passed the data is converted to the `Event` structure, the events are limited by the `EventPool`, and decoded depending on the [pipeline settings](#settings). The event is sent to stream which are handled with `processors`. In the processors the event is passed through the list of action plugins and sent to the output plugin. Output plugin commits the `Event` by calling `pipeline.Commit` function and after the commit is finished the data is considered as processed. More details and architecture is presented in [architecture page](/docs/architecture.md).

## Settings

**`capacity`** *`int`* *`default=1024`* 

Capacity of the `EventPool`. There can only be processed no more than `capacity` events at the same time. It can be considered as one of the rate limiting tools, but its primary role is to control the amount of RAM used by File.d.

<br>

**`avg_log_size`** *`int`* *`default=4096`* 

Expected average size of the input logs in bytes. Used in standard event pool to release buffer memory when its size exceeds this value.

<br>

**`max_event_size`** *`int`* *`default=0`* 

Maximum allowed size of the input logs in bytes. If set to 0, logs of any size are allowed. If set to the value greater than 0, logs with size greater than `max_event_size` are discarded unless `cut_off_event_by_limit` is set to `true`.

<br>

**`cut_off_event_by_limit`** *`bool`* *`default=false`* 

Flag indicating whether to cut logs which have exceeded the `max_event_size`. If set to `true` huge logs are cut and only the first `max_event_size` bytes of the logs are passed further. If set to `false` huge logs are discarded. Only works if `max_event_size` is greater than 0, otherwise does nothing. Useful when there are huge logs which affect the logging system but it is prefferable to deliver them at least partially.

<br>

**`cut_off_event_by_limit_field`** *`string`*

Field to add to log if it was cut by `max_event_size`. E.g. with `cut_off_event_by_limit_field: _cropped`, if the log was cut, the output event will have field `"_cropped":true`. Only works if `cut_off_event_by_limit` is set to `true` and `max_event_size` is greater than 0. Useful for marking cut logs.

<br>

**`decoder`** *`string`* *`default=auto`* 

Which decoder to use on every log from input plugin. Defaults to `auto` meaning the usage of the decoder suggested by the input plugin. Currently most of the time `json` decoder is suggested, the only exception is [k8s input plugin](/plugin/input/k8s/README.md) with CRI type not docker, in that case `cri` decoder is suggested. The full list of the decoders is available on the [decoders page](/decoder/readme.md).

<br>

**`decoder_params`** *`map[string]any`*

Additional parameters for the chosen decoder. The params list varies. It can be found on the [decoders page](/decoder/readme.md) for each of them.

<br>

**`stream_field`** *`string`* *`default=stream`* 

Which field in the log indicates `stream`. Mostly used for distinguishing `stdout` from `stderr` in k8s logs.

<br>

**`maintenance_interval`** *`string`* *`default=5s`* 

How often to perform maintenance. Maintenance includes antispammer maintenance and metric cleanup, metric holder maintenance, increasing basic pipeline metrics with accumulated deltas, logging pipeline stats. The value must be passed in format of duration (`<number>(ms|s|m|h)`).

<br>

**`event_timeout`** *`bool`* *`default=30s`* 

How long the event can process in action plugins and block stream in streamer until it is marked as a timeout event and unlocks stream so that the whole pipeline does not get stuck. The value must be passed in format of duration (`<number>(ms|s|m|h)`).

<br>

**`antispam_threshold`** *`int`* *`default=0`* 

Threshold value for the [antispammer](/pipeline/antispam/README.md#antispammer) to ban sources. If set to 0 antispammer is disabled. If set to the value greater than 0 antispammer is enabled and bans sources which write `antispam_threshold` or more logs in `maintenance_interval` time.

<br>

**`antispam_exceptions`** *`[]`[antispam.Exception](/pipeline/antispam/README.md#exception-parameters)*

The list of antispammer exceptions. If the log matches at least one of the exceptions it is not accounted in antispammer.

<br>

**`meta_cache_size`** *`int`* *`default=1024`* 

Amount of entries in metadata cache.

<br>

**`source_name_meta_field`** *`string`*

The metadata field used to retrieve the name or origin of a data source. You can use it for antispam. Metadata is configured via `meta` parameter in input plugin. For example:

```yaml
input:
    type: k8s
    meta:
        pod_namespace: '{{ .pod_name }}.{{ .namespace_name }}'
pipeline:
    antispam_threshold: 2000
    source_name_meta_field: pod_namespace
```

<br>

**`is_strict`** *`bool`* *`default=false`* 

Whether to fatal on decoding error.

<br>

**`metric_hold_duration`** *`string`* *`default=30m`* 

The amount of time the metric can be idle until it is deleted. Used for deleting rarely updated metrics to save metrics storage resources. The value must be passed in format of duration (`<number>(ms|s|m|h)`).

<br>

**`pool`** *`string`* *`options=std|low_memory`*

Type of `EventPool` that file.d uses to reuse memory. 
`std` pool is an original event pool with pre-allocated events at the start of the application.
This pool only frees up memory if event exceeds `avg_log_size`.

`low_memory` event pool based on Go's [sync.Pool](https://pkg.go.dev/sync#Pool) with lazy memory allocation.
It frees up memory depending on the application load - if file.d processes a lot of events, then a lot of memory will be allocated.
If the application load decreases, then the extra events will be freed up in background.

Note that `low_memory` pool increases the load on the garbage collector. 
If you are confident in what you are doing, you can change the [GOGC](https://tip.golang.org/doc/gc-guide) (file.d uses GOGC=30 as default value)
environment variable to adjust the frequency of garbage collection – this can reduce the load on the CPU.

Both pools support the `capacity` setting, which both pools use to ensure that they do not exceed the number of allocated events.
This parameter is useful for avoiding OOM.

Default pool is `low_memory`.

<br>

## Datetime parse formats

Most of the plugins which work with parsing datetime call `pipeline.ParseTime` function. It accepts datetime layouts the same way as Go [time.Parse](https://pkg.go.dev/time#Parse) (in format of datetime like `2006-01-02T15:04:05.999999999Z07:00`) except unix timestamp formats, they can only be specified via aliases.

For the comfort of use there are aliases to some datetime formats:

+ `ansic` - Mon Jan _2 15:04:05 2006
+ `unixdate` - Mon Jan _2 15:04:05 MST 2006
+ `rubydate` - Mon Jan 02 15:04:05 -0700 2006
+ `rfc822` - 02 Jan 06 15:04 MST
+ `rfc822z` - 02 Jan 06 15:04 -0700
+ `rfc850` - Monday, 02-Jan-06 15:04:05 MST
+ `rfc1123` - Mon, 02 Jan 2006 15:04:05 MST
+ `rfc1123z` - Mon, 02 Jan 2006 15:04:05 -0700
+ `rfc3339` - 2006-01-02T15:04:05Z07:00
+ `rfc3339nano` - 2006-01-02T15:04:05.999999999Z07:00
+ `kitchen` - 3:04PM
+ `stamp` - Jan _2 15:04:05
+ `stampmilli` - Jan _2 15:04:05.000
+ `stampmicro` - Jan _2 15:04:05.000000
+ `stampnano` - Jan _2 15:04:05.000000000
+ `nginx_errorlog` - 2006/01/02 15:04:05
+ `unixtime` - unix timestamp in seconds: 1739959880
+ `unixtimemilli` - unix timestamp in milliseconds: 1739959880999
+ `unixtimemicro` - unix timestamp in microseconds: 1739959880999999 (e.g. `journalctl` writes timestamp in that format in `__REALTIME_TIMESTAMP` field when using json output format)
+ `unixtimenano` - unix timestamp in nanoseconds: 1739959880999999999

**Note**: when using `unixtime(|milli|micro|nano)` if there is a float value its whole part is always considered as seconds and the fractional part is fractions of a second.

## Match modes

> Note: consider using [DoIf match rules](/pipeline/doif/README.md) instead, since it is an advanced version of match modes.

@match-modes|header-description
