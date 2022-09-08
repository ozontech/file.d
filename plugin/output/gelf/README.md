# Elasticsearch output
It sends event batches to the GELF endpoint. Transport level protocol TCP or UDP is configurable.
> It doesn't support UDP chunking. So don't use UDP if event size may be greater than 8192.

GELF messages are separated by null byte. Each message is a JSON with the following fields:
* `version` *`string=1.1`*
* `host` *`string`*
* `short_message` *`string`*
* `full_message` *`string`*
* `timestamp` *`number`*
* `level` *`number`*
* `_extra_field_1` *`string`*
* `_extra_field_2` *`string`*
* `_extra_field_3` *`string`*

Every field with an underscore prefix `_` will be treated as an extra field.
Allowed characters in field names are letters, numbers, underscores, dashes, and dots.

### Config params
**`endpoint`** *`string`* *`required`* 

An address of gelf endpoint. Format: `HOST:PORT`. E.g. `localhost:12201`.

<br>

**`reconnect_interval`** *`cfg.Duration`* *`default=1m`* 

The plugin reconnects to endpoint periodically using this interval. It is useful if an endpoint is a load balancer.

<br>

**`connection_timeout`** *`cfg.Duration`* *`default=5s`* 

How much time to wait for the connection?

<br>

**`write_timeout`** *`cfg.Duration`* *`default=10s`* 

How much time to wait for the connection?

<br>

**`host_field`** *`string`* *`default=host`* 

Which field of the event should be used as `host` GELF field.

<br>

**`short_message_field`** *`string`* *`default=message`* 

Which field of the event should be used as `short_message` GELF field.

<br>

**`default_short_message_value`** *`string`* *`default=not set`* 

The default value for `short_message` GELF field if nothing is found in the event.

<br>

**`full_message_field`** *`string`* 

Which field of the event should be used as `full_message` GELF field.

<br>

**`timestamp_field`** *`string`* *`default=time`* 

Which field of the event should be used as `timestamp` GELF field.

<br>

**`timestamp_field_format`** *`string`* *`default=rfc3339nano`* *`options=ansic|unixdate|rubydate|rfc822|rfc822z|rfc850|rfc1123|rfc1123z|rfc3339|rfc3339nano|kitchen|stamp|stampmilli|stampmicro|stampnano`* 

In which format timestamp field should be parsed.

<br>

**`level_field`** *`string`* *`default=level`* 

Which field of the event should be used as a `level` GELF field. Level field should contain level number or string according to RFC 5424:
* `7` or `debug`
* `6` or `info`
* `5` or `notice`
* `4` or `warning`
* `3` or `error`
* `2` or `critical`
* `1` or `alert`
* `0` or `emergency`

Otherwise `6` will be used.

<br>

**`workers_count`** *`cfg.Expression`* *`default=gomaxprocs*4`* 

How many workers will be instantiated to send batches.

<br>

**`batch_size`** *`cfg.Expression`* *`default=capacity/4`* 

A maximum quantity of events to pack into one batch.

<br>

**`batch_size_bytes`** *`cfg.Expression`* *`default=0`* 

A minimum size of events in a batch to send.
If both batch_size and batch_size_bytes are set, they will work together.

<br>

**`batch_flush_timeout`** *`cfg.Duration`* *`default=200ms`* 

After this timeout the batch will be sent even if batch isn't completed.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*