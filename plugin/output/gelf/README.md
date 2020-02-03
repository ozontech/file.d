# Elasticsearch output
Plugin sends event batches to the GELF endpoint. Transport level protocol TCP or UDP is configurable.
> It doesn't support UDP chunking. So don't use UDP if event size may be grater than 8192.

GELF messages are separated by null byte. Each message is a JSON with the following fields:
* `version`, string, should be `1.1`
* `host`, string
* `short_message`, string
* `full_message`, string
* `timestamp`, number
* `level`, number
* `_extra_field_1`, string
* `_extra_field_2`, string
* `_extra_field_3`, string

Every field with an underscore prefix (_) will be treated as an extra field.
Allowed characters in a field names are any word character(letter, number, underscore), dashes and dots.

## Config params
### endpoint

`string`  `required` 

Address of gelf endpoint. Format: `HOST:PORT`. E.g. `localhost:12201`.

### reconnect_interval

`cfg.Duration` `default=1m`  

Plugin reconnects to endpoint periodically using this interval. Useful if endpoint is a load balancer.

### connection_timeout

`cfg.Duration` `default=5s`  

How much time to wait for connection.

### host_field

`string` `default=host`  

Which field of event should be used as `host` GELF field.

### short_message_field

`string` `default=message`  

Which field of event should be used as `short_message` GELF field.

### default_short_message_value

`string` `default=not set`  

Default value for `short_message` GELF field if nothing is found in the event.

### full_message_field

`string`   

Which field of event should be used as `full_message` GELF field.

### timestamp_field

`string` `default=time`  

Which field of event should be used as `timestamp` GELF field.

### timestamp_field_format

`string` `default=rfc3339nano`  `options=ansic|unixdate|rubydate|rfc822|rfc822z|rfc850|rfc1123|rfc1123z|rfc3339|rfc3339nano|kitchen|stamp|stampmilli|stampmicro|stampnano`

In which format timestamp field should be parsed.

### level_field

`string` `default=level`  

Which field of event should be used as `level` GELF field. Level field should contain level number or string according to RFC 5424:
* `7`/`debug`
* `6`/`info`
* `5`/`notice`
* `4`/`warning`
* `3`/`error`
* `2`/`critical`
* `1`/`alert`
* `0`/`emergency`
Otherwise `6` will be used.

### workers_count

`cfg.Expression` `default=gomaxprocs*4`  

How much workers will be instantiated to send batches.

### batch_size

`cfg.Expression` `default=capacity/4`  

Maximum quantity of events to pack into one batch.

### batch_flush_timeout

`cfg.Duration` `default=200ms`  

After this timeout batch will be sent even if batch isn't completed.


##
 *Generated using **insane-doc***