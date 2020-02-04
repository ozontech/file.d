# Throttle action plugin
Plugin drops events if event flow gets higher than a configured threshold.

## Config params
### throttle_field

`cfg.FieldSelector`   

Event field which will be used as a key for throttling.
It means that throttling will work separately for events with different keys.
If not set, it's assumed that all events have the same key.

### time_field

`cfg.FieldSelector` `default=time`  

Event field which defines the time when event was fired.
It used to detect event throughput in particular time range.
If not set current time will be taken.

### time_field_format

`string` `default=rfc3339nano`  `options=ansic|unixdate|rubydate|rfc822|rfc822z|rfc850|rfc1123|rfc1123z|rfc3339|rfc3339nano|kitchen|stamp|stampmilli|stampmicro|stampnano`

Defines how to parse time field format.

### default_limit

`int64` `default=5000`  

Default limit of events that plugin allows per `interval`

### interval

`cfg.Duration` `default=1m`  

Time interval to check event throughput.

### buckets_count

`int` `default=60`  

How much time buckets to hold in the memory. E.g. if `buckets_count` is `60` and `interval` is `5m`,
then `5 hours` will be covered. Events with time later than `now() - 5h` will be dropped even if threshold isn't exceeded.

### rules

`[]RuleConfig`   

Rules can override `default_limit` for different group of event. It's a list of objects.
Each object have `limit` and `conditions` field.
* `limit` – value which will override `default_limit`, if `conditions` are met.
* `conditions` – a map of `event field name => event field value`. Conditions are checked using `AND` operator.


##
 *Generated using **insane-doc***