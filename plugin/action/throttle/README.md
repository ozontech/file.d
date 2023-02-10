# Throttle plugin
It discards the events if pipeline throughput gets higher than a configured threshold.

### Config params
**`throttle_field`** *`cfg.FieldSelector`* 

The event field which will be used as a key for throttling.
It means that throttling will work separately for events with different keys.
If not set, it's assumed that all events have the same key.

<br>

**`time_field`** *`cfg.FieldSelector`* *`default=time`* 

The event field which defines the time when event was fired.
It is used to detect the event throughput in a particular time range.
If not set, the current time will be taken.

<br>

**`time_field_format`** *`string`* *`default=rfc3339nano`* *`options=ansic|unixdate|rubydate|rfc822|rfc822z|rfc850|rfc1123|rfc1123z|rfc3339|rfc3339nano|kitchen|stamp|stampmilli|stampmicro|stampnano|unixtime|nginx_errorlog`* 

It defines how to parse the time field format.

<br>

**`default_limit`** *`int64`* *`default=5000`* 

The default events limit that plugin allows per `bucket_interval`.

<br>

**`limit_kind`** *`string`* *`default=count`* *`options=count|size`* 

It defines subject of limiting: number of messages or total size of the messages.

<br>

**`limiter_backend`** *`string`* *`default=memory`* *`options=memory|redis`* 

Defines kind of backend.

<br>

**`redis_backend_config`** *`RedisBackendConfig`* 

It contains redis settings

<br>

**`buckets_count`** *`int`* *`default=60`* 

How much time buckets to hold in the memory. E.g. if `buckets_count` is `60` and `bucket_interval` is `5m`,
then `5 hours` will be covered. Events with time later than `now() - 5h` will be dropped even if threshold isn't exceeded.

<br>

**`bucket_interval`** *`cfg.Duration`* *`default=1m`* 

Time interval to check event throughput.

<br>

**`rules`** *`[]RuleConfig`* 

Rules to override the `default_limit` for different group of event. It's a list of objects.
Each object has the `limit` and `conditions` fields.
* `limit` – the value which will override the `default_limit`, if `conditions` are met.
* `limit_kind` – the type of a limit: `count` - number of messages, `size` - total size from all messages
* `conditions` – the map of `event field name => event field value`. The conditions are checked using `AND` operator.

<br>

**`endpoint`** *`string`* 


<br>

**`password`** *`string`* 

Password to redis server.

<br>

**`sync_interval`** *`cfg.Duration`* *`default=5s`* 

Defines sync interval between global and local limiters.

<br>

**`worker_count`** *`int`* *`default=32`* 

Defines num of parallel workers that will sync limits.

<br>

**`timeout`** *`cfg.Duration`* *`default=1s`* 

Defines redis timeout.

<br>

**`max_retries`** *`int`* *`default=3`* 

Defines redis maximum number of retries. If set to 0, no retries will happen.

<br>

**`min_retry_backoff`** *`cfg.Duration`* *`default=8ms`* 

Defines redis minimum backoff between each retry. If set to 0, sets default 8ms. If set to -1, disables backoff.

<br>

**`max_retry_backoff`** *`cfg.Duration`* *`default=512ms`* 

Defines redis maximum backoff between each retry. If set to 0, sets default 512ms. If set to -1, disables backoff.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*