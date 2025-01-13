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

**`time_field_format`** *`string`* *`default=rfc3339nano`* 

It defines how to parse the time field format.

<br>

**`default_limit`** *`int64`* *`default=5000`* 

The default events limit that plugin allows per `bucket_interval`.

<br>

**`limit_kind`** *`string`* *`default=count`* *`options=count|size`* 

It defines subject of limiting: number of messages or total size of the messages.

<br>

**`limiter_backend`** *`string`* *`default=memory`* *`options=memory|redis`* 

Defines kind of backend. When redis backend is chosen and if by any reason plugin cannot connect to redis,
limiters will not start syncing with redis until successful reconnect.

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
Each object has the `limit`, `limit_kind` and `conditions` fields as well as an optional `limit_distribution` field.
* `limit` – the value which will override the `default_limit`, if `conditions` are met.
* `limit_kind` – the type of limit: `count` - number of messages, `size` - total size from all messages
* `conditions` – the map of `event field name => event field value`. The conditions are checked using `AND` operator.
* `limit_distribution` – see `LimitDistributionConfig` for details.

<br>

**`limiter_expiration`** *`cfg.Duration`* *`default=30m`* 

Time interval after which unused limiters are removed.

<br>

**`limit_distribution`** *`LimitDistributionConfig`* 

It allows to distribute the `default_limit` between events by condition.

`LimitDistributionConfig` params:
* `field` - the event field on which the distribution will be based.
* `ratios` - the list of objects. Each object has:
	* `ratio` - distribution ratio, value must be in range [0.0;1.0].
	* `values` - the list of strings which contains all `field` values that fall into this distribution.
* `metric_labels` - list of metric labels.

> Notes:
> 1. Sum of ratios must be in range [0.0;1.0].
> 2. If sum of ratios less than 1, then adding **default distribution** with ratio **1-sum**,
> otherwise **default distribution** isn't used.
> All events for which the value in the `field` doesn't fall into any of the distributions:
>   * fall into default distribution, if it exists
>   * throttled, otherwise
> 3. **default distribution** can "steal" limit from other distributions after it has exhausted its.
> This is done in order to avoid reserving limits for explicitly defined distributions.

`LimitDistributionConfig` example:
```yaml
field: log.level
ratios:
  - ratio: 0.5
    values: ['error']
  - ratio: 0.3
    values: ['warn', 'info']
```
For this config and the `default_limit=100`:
* events with `log.level=error` will be NO MORE than `50`
* events with `log.level=warn` or `log.level=info` will be NO MORE than `30`
* there will be AT LEAST `20` other events
(can be up to `100` if there are no events with `log.level=error/warn/info`)

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

**`limiter_key_field`** *`cfg.FieldSelector`* 

Defines the event field from which values are used as limiter keys. Serves as an override of the default limiter keys naming pattern.
If not set limiter keys are formed using pipeline name, throttle field and throttle field value.

<br>

**`limiter_value_field`** *`string`* 

Defines field with limit inside json object stored in value
(e.g. if set to "limit", values must be of kind `{"limit":"<int>",...}`).
If not set limiter values are considered as non-json data.

<br>

**`limiter_distribution_field`** *`string`* 

Defines field with limit distribution inside json object stored in value
(e.g. if set to "distribution", value must be of kind `{"distribution":{<object>},...}`).
Distribution object example:
```json
{
  "field": "log.level",
  "ratios": [
    {
      "ratio": 0.5,
      "values": ["error"]
    },
    {
      "ratio": 0.3,
      "values": ["warn", "info"]
    }
  ],
  "enabled": true
}
```
> If `limiter_value_field` and `limiter_distribution_field` not set, distribution will not be stored.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*