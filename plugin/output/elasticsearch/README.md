# Elasticsearch output
Plugin writes events into Elasticsearch. It uses `_bulk` API to send events in batches.
If a network error occurs batch will be infinitely tries to be delivered to random endpoint.

## Config params
### index_format

`string`  `required` 

Defines pattern of elasticsearch index name. Use `%` character as a placeholder. Use `index_values` to define values for replacement.
E.g. if `index_format="my-index-%-%"` and `index_values="service,@time"` and event is `{"service"="my-service"}`
then index for that event will be `my-index-my-service-2020-01-05`. First `%` replaced with `service` field of the event and the second
replaced with current time(see `time_format` option)

### endpoints

`string`  `required` 

Comma separated list of elasticsearch endpoints in format `SCHEMA://HOST:PORT`

### index_values

`string` `default=@time`  

Comma-separated list of event fields which will be used for replacement `index_format`.
There is a special field `@time` which equals to current time. Use `time_format` to define time format.
E.g. `service,@time`

### time_format

`string` `default=2006-01-02`  

Time format pattern to use as value for the `@time` placeholder.
> Check out https://golang.org/pkg/time/#Parse for details.

### flush_timeout

`pipeline.Duration` `default=200ms`  

After this timeout batch will be sent even if batch isn't completed.

### connection_timeout

`pipeline.Duration` `default=5s`  

How much time to wait for connection.

### workers_count

`int` `default=gomaxprocs*4`  

How much workers will be instantiated to send batches.

### batch_size

`int` `default=capacity/4`  

Maximum quantity of events to pack into one batch.

### index_error_warn_only

`bool` `default=false`  

If set to true, indexing error won't lead to fatal error



 Generated using *insane-doc*