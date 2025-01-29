# Elasticsearch output
It sends events into Elasticsearch. It uses `_bulk` API to send events in batches.
If a network error occurs, the batch will infinitely try to be delivered to the random endpoint.

### Config params
**`endpoints`** *`[]string`* *`required`* 

The list of elasticsearch endpoints in the following format: `SCHEMA://HOST:PORT`

<br>

**`use_gzip`** *`bool`* *`default=false`* 

If set, the plugin will use gzip encoding.

<br>

**`gzip_compression_level`** *`string`* *`default=default`* *`options=default|no|best-speed|best-compression|huffman-only`* 

Gzip compression level. Used if `use_gzip=true`.

<br>

**`username`** *`string`* 

Username for HTTP Basic Authentication.

<br>

**`password`** *`string`* 

Password for HTTP Basic Authentication.

<br>

**`api_key`** *`string`* 

Base64-encoded token for authorization; if set, overrides username/password.

<br>

**`ca_cert`** *`string`* 
Path or content of a PEM-encoded CA file.

<br>

**`keep_alive`** *`KeepAliveConfig`* 

Keep-alive config.

`KeepAliveConfig` params:
* `max_idle_conn_duration` - idle keep-alive connections are closed after this duration.
By default idle connections are closed after `10s`.
* `max_conn_duration` - keep-alive connections are closed after this duration.
If set to `0` - connection duration is unlimited.
By default connection duration is `5m`.

<br>

**`index_format`** *`string`* *`default=file-d-%`* 

It defines the pattern of elasticsearch index name. Use `%` character as a placeholder. Use `index_values` to define values for the replacement.
E.g. if `index_format="my-index-%-%"` and `index_values="service,time"` and event is `{"service"="my-service"}`
then index for that event will be `my-index-my-service-2020-01-05`. First `%` replaced with `service` field of the event and the second
replaced with current time(see `time_format` option)

<br>

**`index_values`** *`[]string`* *`default=[@time]`* 

A comma-separated list of event fields which will be used for replacement `index_format`.
There is a special field `time` which equals the current time. Use the `time_format` to define a time format.
E.g. `[service, time]`

<br>

**`time_format`** *`string`* *`default=2006-01-02`* 

The time format pattern to use as value for the `time` placeholder.
> Check out [func Parse doc](https://golang.org/pkg/time/#Parse) for details.

<br>

**`connection_timeout`** *`cfg.Duration`* *`default=5s`* 

It defines how much time to wait for the connection.

<br>

**`workers_count`** *`cfg.Expression`* *`default=gomaxprocs*4`* 

It defines how many workers will be instantiated to send batches.

<br>

**`batch_size`** *`cfg.Expression`* *`default=capacity/4`* 

A maximum quantity of events to pack into one batch.

<br>

**`batch_size_bytes`** *`cfg.Expression`* *`default=0`* 

A minimum size of events in a batch to send.
If both batch_size and batch_size_bytes are set, they will work together.

<br>

**`batch_flush_timeout`** *`cfg.Duration`* *`default=200ms`* 

After this timeout batch will be sent even if batch isn't full.

<br>

**`batch_op_type`** *`string`* *`default=index`* *`options=index|create`* 

Operation type to be used in batch requests. It can be `index` or `create`. Default is `index`.
> Check out [_bulk API doc](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html) for details.

<br>

**`retry`** *`int`* *`default=10`* 

Retries of insertion. If File.d cannot insert for this number of attempts,
File.d will fall with non-zero exit code or skip message (see fatal_on_failed_insert).

<br>

**`fatal_on_failed_insert`** *`bool`* *`default=false`* 

After an insert error, fall with a non-zero exit code or not
**Experimental feature**

<br>

**`retention`** *`cfg.Duration`* *`default=1s`* 

Retention milliseconds for retry to DB.

<br>

**`retention_exponentially_multiplier`** *`int`* *`default=2`* 

Multiplier for exponential increase of retention between retries

<br>

**`strict`** *`bool`* *`default=false`* 

After a non-retryable write error, fall with a non-zero exit code or not

<br>

**`ingest_pipeline`** *`string`* 

The name of the ingest pipeline to write events to.

<br>


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*