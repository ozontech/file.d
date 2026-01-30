# HTTP output
It sends events to arbitrary HTTP endpoints. It uses POST requests to send events in batches.
If a network error occurs, the batch will infinitely try to be delivered to the random endpoint.

Supports [dead queue](/plugin/output/README.md#dead-queue).

### Config params
**`endpoints`** *`[]string`* *`required`* 

The list of HTTP endpoints in the following format: `SCHEMA://HOST:PORT/PATH`

<br>

**`content_type`** *`string`* *`default=application/json`* 

Content-Type header for HTTP requests.

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

**`retry`** *`int`* *`default=10`* 

Retries of insertion. If File.d cannot insert for this number of attempts,
File.d will fall with non-zero exit code or skip message (see fatal_on_failed_insert).

<br>

**`fatal_on_failed_insert`** *`bool`* *`default=false`* 

After an insert error, fall with a non-zero exit code or not. A configured deadqueue disables fatal exits.

<br>

**`split_batch`** *`bool`* *`default=false`* 

Enable split big batches

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


<br>*Generated using [__insane-doc__](https://github.com/vitkovskii/insane-doc)*